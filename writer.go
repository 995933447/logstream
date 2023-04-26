package logstream

import (
	"errors"
	"github.com/995933447/confloader"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrWriterExited = errors.New("writer already exited")
)

func NewWriter(cfgFilePath string) (*Writer, error) {
	if cfgFilePath == "" {
		cfgFilePath = defaultCfgFilePath
	}

	var cfg Cfg
	cfgLoader := confloader.NewLoader(cfgFilePath, refreshCfgInterval, &cfg)
	err := cfgLoader.Load()
	if err != nil {
		return nil, err
	}

	writer := &Writer{
		baseDir:           strings.TrimRight(cfg.BaseDir, string(filepath.Separator)),
		compressTopics:    NewTopicSet(),
		topicOutputMap:    map[string]*Output{},
		msgChan:           make(chan *Msg, 100000),
		flushSignCh:       make(chan struct{}),
		unwatchCfgSignCh:  make(chan struct{}),
		idxFileMaxItemNum: cfg.IdxFileMaxItemNum,
	}
	writer.dataFileMaxBytes, err = parseMemSizeStrToBytes(cfg.DataFileMaxSize)
	if err != nil {
		return nil, err
	}
	writer.compressTopics.reset(cfg.CompressTopics)

	go func() {
		watchWriterCfg(writer, &cfg, cfgLoader)
	}()

	go func() {
		writer.loop()
	}()

	return writer, nil
}

func watchWriterCfg(writer *Writer, cfg *Cfg, cfgLoader *confloader.Loader) {
	refreshCfgErr := make(chan error)
	go func() {
		refreshCfgTk := time.NewTicker(refreshCfgInterval + time.Second)
		defer refreshCfgTk.Stop()
		for {
			select {
			case err := <-refreshCfgErr:
				Logger.Debug(nil, err)
			case <-writer.unwatchCfgSignCh:
			case <-refreshCfgTk.C:
				writer.opOutputMu.Lock()
				var err error
				writer.dataFileMaxBytes, err = parseMemSizeStrToBytes(cfg.DataFileMaxSize)
				if err != nil {
					Logger.Debug(nil, err)
					continue
				}
				writer.idxFileMaxItemNum = cfg.IdxFileMaxItemNum
				writer.compressTopics.reset(cfg.CompressTopics)
				if writer.baseDir != cfg.BaseDir {
					writer.baseDir = cfg.BaseDir
					for _, output := range writer.topicOutputMap {
						output.close()
					}
					writer.topicOutputMap = map[string]*Output{}
				}
				writer.opOutputMu.Unlock()
			}

			if writer.IsExited() {
				break
			}

			if writer.IsStopped() {
				time.Sleep(time.Second * 5)
			}
		}
	}()
	cfgLoader.WatchToLoad(refreshCfgErr)
}

type Msg struct {
	topic      string
	buf        []byte
	compressed []byte
}

type Writer struct {
	baseDir           string
	idxFileMaxItemNum uint32
	dataFileMaxBytes  uint32
	compressTopics    *TopicSet
	topicOutputMap    map[string]*Output
	msgChan           chan *Msg
	unwatchCfgSignCh  chan struct{}
	flushSignCh       chan struct{}
	flushCond         *sync.Cond
	flushWait         sync.WaitGroup
	status            runState
	opOutputMu        sync.Mutex
}

func (w *Writer) loop() {
	syncDiskTk := time.NewTicker(5 * time.Second)
	checkCorruptTk := time.NewTicker(30 * time.Second)
	defer func() {
		syncDiskTk.Stop()
		checkCorruptTk.Stop()
	}()

	for {
		select {
		case <-syncDiskTk.C:
			w.opOutputMu.Lock()
			for _, output := range w.topicOutputMap {
				output.syncDisk()
			}
			w.opOutputMu.Unlock()
		case <-checkCorruptTk.C:
			w.opOutputMu.Lock()
			for _, output := range w.topicOutputMap {
				corrupted, err := output.isCorrupted()
				if err != nil {
					Logger.Debug(nil, err)
					continue
				}

				// blessing for god, never been true
				if !corrupted {
					continue
				}

				Logger.Debug(nil, errFileCorrupted)

				if err = output.openNewFile(); err != nil {
					output.idxFp = nil
					output.dataFp = nil
					Logger.Debug(nil, err)
				}
			}
			w.opOutputMu.Unlock()
		case msg := <-w.msgChan:
			w.opOutputMu.Lock()
			if err := w.doWriteMost([]*Msg{msg}); err != nil {
				Logger.Debug(nil, err)
			}
			w.opOutputMu.Unlock()
		case <-w.flushSignCh:
			w.opOutputMu.Lock()
			if err := w.doWriteMost(nil); err != nil {
				Logger.Debug(nil, err)
			}

			for _, output := range w.topicOutputMap {
				output.syncDisk()
			}

			if w.IsRunning() {
				w.flushWait.Done()
				w.opOutputMu.Unlock()
				break
			}

			if w.IsExiting() {
				w.status = runStateExited
				for _, output := range w.topicOutputMap {
					output.close()
				}
			} else {
				w.status = runStateStopped
			}

			w.unwatchCfgSignCh <- struct{}{}
			w.flushWait.Done()
			w.opOutputMu.Unlock()

			return
		}
	}
}

// write msg as many as we can
func (w *Writer) doWriteMost(msgList []*Msg) error {
	var total int
	for _, msg := range msgList {
		total += len(msg.buf)
	}
	for {
		select {
		case more := <-w.msgChan:
			msgList = append(msgList, more)
			total += len(more.buf)
			if total >= 2*1024*1024 {
				goto doWrite
			}
		default:
			goto doWrite
		}
	}
doWrite:
	return w.doWrite(msgList)
}

func (w *Writer) doWrite(msgList []*Msg) error {
	topicToMsgListMap := make(map[string][]*Msg)
	for _, msg := range msgList {
		topicMsgList := topicToMsgListMap[msg.topic]
		topicToMsgListMap[msg.topic] = append(topicMsgList, msg)
	}

	var lastErr error
	for topic, topicMsgList := range topicToMsgListMap {
		output, ok := w.topicOutputMap[topic]
		if !ok {
			newestSeq, err := scanDirToParseNewestSeq(w.baseDir, topic)
			if err != nil {
				lastErr = err
				continue
			}
			output = newOutput(w, topic, newestSeq)
			w.topicOutputMap[topic] = output
		}

		if err := output.write(topicMsgList); err != nil {
			lastErr = err
		}
	}

	if lastErr != nil {
		return lastErr
	}

	return nil
}

func (w *Writer) Resume() error {
	w.opOutputMu.Lock()
	defer w.opOutputMu.Unlock()

	if w.IsRunning() {
		return nil
	}

	// already exited, Not allow resume
	if w.IsExited() {
		return ErrWriterExited
	}

	w.status = runStateRunning
	go w.loop()

	return nil
}

func (w *Writer) Write(topic string, buf []byte) {
	w.msgChan <- &Msg{
		topic: topic,
		buf:   buf,
	}
}

func (w *Writer) Flush() {
	w.flushWait.Add(1)
	w.flushSignCh <- struct{}{}
	w.flushWait.Wait()
}

func (w *Writer) Stop() {
	w.flushWait.Add(1)
	w.status = runStateStopping
	w.flushSignCh <- struct{}{}
	w.flushWait.Wait()
}

func (w *Writer) Exit() {
	w.flushWait.Add(1)
	w.status = runStateExiting
	w.flushSignCh <- struct{}{}
	w.flushWait.Wait()
}

func (w *Writer) IsStopping() bool {
	return w.status == runStateRunning
}

func (w *Writer) IsStopped() bool {
	return w.status == runStateStopped
}

func (w *Writer) IsExiting() bool {
	return w.status == runStateExiting
}

func (w *Writer) IsExited() bool {
	return w.status == runStateExited
}

func (w *Writer) IsRunning() bool {
	return w.status == runStateRunning
}
