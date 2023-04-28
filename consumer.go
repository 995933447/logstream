package logstream

import (
	"encoding/binary"
	"errors"
	"github.com/fsnotify/fsnotify"
	"github.com/golang/snappy"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var errConsumerNotSubscribedTopic = errors.New("consumer not subscribed topic")

func newConsumer(reader *Reader, topic string) (*Consumer, error) {
	consumer := &Consumer{
		Reader:            reader,
		topic:             topic,
		confirmMsgCh:      make(chan *confirmMsgReq),
		unsubscribeSignCh: make(chan struct{}),
	}

	finishRec, err := newConsumeWaterMarkRec(reader.baseDir, topic)
	if err != nil {
		return nil, err
	}

	consumer.finishRec = finishRec

	seq, idxNum := finishRec.getWaterMark()

	consumer.nextIdxCursor = idxNum

	consumer.idxFp, err = makeSeqIdxFp(consumer.baseDir, consumer.topic, seq, os.O_CREATE|os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	consumer.dataFp, err = makeSeqDataFp(consumer.baseDir, consumer.topic, seq, os.O_CREATE|os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	if err = consumer.refreshMsgNum(); err != nil {
		return nil, err
	}

	consumer.pendingRec, err = newConsumePendingRec(reader.baseDir, topic)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

type confirmMsgReq struct {
	seq       uint64
	idxOffset uint32
}

type Consumer struct {
	*Reader
	topic               string
	status              runState
	idxFp               *os.File
	dataFp              *os.File
	nextIdxCursor       uint32
	finishRec           *ConsumeWaterMarkRec
	opFinishRecMu       sync.RWMutex
	pendingRec          *ConsumePendingRec
	opPendingRecMu      sync.RWMutex
	msgNum              uint32
	isWaitingMsgConfirm atomic.Value // bool
	unsubscribeSignCh   chan struct{}
	confirmMsgCh        chan *confirmMsgReq
}

func (c *Consumer) switchNextSeqFile() error {
	nextSeq, err := scanDirToParseNextSeq(c.baseDir, c.topic, c.finishRec.seq)
	if err != nil {
		return err
	}

	idxFp, err := makeSeqIdxFp(c.baseDir, c.topic, nextSeq, os.O_RDONLY)
	if err != nil {
		return err
	}

	dataFp, err := makeSeqDataFp(c.baseDir, c.topic, nextSeq, os.O_RDONLY)
	if err != nil {
		return err
	}

	err = c.finishRec.updateWaterMark(nextSeq, 0)
	if err != nil {
		return err
	}

	c.idxFp = idxFp
	c.dataFp = dataFp
	c.nextIdxCursor = 0
	if err = c.refreshMsgNum(); err != nil {
		return err
	}

	return nil
}

func (c *Consumer) IsWaitingMsgConfirm() bool {
	isWaiting := c.isWaitingMsgConfirm.Load()
	if isWaiting != nil {
		return isWaiting.(bool)
	}
	return false
}

func (c *Consumer) subscribe() error {
	fileWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	if c.isSubscribed() {
		return errors.New("already in running")
	}

	c.status = runStateRunning

	// windows no fsnotify
	directlyTrySwitchFileTk := time.NewTicker(time.Second * 2)
	defer directlyTrySwitchFileTk.Stop()
	var needWatchNewFile = true
	for {
		needSwitchNewFile, err := c.finishRec.isOffsetsFinishedInSeq()
		if err != nil {
			Logger.Error(nil, err)
			time.Sleep(time.Millisecond * 500)
			continue
		}

		if needSwitchNewFile {
			for {
				if err = c.switchNextSeqFile(); err != nil {
					if err == errSeqNotFound {
						time.Sleep(time.Second)
						continue
					}

					Logger.Error(nil, err)
					time.Sleep(time.Millisecond * 500)
					continue
				}
				needWatchNewFile = true
				break
			}
		}

		if !c.IsWaitingMsgConfirm() && c.nextIdxCursor < c.msgNum {
			c.isWaitingMsgConfirm.Store(true)
			c.Reader.schedCh <- c
			continue
		}

		if needWatchNewFile {
			Logger.Debug(nil, "watched new index file:"+c.idxFp.Name())
			if err = fileWatcher.Add(c.idxFp.Name()); err != nil {
				Logger.Error(nil, err)
				time.Sleep(time.Second)
				continue
			}

			needWatchNewFile = false
			if err = c.refreshMsgNum(); err != nil {
				Logger.Error(nil, err)
			}
			continue
		}

		Logger.Debug(nil, c.topic+" loop into select")
		select {
		case <-directlyTrySwitchFileTk.C:
		case event := <-fileWatcher.Events:
			Logger.Debug(nil, "watch file changed")
			if event.Has(fsnotify.Chmod) {
				break
			}

			if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
				needSwitchNewFile = true
				break
			}

			if err = c.refreshMsgNum(); err != nil {
				Logger.Debug(nil, err)
			}
		case <-c.unsubscribeSignCh:
			Logger.Debug(nil, "unwatched")
			c.status = runStateExited
			goto out
		case confirmed := <-c.confirmMsgCh:
			Logger.Debug(nil, "rev confirmed")
			var confirmedList []*confirmMsgReq
			confirmedList = append(confirmedList, confirmed)
			for {
				select {
				case more := <-c.confirmMsgCh:
					confirmedList = append(confirmedList, more)
				default:
				}
				break
			}

			var (
				confirmedMax *confirmMsgReq
				unPends      []*pendingMsgIdx
			)
			for _, confirmed := range confirmedList {
				unPends = append(unPends, &pendingMsgIdx{
					seq:       confirmed.seq,
					idxOffset: confirmed.idxOffset,
				})

				if confirmedMax == nil {
					confirmedMax = confirmed
					continue
				}
				if confirmed.seq > confirmedMax.seq {
					confirmedMax = confirmed
					continue
				}
				if confirmed.seq == confirmedMax.seq && confirmed.idxOffset > confirmedMax.idxOffset {
					confirmedMax = confirmed
				}
			}

			if notPending, err := c.unPendMsg(unPends); err != nil {
				Logger.Error(nil, err)
				break
			} else if !notPending {
				break
			}

			if err = c.updateFinishWaterMark(confirmedMax.seq, confirmedMax.idxOffset); err != nil {
				Logger.Error(nil, err)
				break
			}

			if confirmedMax.idxOffset < c.nextIdxCursor-1 {
				break
			}

			c.isWaitingMsgConfirm.Store(false)
		}
	}
out:
	return nil
}

func (c *Consumer) refreshMsgNum() error {
	idxFileState, err := c.idxFp.Stat()
	if err != nil {
		return err
	}
	c.msgNum = uint32(idxFileState.Size()) / idxBytes
	return nil
}

type PoppedMsgItem struct {
	Topic     string
	Seq       uint64
	IdxOffset uint32
	Data      []byte
	CreatedAt int64
}

func (c *Consumer) consumeBatch() ([]*PoppedMsgItem, bool, error) {
	if !c.isSubscribed() {
		return nil, false, errConsumerNotSubscribedTopic
	}

	if c.msgNum <= c.nextIdxCursor {
		return nil, false, nil
	}

	var (
		items          []*PoppedMsgItem
		pendings       []*pendingMsgIdx
		totalDataBytes int
		bin            = binary.LittleEndian
	)
	for {
		if totalDataBytes > 2*1024*1024 {
			break
		}

		if c.msgNum <= c.nextIdxCursor {
			break
		}

		idxBuf := make([]byte, idxBytes)
		seekIdxBufOffset := c.nextIdxCursor * idxBytes
		var isEOF bool
		_, err := c.idxFp.ReadAt(idxBuf, int64(seekIdxBufOffset))
		if err != nil {
			if err != io.EOF {
				return nil, false, err
			}
			isEOF = true
		}

		if len(idxBuf) == 0 {
			break
		}

		boundaryBegin := bin.Uint16(idxBuf[:bufBoundaryBytes])
		boundaryEnd := bin.Uint16(idxBuf[idxBytes-bufBoundaryBytes:])
		if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
			return nil, false, errFileCorrupted
		}

		createdAt := bin.Uint32(idxBuf[bufBoundaryBytes : bufBoundaryBytes+4])
		offset := bin.Uint32(idxBuf[bufBoundaryBytes+4 : bufBoundaryBytes+8])
		dataBytes := bin.Uint32(idxBuf[bufBoundaryBytes+8 : bufBoundaryBytes+12])
		isCompressedFlag := idxBuf[bufBoundaryBytes+12]

		dataBuf := make([]byte, dataBytes)
		_, err = c.dataFp.ReadAt(dataBuf, int64(offset))
		if err != nil {
			if err != io.EOF {
				return nil, false, err
			}
			isEOF = true
		}

		if len(dataBuf) == 0 {
			break
		}

		boundaryBegin = bin.Uint16(dataBuf[:bufBoundaryBytes])
		boundaryEnd = bin.Uint16(dataBuf[dataBytes-bufBoundaryBytes:])
		if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
			return nil, false, errFileCorrupted
		}

		data := dataBuf[bufBoundaryBytes : dataBytes-bufBoundaryBytes]
		if isCompressedFlag == 1 {
			data, err = snappy.Decode(nil, data)
			if err != nil {
				return nil, false, err
			}
		}

		seq, _ := c.finishRec.getWaterMark()
		items = append(items, &PoppedMsgItem{
			Topic:     c.topic,
			IdxOffset: c.nextIdxCursor,
			Seq:       seq,
			Data:      data,
			CreatedAt: int64(createdAt),
		})
		pendings = append(pendings, &pendingMsgIdx{
			seq:       seq,
			idxOffset: c.nextIdxCursor,
		})

		if isEOF {
			break
		}

		totalDataBytes += len(data)
		c.nextIdxCursor++
	}

	if err := c.pendingRec.pending(pendings, false); err != nil {
		return nil, false, err
	}

	return items, true, nil
}

func (c *Consumer) unsubscribe() {
	c.unsubscribeSignCh <- struct{}{}
}

func (c *Consumer) isSubscribed() bool {
	return c.status == runStateRunning
}

func (c *Consumer) confirmMsg(seq uint64, idxOffset uint32) {
	if c.isSubscribed() {
		c.confirmMsgCh <- &confirmMsgReq{
			seq:       seq,
			idxOffset: idxOffset,
		}
		return
	}

	if notPending, err := c.unPendMsg([]*pendingMsgIdx{{seq: seq, idxOffset: idxOffset}}); err != nil {
		Logger.Error(nil, err)
		return
	} else if !notPending {
		return
	}
	if err := c.updateFinishWaterMark(seq, idxOffset); err != nil {
		Logger.Error(nil, err)
	}
}

func (c *Consumer) isNotConfirmed(seq uint64, idxOffset uint32) bool {
	if c.pendingRec.isEmpty() {
		return false
	}

	return c.pendingRec.isPending(seq, idxOffset)
}

func (c *Consumer) unPendMsg(pendings []*pendingMsgIdx) (bool, error) {
	c.opPendingRecMu.Lock()
	defer c.opPendingRecMu.Unlock()

	if err := c.pendingRec.unPend(pendings, false); err != nil {
		return false, err
	}

	if !c.pendingRec.isEmpty() {
		return false, nil
	}

	return true, nil
}

func (c *Consumer) updateFinishWaterMark(seq uint64, offset uint32) error {
	c.opFinishRecMu.Lock()
	defer c.opFinishRecMu.Unlock()

	consumingSeq, consumedIdxNum := c.finishRec.getWaterMark()

	if consumingSeq > seq {
		return nil
	}

	if consumingSeq == seq && consumedIdxNum >= offset+1 {
		return nil
	}

	if err := c.finishRec.updateWaterMark(seq, offset+1); err != nil {
		return err
	}

	return nil
}
