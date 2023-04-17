package logstream

import (
	"encoding/binary"
	"github.com/golang/snappy"
	"os"
	"time"
)

func newOutput(writer *Writer, topic string, seq uint64) *Output {
	return &Output{
		Writer: writer,
		topic:  topic,
		seq:    seq,
	}
}

type Output struct {
	*Writer
	topic            string
	seq              uint64
	idxFp            *os.File
	dataFp           *os.File
	writtenIdxNum    uint32
	writtenDataBytes uint32
	openFileTime     time.Time
	idxBuf           []byte
	dataBuf          []byte
}

func (o *Output) isCorrupted() (bool, error) {
	if o.idxFp != nil {
		fileState, err := o.idxFp.Stat()
		if err != nil {
			return false, err
		}
		curBytes := uint32(fileState.Size())
		if curBytes%idxBytes != 0 {
			return true, nil
		}
		expectBytes := o.writtenIdxNum * idxBytes
		if expectBytes != curBytes {
			if curBytes < expectBytes {
				return true, nil
			} else {
				// reset index
				o.writtenIdxNum = curBytes / idxBytes
			}
		}
	}
	if o.dataFp != nil {
		fileState, err := o.dataFp.Stat()
		if err != nil {
			return false, err
		}
		curBytes := uint32(fileState.Size())
		if curBytes != o.writtenDataBytes {
			if curBytes < o.writtenDataBytes {
				return true, nil
			}
			// reset write bytes
			o.writtenDataBytes = curBytes
		}
	}
	return false, nil
}

func (o *Output) syncDisk() {
	if o.idxFp != nil {
		if err := o.idxFp.Sync(); err != nil {
			Logger.Warn(nil, err)
		}
	}
	if o.dataFp != nil {
		if err := o.dataFp.Sync(); err != nil {
			Logger.Warn(nil, err)
		}
	}
}

func (o *Output) isAtSameHourSinceLastOpenFile() bool {
	return o.openFileTime.Format("2006010215") == time.Now().Format("2006010215")
}

func (o *Output) write(msgList []*Msg) error {
	if len(msgList) == 0 {
		return nil
	}

	if o.idxFp == nil || o.dataFp == nil || !o.isAtSameHourSinceLastOpenFile() {
		if err := o.openNewFile(); err != nil {
			return err
		}
	}

	var (
		compressedFlagInIdx byte
		enabledCompress     bool
	)
	if enabledCompress = o.Writer.compressTopics.exist(o.topic); enabledCompress {
		compressedFlagInIdx = 1
	}
	bin := binary.LittleEndian
	for {
		accumIdxNum := o.writtenIdxNum
		oldAccumIdxNum := o.writtenIdxNum
		accumDataBytes := o.writtenDataBytes
		var (
			msg               *Msg
			needSwitchNewFile bool
		)
		for _, msg = range msgList {
			if !o.isAtSameHourSinceLastOpenFile() {
				needSwitchNewFile = true
				break
			}

			var incrementBytes uint32
			if enabledCompress {
				msg.compressed = snappy.Encode(nil, msg.buf)
				incrementBytes = uint32(len(msg.compressed))
			} else {
				incrementBytes = uint32(len(msg.buf))
			}

			incrementBytes += bufBoundariesBytes

			accumIdxNum++
			accumDataBytes += incrementBytes
			if (o.idxFileMaxItemNum == 0 || accumIdxNum <= o.idxFileMaxItemNum) && (o.dataFileMaxBytes == 0 || accumDataBytes <= o.dataFileMaxBytes) {
				continue
			}

			needSwitchNewFile = true
			accumIdxNum--
			accumDataBytes -= incrementBytes
			break
		}

		batchWriteIdxNum := accumIdxNum - oldAccumIdxNum

		// file had reached max size
		if batchWriteIdxNum <= 0 {
			if err := o.openNewFile(); err != nil {
				return err
			}
			continue
		}

		idxBufBytes := batchWriteIdxNum * idxBytes
		if uint32(len(o.idxBuf)) < idxBufBytes {
			o.idxBuf = make([]byte, idxBufBytes)
		}

		dataBufBytes := accumDataBytes - o.writtenDataBytes
		if uint32(len(o.dataBuf)) < dataBufBytes {
			o.dataBuf = make([]byte, dataBufBytes)
		}

		dataBuf := o.dataBuf
		idxBuf := o.idxBuf
		dataBufOffset := o.writtenDataBytes
		for i := uint32(0); i < batchWriteIdxNum; i++ {
			var buf []byte
			if !enabledCompress {
				buf = msgList[i].buf
			} else {
				buf = msgList[i].compressed
			}

			bufBytes := len(buf)

			bin.PutUint16(dataBuf[:bufBoundaryBytes], bufBoundaryBegin)
			copy(dataBuf[bufBoundaryBytes:], buf)
			bin.PutUint16(dataBuf[bufBoundaryBytes+bufBytes:], bufBoundaryEnd)
			dataItemBufBytes := bufBytes + bufBoundariesBytes
			dataBuf = dataBuf[dataItemBufBytes:]

			bin.PutUint16(idxBuf[:bufBoundaryBytes], bufBoundaryBegin)
			idxBuf = idxBuf[bufBoundaryBytes:]
			bin.PutUint32(idxBuf[0:4], uint32(time.Now().Unix())) // created at
			bin.PutUint32(idxBuf[4:8], dataBufOffset)             // offset
			bin.PutUint32(idxBuf[8:12], uint32(dataItemBufBytes)) // size
			idxBuf[12] = compressedFlagInIdx
			dataBufOffset += uint32(dataItemBufBytes)
			bin.PutUint16(idxBuf[28:], bufBoundaryEnd)
			idxBuf = idxBuf[28+bufBoundaryBytes:]
		}

		n, err := o.dataFp.Write(o.dataBuf[:dataBufBytes])
		if err != nil {
			return err
		}
		for uint32(n) < dataBufBytes {
			more, err := o.dataFp.Write(o.dataBuf[n:dataBufBytes])
			if err != nil {
				return err
			}
			n += more
		}

		n, err = o.idxFp.Write(o.idxBuf[:idxBufBytes])
		if err != nil {
			return err
		}
		for uint32(n) < idxBufBytes {
			more, err := o.idxFp.Write(o.idxBuf[n:idxBufBytes])
			if err != nil {
				return err
			}
			n += more
		}

		msgList = msgList[batchWriteIdxNum:]

		// all msg been written
		if !needSwitchNewFile || len(msgList) == 0 {
			o.writtenIdxNum = accumIdxNum
			o.writtenDataBytes = accumDataBytes
			break
		}

		if err = o.openNewFile(); err != nil {
			return err
		}
	}

	return nil
}

func (o *Output) close() {
	if o.idxFp != nil {
		_ = o.idxFp.Close()
	}
	if o.dataFp != nil {
		_ = o.dataFp.Close()
	}
	o.writtenDataBytes = 0
	o.writtenIdxNum = 0
	o.openFileTime = time.Time{}
}

func (o *Output) openNewFile() error {
	curSeq := o.seq
	if o.idxFp != nil && o.dataFp != nil {
		curSeq++
	}

	idxFp, err := makeSeqIdxFp(o.baseDir, o.topic, curSeq, os.O_CREATE|os.O_APPEND)
	if err != nil {
		return err
	}

	dataFp, err := makeSeqDataFp(o.baseDir, o.topic, curSeq, os.O_CREATE|os.O_APPEND)
	if err != nil {
		return err
	}

	o.close()

	o.seq = curSeq
	o.idxFp = idxFp
	o.dataFp = dataFp
	o.openFileTime = time.Now()

	idxFileState, err := o.idxFp.Stat()
	if err != nil {
		return err
	}

	dataFileState, err := o.dataFp.Stat()
	if err != nil {
		return err
	}

	if idxFileState.Size()%idxBytes > 0 {
		return errFileCorrupted
	}

	o.writtenIdxNum = uint32(idxFileState.Size() / int64(idxBytes))
	o.writtenDataBytes = uint32(dataFileState.Size())

	return nil
}
