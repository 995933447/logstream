package logstream

import (
	"encoding/binary"
	"fmt"
	"os"
)

const (
	finishWaterMarkBytes         = 12
	seqInFinishWaterMarkBytes    = 8
	idxNumInFinishWaterMarkBytes = 4
)

func newConsumeWaterMarkRec(baseDir, topic string) (*ConsumeWaterMarkRec, error) {
	rec := &ConsumeWaterMarkRec{
		baseDir: baseDir,
		topic:   topic,
	}

	topicDir := fmt.Sprintf(getTopicFileDir(baseDir, topic))
	if err := mkdirIfNotExist(topicDir); err != nil {
		return nil, err
	}

	var (
		err error
	)
	rec.fp, err = makeFinishRcFp(baseDir, topic)
	if err != nil {
		return nil, err
	}

	fileState, err := rec.fp.Stat()
	if err != nil {
		return nil, err
	}

	if fileState.Size() <= 0 {
		seq, err := scanDirToParseOldestSeq(baseDir, topic)
		if err != nil {
			return nil, err
		}
		err = rec.updateWaterMark(seq, 0)
		if err != nil {
			return nil, err
		}
	} else if _, _, err = rec.refreshAndGetOffset(); err != nil {
		return nil, err
	}

	rec.idxFp, err = makeSeqIdxFp(baseDir, topic, rec.seq, os.O_CREATE|os.O_RDONLY)
	if err != nil {
		return nil, err
	}

	return rec, nil
}

type FinishWaterMark struct {
	seq    uint64
	idxNum uint32
}

type ConsumeWaterMarkRec struct {
	baseDir string
	topic   string
	fp      *os.File
	idxFp   *os.File
	buf     [finishWaterMarkBytes]byte
	FinishWaterMark
}

func (r *ConsumeWaterMarkRec) isOffsetsFinishedInSeq() (bool, error) {
	newestSeq, err := scanDirToParseNewestSeq(r.baseDir, r.topic)
	if err != nil {
		return false, err
	}

	if newestSeq == r.seq {
		return false, nil
	}

	idxFileState, err := r.idxFp.Stat()
	if err != nil {
		return false, err
	}

	return idxFileState.Size()/idxBytes == int64(r.idxNum), nil
}

func (r *ConsumeWaterMarkRec) refreshAndGetOffset() (uint64, uint32, error) {
	n, err := r.fp.Read(r.buf[:])
	if err != nil {
		return 0, 0, err
	}

	for {
		if n >= finishWaterMarkBytes {
			break
		}

		more, err := r.fp.Read(r.buf[n:])
		if err != nil {
			return 0, 0, err
		}

		n += more
	}

	bin := binary.LittleEndian
	r.seq = bin.Uint64(r.buf[:seqInFinishWaterMarkBytes])
	r.idxNum = bin.Uint32(r.buf[seqInFinishWaterMarkBytes:])

	return r.seq, r.idxNum, nil
}

func (r *ConsumeWaterMarkRec) getWaterMark() (uint64, uint32) {
	return r.seq, r.idxNum
}

func (r *ConsumeWaterMarkRec) syncDisk() {
	if err := r.fp.Sync(); err != nil {
		Logger.Warn(nil, err)
	}
}

func (r *ConsumeWaterMarkRec) updateWaterMark(seq uint64, idxNum uint32) error {
	var err error
	if seq != r.seq {
		r.idxFp, err = makeSeqIdxFp(r.baseDir, r.topic, seq, os.O_RDONLY)
		if err != nil {
			return err
		}

		idxFileState, err := r.idxFp.Stat()
		if err != nil {
			return err
		}

		maxIdxNum := uint32(idxFileState.Size() / idxBytes)
		if maxIdxNum < idxNum {
			idxNum = maxIdxNum
		}
	}

	bin := binary.LittleEndian
	bin.PutUint64(r.buf[:seqInFinishWaterMarkBytes], seq)
	bin.PutUint32(r.buf[seqInFinishWaterMarkBytes:], idxNum)
	n, err := r.fp.WriteAt(r.buf[:], 0)
	if err != nil {
		return err
	}

	for {
		if n >= finishWaterMarkBytes {
			break
		}
		more, err := r.fp.Write(r.buf[:])
		if err != nil {
			return err
		}
		n += more
	}

	r.seq, r.idxNum = seq, idxNum

	return nil
}
