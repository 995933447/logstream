package logstream

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// item begin marker | seq | idx offset | item end marker
//
//	2                |  8  |   4        | 	   2
const (
	seqInPendingIdxBufBytes = 8
	pendingIdxBufBytes      = 12 + bufBoundariesBytes
)

type pendingMsgIdx struct {
	seq       uint64
	idxOffset uint32
}

type ConsumePendingRec struct {
	baseDir         string
	topic           string
	pendingMsgIdxes map[uint64]map[uint32]struct{}
	unPendMsgIdxes  map[uint64]map[uint32]struct{}
	pendingFp       *os.File
	unPendFp        *os.File
	pendingBuf      []byte
	unPendBuf       []byte
}

func newConsumePendingRec(baseDir, topic string) (*ConsumePendingRec, error) {
	rec := &ConsumePendingRec{
		baseDir:         baseDir,
		topic:           topic,
		pendingMsgIdxes: map[uint64]map[uint32]struct{}{},
		unPendMsgIdxes:  map[uint64]map[uint32]struct{}{},
	}

	topicDir := fmt.Sprintf(getTopicFileDir(baseDir, topic))
	if err := mkdirIfNotExist(topicDir); err != nil {
		return nil, err
	}

	var err error
	rec.pendingFp, rec.unPendFp, err = makePendingRcFps(baseDir, topic)
	if err != nil {
		return nil, err

	}

	if err = rec.load(); err != nil {
		return nil, err
	}

	return rec, nil
}

func (r *ConsumePendingRec) syncDisk() {
	_ = r.pendingFp.Sync()
	_ = r.unPendFp.Sync()
}

func (r *ConsumePendingRec) isConfirmed(seq uint64, idxOffset uint32) bool {
	if idxSet, ok := r.unPendMsgIdxes[seq]; ok {
		if _, ok = idxSet[idxOffset]; ok {
			return true
		}
	}

	return false
}

func (r *ConsumePendingRec) isPending(seq uint64, idxOffset uint32) bool {
	if idxSet, ok := r.pendingMsgIdxes[seq]; !ok {
		return false
	} else if _, ok = idxSet[idxOffset]; !ok {
		return false
	}

	return true
}

func (r *ConsumePendingRec) isEmpty() bool {
	return len(r.pendingMsgIdxes) == 0
}

func (r *ConsumePendingRec) load() error {
	pendings, err := r.loadPendings(false)
	if err != nil {
		return err
	}

	if err = r.pending(pendings, true); err != nil {
		return err
	}

	unPends, err := r.loadPendings(true)
	if err != nil {
		return err
	}

	if err = r.unPend(unPends, true); err != nil {
		return err
	}

	return nil
}

func (r *ConsumePendingRec) loadPendings(isLoadUnPend bool) ([]*pendingMsgIdx, error) {
	var cursor int64
	bin := binary.LittleEndian
	var (
		batchBufSize = pendingIdxBufBytes * 170
		pendings     []*pendingMsgIdx
		fp           *os.File
	)
	if isLoadUnPend {
		fp = r.unPendFp
	} else {
		fp = r.pendingFp
	}
	for {
		pendingBuf := make([]byte, batchBufSize)
		var isEOF bool
		n, err := fp.ReadAt(pendingBuf, cursor)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			isEOF = true
		}

		if n%pendingIdxBufBytes > 0 {
			return nil, errFileCorrupted
		}

		pendingBuf = pendingBuf[:n]
		if len(pendingBuf) == 0 {
			break
		}

		for {
			boundaryBegin := bin.Uint16(pendingBuf[:bufBoundaryBytes])
			boundaryEnd := bin.Uint16(pendingBuf[pendingIdxBufBytes-bufBoundaryBytes:])
			if boundaryBegin != bufBoundaryBegin || boundaryEnd != bufBoundaryEnd {
				return nil, errFileCorrupted
			}

			seq := bin.Uint64(pendingBuf[bufBoundaryBytes : bufBoundaryBytes+8])
			idxOffset := bin.Uint32(pendingBuf[bufBoundaryBytes+8 : bufBoundaryBytes+12])
			pendings = append(pendings, &pendingMsgIdx{
				seq:       seq,
				idxOffset: idxOffset,
			})
			pendingBuf = pendingBuf[pendingIdxBufBytes:]
			if len(pendingBuf) < pendingIdxBufBytes {
				break
			}
		}

		if n < batchBufSize {
			break
		}

		if isEOF {
			break
		}

		cursor += int64(n)
	}

	return pendings, nil
}

func (r *ConsumePendingRec) pending(pendings []*pendingMsgIdx, onlyPendOnMem bool) error {
	var enqueued []*pendingMsgIdx
	for _, pending := range pendings {
		if unPendIdxSet, ok := r.unPendMsgIdxes[pending.seq]; ok {
			if _, ok = unPendIdxSet[pending.idxOffset]; ok {
				continue
			}
		}

		if unPendIdxSet, ok := r.unPendMsgIdxes[pending.seq]; ok {
			if _, ok = unPendIdxSet[pending.idxOffset]; ok {
				continue
			}
		}

		pendingIdxSet, ok := r.pendingMsgIdxes[pending.seq]
		if !ok {
			pendingIdxSet = map[uint32]struct{}{}
			r.pendingMsgIdxes[pending.seq] = pendingIdxSet
		} else {
			if _, ok = pendingIdxSet[pending.idxOffset]; ok {
				continue
			}
			pendingIdxSet[pending.idxOffset] = struct{}{}
		}

		enqueued = append(enqueued, pending)
	}

	if !onlyPendOnMem {
		if err := r.write(false, enqueued); err != nil {
			return err
		}
	}

	for _, pending := range enqueued {
		r.pendingMsgIdxes[pending.seq][pending.idxOffset] = struct{}{}
	}

	return nil
}

func (r *ConsumePendingRec) unPend(pendings []*pendingMsgIdx, onlyUnPendMem bool) error {
	var enqueued []*pendingMsgIdx
	for _, pending := range pendings {
		unPendIdxSet, ok := r.unPendMsgIdxes[pending.seq]
		if !ok {
			unPendIdxSet = map[uint32]struct{}{}
			r.unPendMsgIdxes[pending.seq] = unPendIdxSet
		} else {
			if _, ok = unPendIdxSet[pending.idxOffset]; ok {
				return nil
			}
			unPendIdxSet[pending.idxOffset] = struct{}{}
		}
		enqueued = append(enqueued, pending)
	}

	if !onlyUnPendMem {
		if err := r.write(true, enqueued); err != nil {
			return err
		}
	}

	for _, pending := range enqueued {
		r.unPendMsgIdxes[pending.seq][pending.idxOffset] = struct{}{}
		pendingMsgIdxes, ok := r.pendingMsgIdxes[pending.seq]
		if !ok {
			return nil
		}
		delete(pendingMsgIdxes, pending.idxOffset)
		if len(pendingMsgIdxes) > 0 {
			continue
		}
		delete(r.pendingMsgIdxes, pending.seq)
	}

	if r.isEmpty() {
		if err := r.pendingFp.Truncate(0); err == nil {
			if _, err = r.pendingFp.Seek(0, 0); err != nil {
				return err
			}
		}

		if err := r.unPendFp.Truncate(0); err == nil {
			if _, err = r.unPendFp.Seek(0, 0); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *ConsumePendingRec) write(isUnPend bool, pendings []*pendingMsgIdx) error {
	needBufLen := len(pendings) * pendingIdxBufBytes
	var bufLen int
	if !isUnPend {
		bufLen = len(r.pendingBuf)
	} else {
		bufLen = len(r.unPendBuf)
	}

	var totalBuf []byte
	needExpandBuf := bufLen < needBufLen
	if isUnPend {
		if needExpandBuf {
			r.unPendBuf = make([]byte, needBufLen)
		}
		totalBuf = r.unPendBuf
	} else {
		if needExpandBuf {
			r.pendingBuf = make([]byte, needBufLen)
		}
		totalBuf = r.pendingBuf
	}

	buf := totalBuf

	bin := binary.LittleEndian
	for _, pending := range pendings {
		bin.PutUint16(buf[:bufBoundaryBytes], bufBoundaryBegin)
		bin.PutUint64(buf[bufBoundaryBytes:bufBoundaryBytes+seqInPendingIdxBufBytes], pending.seq)
		bin.PutUint32(buf[bufBoundaryBytes+seqInPendingIdxBufBytes:], pending.idxOffset)
		bin.PutUint16(buf[pendingIdxBufBytes-bufBoundaryBytes:], bufBoundaryEnd)
		buf = buf[pendingIdxBufBytes:]
	}

	var (
		total int
		fp    *os.File
	)
	if isUnPend {
		fp = r.unPendFp
	} else {
		fp = r.pendingFp
	}
	for {
		n, err := fp.Write(totalBuf[total:needBufLen])
		if err != nil {
			return err
		}

		total += n

		if total >= needBufLen {
			break
		}
	}

	return nil
}
