package logstream

import (
	"errors"
	"time"
)

const (
	topicDirPrefix     = "logstreamtopic_"
	refreshCfgInterval = 10 * time.Second
)

const (
	idxFileSuffix   = ".idx"
	dataFileSuffix  = ".dat"
	finishRcSuffix  = ".fin"
	pendingRcSuffix = ".pend"
	unPendRcSuffix  = ".unPend"
)

type runState int

const (
	runStateNil runState = iota
	runStateRunning
	runStateStopping
	runStateStopped
	runStateExiting
	runStateExited
)

var (
	errSeqNotFound   = errors.New("file sequence not found")
	errFileCorrupted = errors.New("oh wtf?? file occur corruption")
)

// Data file format is:
// item begin marker | data | item end marker
//
//	2                |   V  |       2
//
// Index file format is
// item begin marker | created at | data offset | data len |  enabled compress |   remain bytes | item end marker
//
//	2                |      4     |   4         |      4   |        1          |       15	    |	   2
const (
	idxBytes           = 32
	bufBoundariesBytes = 4
	bufBoundaryBytes   = 2
	bufBoundaryBegin   = uint16(0x1234)
	bufBoundaryEnd     = uint16(0x5678)
)
