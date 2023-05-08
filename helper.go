package logstream

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func genIdxFileName(baseDir, topic string, seq uint64) string {
	return fmt.Sprintf("%s/%s_%d"+idxFileSuffix, getTopicFileDir(baseDir, topic), time.Now().Format("2006010215"), seq)
}

func genDataFileName(baseDir, topic string, seq uint64) string {
	return fmt.Sprintf("%s/%s_%d"+dataFileSuffix, getTopicFileDir(baseDir, topic), time.Now().Format("2006010215"), seq)
}

func genFinishRcFileName(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s"+finishRcSuffix, getTopicFileDir(baseDir, topic), time.Now().Format("2006010215"))
}

func genPendingRcFileName(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s"+pendingRcSuffix, getTopicFileDir(baseDir, topic), time.Now().Format("2006010215"))
}

func genUnPendRcFileName(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s"+unPendRcSuffix, getTopicFileDir(baseDir, topic), time.Now().Format("2006010215"))
}

func getTopicFileDir(baseDir, topic string) string {
	return fmt.Sprintf("%s/%s%s", baseDir, topicDirPrefix, topic)
}

func scanDirToParseTopics(dir string, checkTopicValid func(topic string) bool) ([]string, error) {
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var topics []string
	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		fileName := file.Name()
		if !strings.HasPrefix(fileName, topicDirPrefix) {
			continue
		}

		topic := fileName[len(topicDirPrefix):]

		if !checkTopicValid(topic) {
			continue
		}

		topics = append(topics, topic)
	}

	return topics, nil
}

func mkdirIfNotExist(dir string) error {
	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		if err = os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}

func makeSeqIdxFp(baseDir, topic string, seq uint64, flag int) (*os.File, error) {
	dir := getTopicFileDir(baseDir, topic)
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), idxFileSuffix) {
			continue
		}

		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		var curSeq uint64
		if seqStr != "" {
			var err error
			curSeq, err = strconv.ParseUint(seqStr, 10, 64)
			if err != nil {
				return nil, err
			}
		}

		if curSeq != seq {
			continue
		}

		return os.OpenFile(dir+"/"+file.Name(), flag, os.ModePerm)
	}

	return os.OpenFile(genIdxFileName(baseDir, topic, seq), flag, os.ModePerm)
}

func makeSeqDataFp(baseDir, topic string, seq uint64, flag int) (*os.File, error) {
	dir := getTopicFileDir(baseDir, topic)
	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), dataFileSuffix) {
			continue
		}

		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		if seqStr == "" {
			continue
		}

		curSeq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			return nil, err
		}

		if curSeq != seq {
			continue
		}

		return os.OpenFile(dir+"/"+file.Name(), flag, os.ModePerm)
	}

	return os.OpenFile(genDataFileName(baseDir, topic, seq), flag, os.ModePerm)
}

func makePendingRcFps(baseDir, topic string) (*os.File, *os.File, error) {
	dir := getTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}

	var pendingFp, unPendFp *os.File
	for _, file := range files {
		if pendingFp == nil && strings.HasSuffix(file.Name(), pendingRcSuffix) {
			pendingFp, err = os.OpenFile(dir+"/"+file.Name(), os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
			if err != nil {
				return nil, nil, err
			}
		}

		if unPendFp == nil && strings.HasSuffix(file.Name(), unPendRcSuffix) {
			unPendFp, err = os.OpenFile(dir+"/"+file.Name(), os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
			if err != nil {
				return nil, nil, err
			}
		}

		if pendingFp != nil && unPendFp != nil {
			return pendingFp, unPendFp, nil
		}
	}

	pendingFp, err = os.OpenFile(genPendingRcFileName(baseDir, topic), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, nil, err
	}

	unPendFp, err = os.OpenFile(genUnPendRcFileName(baseDir, topic), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, nil, err
	}

	return pendingFp, unPendFp, nil
}

func makeFinishRcFp(baseDir, topic string) (*os.File, error) {
	dir := getTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), finishRcSuffix) {
			continue
		}

		fp, err := os.OpenFile(dir+"/"+file.Name(), os.O_RDWR, os.ModePerm)
		if err != nil {
			return nil, err
		}

		return fp, nil
	}

	return os.OpenFile(genFinishRcFileName(baseDir, topic), os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func scanDirToParseOldestSeq(baseDir, topic string) (uint64, error) {
	dir := getTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return 0, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	var oldestSeq uint64 = 1
	for _, file := range files {
		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		curSeq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			return 0, err
		}

		if curSeq < oldestSeq {
			oldestSeq = curSeq
		}
	}

	return oldestSeq, nil
}

func scanDirToParseNewestSeq(baseDir, topic string) (uint64, error) {
	dir := getTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return 0, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	var newestSeq uint64 = 1
	for _, file := range files {
		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		var curSeq uint64
		if seqStr != "" {
			curSeq, err = strconv.ParseUint(seqStr, 10, 64)
			if err != nil {
				return 0, err
			}
		}

		if curSeq > newestSeq {
			newestSeq = curSeq
		}
	}

	return newestSeq, nil
}

func scanDirToParseNextSeq(baseDir, topic string, seq uint64) (uint64, error) {
	dir := getTopicFileDir(baseDir, topic)

	if err := mkdirIfNotExist(dir); err != nil {
		return 0, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	var nextSeq uint64
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), idxFileSuffix) {
			continue
		}

		seqStr, ok := parseFileSeqStr(file)
		if !ok {
			continue
		}

		if seqStr == "" {
			continue
		}

		curSeq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			return 0, err
		}

		if curSeq <= seq {
			continue
		}

		if nextSeq == 0 {
			nextSeq = curSeq
			continue
		}

		if curSeq < nextSeq {
			nextSeq = curSeq
		}
	}

	if nextSeq == 0 {
		return 0, errSeqNotFound
	}

	return nextSeq, nil
}

func parseMemSizeStrToBytes(size string) (uint32, error) {
	size = strings.ToUpper(size)
	switch true {
	case strings.HasSuffix(size, "KB"), strings.HasSuffix(size, "K"):
		sizeVal, err := strconv.ParseUint(strings.TrimRight(strings.TrimRight(size, "K"), "KB"), 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(sizeVal) * 1024, nil
	case strings.HasSuffix(size, "M"), strings.HasSuffix(size, "MB"):
		sizeVal, err := strconv.ParseUint(strings.TrimRight(strings.TrimRight(size, "M"), "MB"), 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(sizeVal) * 1024 * 1024, nil
	case strings.HasSuffix(size, "G"), strings.HasSuffix(size, "GB"):
		sizeVal, err := strconv.ParseUint(strings.TrimRight(strings.TrimRight(size, "G"), "GB"), 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(sizeVal) * 1024 * 1024 * 1024, nil
	case strings.HasSuffix(size, "B"):
		sizeVal, err := strconv.ParseUint(strings.TrimRight(size, "B"), 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(sizeVal), nil
	}
	sizeVal, err := strconv.ParseUint(size, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(sizeVal), nil
}

func parseFileSeqStr(file os.DirEntry) (string, bool) {
	if file.IsDir() {
		return "", false
	}

	fileName := file.Name()

	suffixPos := strings.IndexByte(fileName, '.')
	if suffixPos <= 0 {
		return "", false
	}

	fileNameWithoutSuffix := fileName[:suffixPos]
	fileNameChunk := strings.Split(fileNameWithoutSuffix, "_")
	if len(fileNameChunk) != 2 {
		return "", false
	}

	return fileNameChunk[1], true
}
