package logstream

import (
	"container/heap"
	"time"
)

type retryMsgQueue [][]*PoppedMsgItem

func newRetryMsgQueue(itemsList [][]*PoppedMsgItem) *retryMsgQueue {
	var q retryMsgQueue
	for _, items := range itemsList {
		q = append(q, items)
	}

	heap.Init(&q)
	return &q
}

func (q *retryMsgQueue) popRetry() []*PoppedMsgItem {
	if len(*q) == 0 {
		return nil
	}
	if (*q)[0][0].RetryAt > uint32(time.Now().Unix()) {
		return nil
	}
	return heap.Pop(q).([]*PoppedMsgItem)
}

func (q *retryMsgQueue) pushRetry(retry []*PoppedMsgItem) {
	heap.Push(q, retry)
}

func (q *retryMsgQueue) Len() int {
	return len(*q)
}

func (q retryMsgQueue) Swap(i, j int) {
	tmp := q[i]
	q[i] = q[j]
	q[j] = tmp
}

func (q retryMsgQueue) Less(i, j int) bool {
	return q[i][0].RetryAt < q[j][0].RetryAt
}

func (q *retryMsgQueue) Push(x interface{}) {
	*q = append(*q, x.([]*PoppedMsgItem))
}

func (q *retryMsgQueue) Pop() interface{} {
	queenLen := len(*q)
	if queenLen == 0 {
		return nil
	}
	lastItemIndex := queenLen - 1
	item := (*q)[lastItemIndex]
	*q = (*q)[:lastItemIndex]
	return item
}

func (q retryMsgQueue) Peek() []*PoppedMsgItem {
	return q[0]
}
