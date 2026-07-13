package pq

import (
	"sync"
)

type PriorityChannel[T any] struct {
	mu    sync.Mutex
	queue *PriorityQueue[Comparator[ChannelMessage[T]], ChannelMessage[T]]
}
type ChannelMessage[T any] struct {
	Payload  T
	Priority int
}

func cmpChannelMessage[T any](a, b ChannelMessage[T]) int {
	return b.Priority - a.Priority
}

func NewPriorityChannel[T any]() *PriorityChannel[T] {
	pc := &PriorityChannel[T]{
		queue: NewPriorityQueue(cmpChannelMessage[T]),
	}
	return pc
}

func (pc *PriorityChannel[T]) Push(item T, priority int) {
	pc.mu.Lock()
	pc.queue.Push(ChannelMessage[T]{item, priority})
	pc.mu.Unlock()
}

func (pc *PriorityChannel[T]) Pop() (T, int, bool) {
	pc.mu.Lock()
	t, b := pc.queue.Pop()
	pc.mu.Unlock()
	return t.Payload, t.Priority, b
}

func (pc *PriorityChannel[T]) TryImmediatePop() (T, int, bool) {
	if pc.mu.TryLock() {
		t, b := pc.queue.Pop()
		pc.mu.Unlock()
		return t.Payload, t.Priority, b
	}
	var zeroVal T
	return zeroVal, -1, false
}
