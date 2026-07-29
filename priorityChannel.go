package pq

import (
	"context"
	"sync"
)

type PriorityChannel[T any] struct {
	mu    sync.Mutex
	cond  *sync.Cond
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
	pc.cond = sync.NewCond(&pc.mu)
	return pc
}

func (pc *PriorityChannel[T]) Push(item T, priority int) {
	pc.mu.Lock()
	pc.queue.Push(ChannelMessage[T]{item, priority})
	pc.mu.Unlock()
	pc.cond.Signal()
}

func (pc *PriorityChannel[T]) Pop() (T, int, bool) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	t, b := pc.queue.Pop()
	return t.Payload, t.Priority, b
}

// PopBlocking blocks until queue isn't empty
func (pc *PriorityChannel[T]) PopBlocking() (T, int) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for pc.queue.Len() == 0 {
		pc.cond.Wait()
	}
	t, _ := pc.queue.Pop()
	return t.Payload, t.Priority
}

func (pc *PriorityChannel[T]) PopBlockingWithCancel(ctx context.Context) (T, int, error) {
	stop := context.AfterFunc(ctx, pc.cond.Broadcast)
	defer stop()

	pc.mu.Lock()
	defer pc.mu.Unlock()
	for pc.queue.Len() == 0 {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, 0, err
		}
		pc.cond.Wait()
	}
	t, _ := pc.queue.Pop()
	return t.Payload, t.Priority, nil
}

func (pc *PriorityChannel[T]) PopWithCancel(ctx context.Context) (T, int, bool, error) {
	var ti ChannelMessage[T]
	var b bool
	unlocked := make(chan struct{}, 1)
	done := make(chan struct{}, 1)
	go func() {
		pc.mu.Lock()
		unlocked <- struct{}{}
		close(unlocked)
		<-done
		pc.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		done <- struct{}{}
	case <-unlocked:
		ti, b = pc.queue.Pop()
		done <- struct{}{}
	}
	close(done)
	return ti.Payload, ti.Priority, b, ctx.Err()
}

func (pc *PriorityChannel[T]) TryImmediatePop() (T, int, bool) {
	if pc.mu.TryLock() {
		defer pc.mu.Unlock()
		t, b := pc.queue.Pop()
		return t.Payload, t.Priority, b
	}
	var zeroVal T
	return zeroVal, -1, false
}
