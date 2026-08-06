package pq

import (
	"context"
	"errors"
	"sync"
)

type PriorityChannel[T any] struct {
	closing bool
	mu      sync.Mutex
	cond    sync.Cond
	queue   *PriorityQueue[Comparator[ChannelMessage[T]], ChannelMessage[T]]
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
	pc.cond = *sync.NewCond(&pc.mu)
	return pc
}

func (pc *PriorityChannel[T]) Push(item T, priority int) error {
	pc.mu.Lock()
	if pc.closing {
		pc.mu.Unlock()
		return errors.New("priority channel is closed")
	}
	pc.queue.Push(ChannelMessage[T]{item, priority})
	pc.mu.Unlock()
	pc.cond.Signal()
	return nil
}

// PopBlocking blocks until queue isn't empty or context is canceled.
func (pc *PriorityChannel[T]) PopBlocking(ctx context.Context) (T, int, error) {
	stop := context.AfterFunc(ctx, pc.cond.Broadcast)
	defer stop()

	pc.mu.Lock()
	defer pc.mu.Unlock()
	for pc.queue != nil && pc.queue.Len() == 0 {
		if err := ctx.Err(); err != nil {
			var zero T
			return zero, 0, err
		}
		pc.cond.Wait()
	}
	if pc.queue == nil {
		var zero T
		return zero, 0, errors.New("priority channel is closed")
	}
	t, _ := pc.queue.Pop()
	if pc.closing {
		pc.cond.Broadcast()
	}
	return t.Payload, t.Priority, nil
}

func (pc *PriorityChannel[T]) Pop(ctx context.Context) (T, int, bool, error) {
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
		if pc.queue == nil {
			var zeroVal T
			return zeroVal, -1, false, errors.New("priority channel is closed")
		}
		ti, b = pc.queue.Pop()
		if pc.closing {
			pc.cond.Broadcast()
		}
		done <- struct{}{}
	}
	close(done)
	return ti.Payload, ti.Priority, b, ctx.Err()
}

func (pc *PriorityChannel[T]) TryImmediatePop() (T, int, bool) {
	if pc.mu.TryLock() {
		defer pc.mu.Unlock()
		if pc.queue == nil {
			var zeroVal T
			return zeroVal, -1, false
		}
		t, b := pc.queue.Pop()
		if pc.closing {
			pc.cond.Broadcast()
		}
		return t.Payload, t.Priority, b
	}
	var zeroVal T
	return zeroVal, -1, false
}

func (pc *PriorityChannel[T]) Len() int {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.queue.Len()
}

func (pc *PriorityChannel[T]) Close() error {
	pc.mu.Lock()
	pc.closing = true
	for pc.queue.Len() > 0 {
		pc.cond.Wait()
	}
	pc.queue = nil
	pc.mu.Unlock()
	pc.cond.Broadcast()
	return nil
}
