package pq

import (
	"context"
	"errors"
	"sync"
)

// PriorityChannel is a concurrency-safe, channel-like queue that always
// pops the pending item with the highest priority. Zero value is not usable;
// construct one with NewPriorityChannel.
type PriorityChannel[T any] struct {
	closing bool
	mu      sync.Mutex
	cond    sync.Cond
	queue   *PriorityQueue[Comparator[ChannelMessage[T]], ChannelMessage[T]]
}

// ChannelMessage pairs a pushed payload with the priority it was pushed
// under.
type ChannelMessage[T any] struct {
	Payload  T
	Priority int
}

// cmpChannelMessage orders ChannelMessages so that higher Priority values
// sort first.
func cmpChannelMessage[T any](a, b ChannelMessage[T]) int {
	return b.Priority - a.Priority
}

// NewPriorityChannel creates an empty, open PriorityChannel.
func NewPriorityChannel[T any]() *PriorityChannel[T] {
	pc := &PriorityChannel[T]{
		queue: NewPriorityQueue(cmpChannelMessage[T]),
	}
	pc.cond = *sync.NewCond(&pc.mu)
	return pc
}

// Push adds item to the channel under the given priority, waking one
// blocked PopBlocking caller if any is waiting. It returns an error without
// enqueueing item if the channel has been closed.
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

// PopBlocking blocks until the highest-priority item is available to pop,
// ctx is canceled, or the channel is closed. It returns ctx.Err() on
// cancellation, or an error if the channel is closed before an item is
// available.
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

// Pop attempts to pop the highest-priority item without waiting for the
// queue to become non-empty; it only waits for the internal lock, racing
// that wait against ctx cancellation. If the queue is currently empty it
// returns ok=false with a nil error. It returns a non-nil error if ctx is
// canceled before the lock is acquired, or if the channel has been closed.
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

// TryImmediatePop pops the highest-priority item without blocking at all.
// If the internal lock is currently held elsewhere it immediately returns
// ok=false rather than waiting for it.
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

// Len returns the number of items currently pending in the channel.
func (pc *PriorityChannel[T]) Len() int {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.queue.Len()
}

// Close stops the channel from accepting further Pushes, blocks until every
// item already queued has been drained by consumers via Pop or
// PopBlocking, and then releases the underlying queue. Once Close returns,
// Push, Pop, and PopBlocking all report the channel as closed.
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
