package pq

import (
	"context"
	"sync"
)

type PriorityChannel[T any] struct {
	mu       sync.Mutex
	queue    *PriorityQueue[Comparator[ChannelMessage[T]], ChannelMessage[T]]
	nonempty chan struct{}
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
		queue:    NewPriorityQueue(cmpChannelMessage[T]),
		nonempty: make(chan struct{}, 1),
	}
	return pc
}

func (pc *PriorityChannel[T]) Push(item T, priority int) {
	pc.mu.Lock()
	pc.queue.Push(ChannelMessage[T]{item, priority})
	select {
	case pc.nonempty <- struct{}{}:
	default:
	}
	pc.mu.Unlock()
}

func (pc *PriorityChannel[T]) Pop() (T, int, bool) {
	pc.mu.Lock()
	select {
	case <-pc.nonempty:
	default:
	}
	t, b := pc.queue.Pop()
	if pc.queue.Len() != 0 {
		select {
		case pc.nonempty <- struct{}{}:
		default:
		}
	}
	pc.mu.Unlock()
	return t.Payload, t.Priority, b
}

// PopBlocking blocks until queue isn't empty
func (pc *PriorityChannel[T]) PopBlocking() (T, int) {
	<-pc.nonempty
	pc.mu.Lock()
	t, _ := pc.queue.Pop()
	if pc.queue.Len() != 0 {
		pc.nonempty <- struct{}{}
	}
	pc.mu.Unlock()
	return t.Payload, t.Priority
}

func (pc *PriorityChannel[T]) PopBlockingWithCancel(ctx context.Context) (T, int, error) {
	var ti ChannelMessage[T]
	unlocked := make(chan struct{}, 1)
	done := make(chan struct{}, 1)
	go func() {
		<-pc.nonempty
		pc.mu.Lock()
		unlocked <- struct{}{}
		close(unlocked)
		<-done
		pc.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
		select {
		case pc.nonempty <- struct{}{}:
		default:
		}
		done <- struct{}{}
	case <-unlocked:
		ti, _ = pc.queue.Pop()
		if pc.queue.Len() > 0 {
			select {
			case pc.nonempty <- struct{}{}:
			default:
			}
		}
		done <- struct{}{}
	}
	close(done)
	return ti.Payload, ti.Priority, ctx.Err()
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
		select {
		case <-pc.nonempty:
		default:
		}
		t, b := pc.queue.Pop()
		if pc.queue.Len() > 0 {
			select {
			case pc.nonempty <- struct{}{}:
			default:
			}
		}
		pc.mu.Unlock()
		return t.Payload, t.Priority, b
	}
	var zeroVal T
	return zeroVal, -1, false
}
