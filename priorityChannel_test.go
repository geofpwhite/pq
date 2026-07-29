package pq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewPriorityChannelEmpty(t *testing.T) {
	pc := NewPriorityChannel[string]()
	if _, _, ok := pc.Pop(); ok {
		t.Fatalf("expected empty channel Pop to return false")
	}
}

func TestPriorityChannelOrder(t *testing.T) {
	pc := NewPriorityChannel[string]()
	pc.Push("low", 1)
	pc.Push("high", 10)
	pc.Push("mid", 5)

	wantOrder := []struct {
		payload string
		prio    int
	}{
		{"high", 10},
		{"mid", 5},
		{"low", 1},
	}
	for i, want := range wantOrder {
		payload, prio, ok := pc.Pop()
		if !ok {
			t.Fatalf("pop %d: expected ok=true", i)
		}
		if payload != want.payload || prio != want.prio {
			t.Fatalf("pop %d: got (%s,%d), want (%s,%d)", i, payload, prio, want.payload, want.prio)
		}
	}
	if _, _, ok := pc.Pop(); ok {
		t.Fatalf("expected channel empty after draining")
	}
}

func TestPriorityChannelZeroValueOnEmptyPop(t *testing.T) {
	pc := NewPriorityChannel[int]()
	payload, _, ok := pc.Pop()
	if ok {
		t.Fatalf("expected ok=false")
	}
	if payload != 0 {
		t.Fatalf("expected zero value payload, got %d", payload)
	}
}

func TestTryImmediatePopSucceedsWhenUnlocked(t *testing.T) {
	pc := NewPriorityChannel[string]()
	pc.Push("item", 1)

	payload, prio, ok := pc.TryImmediatePop()
	if !ok || payload != "item" || prio != 1 {
		t.Fatalf("unexpected result: %q %d %v", payload, prio, ok)
	}
}

func TestTryImmediatePopFailsWhenLocked(t *testing.T) {
	pc := NewPriorityChannel[string]()
	pc.Push("item", 1)

	pc.mu.Lock()
	defer pc.mu.Unlock()

	payload, prio, ok := pc.TryImmediatePop()
	if ok {
		t.Fatalf("expected TryImmediatePop to fail while locked")
	}
	if payload != "" || prio != -1 {
		t.Fatalf("expected zero value and -1 priority, got %q %d", payload, prio)
	}
}

func TestPopBlockingReturnsImmediatelyWhenItemAvailable(t *testing.T) {
	pc := NewPriorityChannel[string]()
	pc.Push("item", 5)

	done := make(chan struct{})
	var payload string
	var prio int
	go func() {
		payload, prio = pc.PopBlocking()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("PopBlocking did not return for an already-available item")
	}
	if payload != "item" || prio != 5 {
		t.Fatalf("unexpected result: %q %d", payload, prio)
	}
}

func TestPopBlockingBlocksUntilPushed(t *testing.T) {
	pc := NewPriorityChannel[string]()

	done := make(chan struct{})
	var payload string
	var prio int
	go func() {
		payload, prio = pc.PopBlocking()
		close(done)
	}()

	select {
	case <-done:
		t.Fatalf("PopBlocking returned before any item was pushed")
	case <-time.After(50 * time.Millisecond):
	}

	pc.Push("item", 9)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("PopBlocking did not return after an item was pushed")
	}
	if payload != "item" || prio != 9 {
		t.Fatalf("unexpected result: %q %d", payload, prio)
	}
}

func TestPopBlockingOrdersByPriority(t *testing.T) {
	pc := NewPriorityChannel[string]()
	pc.Push("low", 1)
	pc.Push("high", 10)
	pc.Push("mid", 5)

	wantOrder := []struct {
		payload string
		prio    int
	}{
		{"high", 10},
		{"mid", 5},
		{"low", 1},
	}
	for i, want := range wantOrder {
		payload, prio := pc.PopBlocking()
		if payload != want.payload || prio != want.prio {
			t.Fatalf("pop %d: got (%s,%d), want (%s,%d)", i, payload, prio, want.payload, want.prio)
		}
	}
}

func TestPopBlockingConcurrentProducersConsumers(t *testing.T) {
	pc := NewPriorityChannel[int]()

	const producers = 13
	const perProducer = 50
	const total = producers * perProducer

	var wg sync.WaitGroup
	for i := range producers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				pc.Push(j, i)
			}
		}(i)
	}

	results := make(chan int, total)
	for i := 0; i < total; i++ {
		go func() {
			_, prio := pc.PopBlocking()
			results <- prio
		}()
	}

	wg.Wait()

	count := 0
	timeout := time.After(5 * time.Second)
	for count < total {
		select {
		case <-results:
			count++
		case <-timeout:
			t.Fatalf("timed out waiting for consumers: got %d/%d", count, total)
		}
	}
}

func TestPopBlockingWithCancelReturnsItemWhenAvailable(t *testing.T) {
	pc := NewPriorityChannel[string]()
	pc.Push("item", 4)

	payload, prio, err := pc.PopBlockingWithCancel(context.Background())
	if payload != "item" || prio != 4 {
		t.Fatalf("unexpected result: %q %d", payload, prio)
	}
	if err != nil {
		t.Fatalf("expected nil error on successful pop, got %v", err)
	}
}

func TestPopBlockingWithCancelReturnsErrorWhenQueueStaysEmpty(t *testing.T) {
	pc := NewPriorityChannel[string]()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	payload, prio, err := pc.PopBlockingWithCancel(ctx)
	elapsed := time.Since(start)

	if payload != "" || prio != 0 {
		t.Fatalf("expected zero value, got %q %d", payload, prio)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("PopBlockingWithCancel took too long to respect cancellation: %v", elapsed)
	}
}

func TestPopBlockingWithCancelWaitsForPushThenSucceeds(t *testing.T) {
	pc := NewPriorityChannel[string]()

	done := make(chan struct{})
	var payload string
	var prio int
	var err error
	go func() {
		payload, prio, err = pc.PopBlockingWithCancel(context.Background())
		close(done)
	}()

	select {
	case <-done:
		t.Fatalf("PopBlockingWithCancel returned before any item was pushed")
	case <-time.After(50 * time.Millisecond):
	}

	pc.Push("item", 6)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("PopBlockingWithCancel did not return after an item was pushed")
	}

	if payload != "item" || prio != 6 {
		t.Fatalf("unexpected result: %q %d", payload, prio)
	}
	if err != nil {
		t.Fatalf("expected nil error on successful pop, got %v", err)
	}
}

// Regression test: an item pushed shortly after a cancellation must still be
// observable to a later PopBlocking call. The internal goroutine left behind
// by a cancelled PopBlockingWithCancel must not silently consume the
// "nonempty" signal for an item it never actually pops.
func TestPopBlockingWithCancelDoesNotLoseItemPushedAfterCancellation(t *testing.T) {
	pc := NewPriorityChannel[string]()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, _, err := pc.PopBlockingWithCancel(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	// give the internal goroutine time to unwind after the cancellation
	time.Sleep(50 * time.Millisecond)

	pc.Push("item", 2)

	done := make(chan struct{})
	var payload string
	var prio int
	go func() {
		payload, prio = pc.PopBlocking()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("item pushed after cancellation appears to be stuck (lost wakeup)")
	}
	if payload != "item" || prio != 2 {
		t.Fatalf("unexpected result: %q %d", payload, prio)
	}
}

func TestPopWithCancelReturnsItemWhenAvailable(t *testing.T) {
	pc := NewPriorityChannel[string]()
	pc.Push("item", 7)

	payload, prio, ok, err := pc.PopWithCancel(context.Background())
	if !ok || payload != "item" || prio != 7 {
		t.Fatalf("unexpected result: %q %d %v", payload, prio, ok)
	}
	if err != nil {
		t.Fatalf("expected nil error on successful pop, got %v", err)
	}
}

func TestPopWithCancelEmptyQueueReturnsFalseWithNilError(t *testing.T) {
	pc := NewPriorityChannel[string]()

	done := make(chan struct{})
	var payload string
	var prio int
	var ok bool
	var err error
	go func() {
		payload, prio, ok, err = pc.PopWithCancel(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("PopWithCancel blocked on an empty, uncontended channel")
	}
	if ok || payload != "" || prio != 0 {
		t.Fatalf("expected zero value and ok=false, got %q %d %v", payload, prio, ok)
	}
	if err != nil {
		t.Fatalf("expected nil error for an empty queue (not a cancellation), got %v", err)
	}
}

func TestPopWithCancelReturnsCancellationErrorDuringLockContention(t *testing.T) {
	pc := NewPriorityChannel[string]()
	pc.Push("item", 1)

	pc.mu.Lock() // simulate the mutex being held elsewhere

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	done := make(chan struct{})
	var payload string
	var prio int
	var ok bool
	var err error
	go func() {
		payload, prio, ok, err = pc.PopWithCancel(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		pc.mu.Unlock()
		t.Fatalf("PopWithCancel did not return after context cancellation")
	}
	elapsed := time.Since(start)

	if ok || payload != "" || prio != 0 {
		t.Fatalf("expected zero value and ok=false after cancellation, got %q %d %v", payload, prio, ok)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("PopWithCancel took too long to respect cancellation: %v", elapsed)
	}

	pc.mu.Unlock() // release the simulated contention

	value, priority, ok := pc.Pop()
	if !ok || value != "item" || priority != 1 {
		t.Fatalf("expected channel to still work after cancellation, got %q %d %v", value, priority, ok)
	}
}

func TestPopWithCancelWaitsForLockThenSucceeds(t *testing.T) {
	pc := NewPriorityChannel[string]()
	pc.Push("item", 3)

	pc.mu.Lock()

	done := make(chan struct{})
	var payload string
	var prio int
	var ok bool
	var err error
	go func() {
		payload, prio, ok, err = pc.PopWithCancel(context.Background())
		close(done)
	}()

	time.Sleep(20 * time.Millisecond) // give PopWithCancel a chance to start waiting on the lock
	pc.mu.Unlock()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("PopWithCancel did not return once the lock became available")
	}

	if !ok || payload != "item" || prio != 3 {
		t.Fatalf("unexpected result: %q %d %v", payload, prio, ok)
	}
	if err != nil {
		t.Fatalf("expected nil error on successful pop, got %v", err)
	}
}

func TestPopWithCancelDistinguishesCancelFromEmptyQueue(t *testing.T) {
	// Canceled context, contended lock: error should be non-nil.
	pcCancelled := NewPriorityChannel[string]()
	pcCancelled.mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	var cancelledErr error
	var cancelledOK bool
	go func() {
		_, _, cancelledOK, cancelledErr = pcCancelled.PopWithCancel(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		pcCancelled.mu.Unlock()
		t.Fatalf("PopWithCancel did not return for an already-canceled context")
	}
	pcCancelled.mu.Unlock()

	if cancelledOK {
		t.Fatalf("expected ok=false for a canceled pop")
	}
	if !errors.Is(cancelledErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", cancelledErr)
	}

	// Uncancelled context, empty queue: ok=false but error should be nil.
	pcEmpty := NewPriorityChannel[string]()
	emptyPayload, _, emptyOK, emptyErr := pcEmpty.PopWithCancel(context.Background())
	if emptyOK {
		t.Fatalf("expected ok=false for an empty queue, got payload %q", emptyPayload)
	}
	if emptyErr != nil {
		t.Fatalf("expected nil error for an empty queue, got %v", emptyErr)
	}

	// Both report ok=false, but only the cancellation carries an error.
	if cancelledErr == nil {
		t.Fatalf("expected cancellation and empty-queue misses to be distinguishable by error")
	}
}

func TestPriorityChannelConcurrentPushPop(t *testing.T) {
	pc := NewPriorityChannel[int]()
	var wg sync.WaitGroup

	const producers = 8
	const perProducer = 50

	for i := range producers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := range perProducer {
				pc.Push(j, i)
			}
		}(i)
	}
	wg.Wait()

	count := 0
	for _, _, ok := pc.Pop(); ok; _, _, ok = pc.Pop() {
		count++
	}
	if count != producers*perProducer {
		t.Fatalf("expected %d items, got %d", producers*perProducer, count)
	}
}
