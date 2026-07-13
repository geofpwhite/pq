package pq

import (
	"sync"
	"testing"
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

func TestPriorityChannelConcurrentPushPop(t *testing.T) {
	pc := NewPriorityChannel[int]()
	var wg sync.WaitGroup

	const producers = 8
	const perProducer = 50

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
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
