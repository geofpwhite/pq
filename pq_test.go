package pq

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

func minIntCmp(a, b int) int { return a - b }
func maxIntCmp(a, b int) int { return b - a }

func TestNewPriorityQueueEmpty(t *testing.T) {
	q := NewPriorityQueue(minIntCmp)
	if _, ok := q.Peek(); ok {
		t.Fatalf("expected empty queue Peek to return false")
	}
	if _, ok := q.Pop(); ok {
		t.Fatalf("expected empty queue Pop to return false")
	}
}

func TestPushPopOrder(t *testing.T) {
	q := NewPriorityQueue(minIntCmp)
	input := []int{5, 3, 8, 1, 9, 2, 7, 4, 6, 0}
	for _, v := range input {
		q.Push(v)
	}

	want := append([]int(nil), input...)
	sort.Ints(want)

	for i, w := range want {
		got, ok := q.Pop()
		if !ok {
			t.Fatalf("pop %d: expected ok=true", i)
		}
		if got != w {
			t.Fatalf("pop %d: got %d, want %d", i, got, w)
		}
	}
	if _, ok := q.Pop(); ok {
		t.Fatalf("expected queue empty after popping all elements")
	}
}

func TestMaxCmp(t *testing.T) {
	maxCmp := func(a, b int) int { return b - a }
	q := NewPriorityQueue(maxCmp)
	input := []int{5, 3, 8, 1, 9}
	for _, v := range input {
		q.Push(v)
	}

	want := append([]int(nil), input...)
	sort.Sort(sort.Reverse(sort.IntSlice(want)))

	for i, w := range want {
		got, ok := q.Pop()
		if !ok || got != w {
			t.Fatalf("pop %d: got (%d,%v), want %d", i, got, ok, w)
		}
	}
}

func TestPeekDoesNotRemove(t *testing.T) {
	q := NewPriorityQueue(minIntCmp)
	q.Push(42)

	peeked, ok := q.Peek()
	if !ok || peeked != 42 {
		t.Fatalf("unexpected peek result %v %v", peeked, ok)
	}

	peekedAgain, ok := q.Peek()
	if !ok || peekedAgain != 42 {
		t.Fatalf("second peek should return same item, got %v %v", peekedAgain, ok)
	}

	popped, ok := q.Pop()
	if !ok || popped != 42 {
		t.Fatalf("expected pop to return the peeked item, got %v %v", popped, ok)
	}
}

func TestDuplicatePriorities(t *testing.T) {
	q := NewPriorityQueue(minIntCmp)
	for _, v := range []int{1, 1, 1, 2, 2, 0} {
		q.Push(v)
	}

	prev := -1
	count := 0
	for {
		v, ok := q.Pop()
		if !ok {
			break
		}
		if v < prev {
			t.Fatalf("heap property violated: %d popped after %d", v, prev)
		}
		prev = v
		count++
	}
	if count != 6 {
		t.Fatalf("expected 6 items popped, got %d", count)
	}
}

func TestWithMaxCmp(t *testing.T) {
	q := NewPriorityQueue(maxIntCmp)
	for _, v := range []int{1, 2, 1, 1, 2, 0, 0, 1, 0, 3, 0, 0} {
		q.Push(v)
	}

	prev := math.MaxInt
	count := 0
	for {
		v, ok := q.Pop()
		if !ok {
			break
		}
		if v > prev {
			t.Fatalf("heap property violated: %d popped after %d", v, prev)
		}
		prev = v
		count++
	}
	if count != 12 {
		t.Fatalf("expected 12 items popped, got %d", count)
	}
}

func TestStructComparator(t *testing.T) {
	type job struct {
		name string
		prio int
	}
	cmp := func(a, b job) int { return a.prio - b.prio }
	q := NewPriorityQueue(cmp)
	q.Push(job{"low", 5})
	q.Push(job{"high", 1})
	q.Push(job{"mid", 3})

	first, ok := q.Pop()
	if !ok || first.name != "high" {
		t.Fatalf("expected high priority job first, got %+v ok=%v", first, ok)
	}
	second, ok := q.Pop()
	if !ok || second.name != "mid" {
		t.Fatalf("expected mid priority job second, got %+v ok=%v", second, ok)
	}
	third, ok := q.Pop()
	if !ok || third.name != "low" {
		t.Fatalf("expected low priority job third, got %+v ok=%v", third, ok)
	}
}

func TestRandomOrderMatchesSort(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	q := NewPriorityQueue(minIntCmp)

	const n = 1000
	input := make([]int, n)
	for i := range input {
		input[i] = r.Intn(10000)
		q.Push(input[i])
	}

	sort.Ints(input)
	for i, want := range input {
		got, ok := q.Pop()
		if !ok || got != want {
			t.Fatalf("index %d: got (%d,%v), want %d", i, got, ok, want)
		}
	}
	if _, ok := q.Pop(); ok {
		t.Fatalf("expected queue empty after draining all random items")
	}
}
