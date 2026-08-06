package pq

// Comparator orders two values of type T. It returns a negative number if a
// sorts before b (a has higher priority), zero if they are equivalent, and a
// positive number if a sorts after b.
type Comparator[T any] = func(a, b T) int

// PriorityQueue is a binary-heap priority queue ordered by cmp. It is not
// safe for concurrent use; callers needing concurrency should use
// PriorityChannel instead.
type PriorityQueue[C Comparator[T], T any] struct {
	ary []T
	cmp C
}

// NewPriorityQueue creates an empty PriorityQueue that orders items using cmp.
func NewPriorityQueue[C Comparator[T], T any](cmp C) *PriorityQueue[C, T] {
	return &PriorityQueue[C, T]{
		ary: make([]T, 0),
		cmp: cmp,
	}
}

// Push inserts item into the queue.
func (pq *PriorityQueue[C, T]) Push(item T) {
	pq.ary = append(pq.ary, item)
	pq.up(len(pq.ary) - 1)
}

// Pop removes and returns the highest-priority item in the queue. The
// returned bool is false, with a zero value, if the queue is empty.
func (pq *PriorityQueue[C, T]) Pop() (T, bool) {
	if len(pq.ary) == 0 {
		var zero T
		return zero, false
	}
	out := pq.ary[0]
	pq.ary[0] = pq.ary[len(pq.ary)-1]
	pq.ary = pq.ary[:len(pq.ary)-1]
	pq.down()
	return out, true
}

// Peek returns the highest-priority item in the queue without removing it.
// The returned bool is false, with a zero value, if the queue is empty.
func (pq *PriorityQueue[C, T]) Peek() (T, bool) {
	if len(pq.ary) == 0 {
		var zero T
		return zero, false
	}
	return pq.ary[0], true
}

// Len returns the number of items currently in the queue.
func (pq *PriorityQueue[C, T]) Len() int {
	return len(pq.ary)
}

// up restores the heap property by sifting the item at index toward the
// root, swapping with its parent while it outranks it.
func (pq *PriorityQueue[C, T]) up(index int) {
	for pq.cmp(pq.ary[index], pq.ary[(index-1)/2]) < 0 {
		pq.ary[index], pq.ary[(index-1)/2] = pq.ary[(index-1)/2], pq.ary[index]
		index = (index - 1) / 2
	}
}

// down restores the heap property by sifting the root item toward the
// leaves, swapping with its highest-priority child until it no longer
// outranks either child.
func (pq *PriorityQueue[C, T]) down() {
	index := 0
	for ((2*index+1 < len(pq.ary)) && pq.cmp(pq.ary[index], pq.ary[2*index+1]) > 0) ||
		(2*index+2 < len(pq.ary) && pq.cmp(pq.ary[index], pq.ary[2*index+2]) > 0) {
		if 2*index+2 < len(pq.ary) && pq.cmp(pq.ary[2*index+1], pq.ary[2*index+2]) > 0 {
			pq.ary[index], pq.ary[2*index+2] = pq.ary[2*index+2], pq.ary[index]
			index = 2*index + 2
		} else {
			pq.ary[index], pq.ary[2*index+1] = pq.ary[2*index+1], pq.ary[index]
			index = 2*index + 1
		}
	}
}
