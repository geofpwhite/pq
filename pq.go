package pq

type Comparator[T any] = func(a, b T) int

type PriorityQueue[C Comparator[T], T any] struct {
	ary []T
	cmp C
}

func NewPriorityQueue[C Comparator[T], T any](cmp C) *PriorityQueue[C, T] {
	return &PriorityQueue[C, T]{
		ary: make([]T, 0),
		cmp: cmp,
	}
}

func (pq *PriorityQueue[C, T]) Push(item T) {
	pq.ary = append(pq.ary, item)
	pq.up(len(pq.ary) - 1)
}

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

func (pq *PriorityQueue[C, T]) Peek() (T, bool) {
	if len(pq.ary) == 0 {
		var zero T
		return zero, false
	}
	return pq.ary[0], true
}

func (pq *PriorityQueue[C, T]) up(index int) {
	for index > 0 && pq.cmp(pq.ary[index], pq.ary[(index-1)/2]) < 0 {
		pq.ary[index], pq.ary[(index-1)/2] = pq.ary[(index-1)/2], pq.ary[index]
		index = (index - 1) / 2
	}
}

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
