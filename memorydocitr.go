package main

import ()

type MemoryDocItr struct {
	score    float32
	docId    int64
	min, max float32

	scores []float32
	docs   []int64
	index  int
}

func NewMemoryDocItr(scores []float32, docs []int64) *MemoryDocItr {
	return &MemoryDocItr{
		score: 0.0,
		docId: -1,
		min:   0.0,
		max:   1.0,

		scores: scores,
		docs:   docs,
		index:  -1,
	}
}
func (op *MemoryDocItr) DocId() int64 {
	return op.docId
}
func (op *MemoryDocItr) Score() float32 {
	return op.score
}
func (op *MemoryDocItr) SetBounds(min, max float32) bool {
	if min > op.max || max < op.min {
		return false
	}
	if min > op.min {
		op.min = min
	}
	if max < op.max {
		op.max = max
	}
	return true
}
func (op *MemoryDocItr) Name() string { return "MemoryDocItr" }
func (op *MemoryDocItr) Next() bool {
	op.index += 1
	index := op.index
	if index < len(op.scores) {
		score := op.scores[index]
		if score < op.min || score > op.max {
			return op.Next()
		}
		op.score = score
		op.docId = op.docs[index]
		return true
	} else {
		return false
	}
}
