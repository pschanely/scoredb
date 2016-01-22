package scoredb

import (
	"math"
)

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
		score:  0.0,
		docId:  -1,
		min:    float32(math.Inf(-1)),
		max:    float32(math.Inf(1)),
		scores: scores,
		docs:   docs,
		index:  -1,
	}
}
func (op *MemoryDocItr) Cur() (int64, float32) {
	return op.docId, op.score
}
func (op *MemoryDocItr) GetBounds() (min, max float32) { return op.min, op.max }
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
func (op *MemoryDocItr) Close()       {}
func (op *MemoryDocItr) Next(minId int64) bool {
	for {
		op.index += 1
		index := op.index
		if index >= len(op.docs) {
			return false
		}
		docId := op.docs[index]
		if docId >= minId {
			score := op.scores[index]
			if score >= op.min && score <= op.max {
				op.score = score
				op.docId = op.docs[index]
				return true
			}
		}
	}
}
