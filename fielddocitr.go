package main

import (
	"container/heap"
	"fmt"
)

type FieldDocItr struct {
	score    float32
	docId    int64
	min, max float32

	lists FieldDocItrs
}

func NewFieldDocItr(lists FieldDocItrs) *FieldDocItr {
	return &FieldDocItr{
		score: 0.0,
		docId: -1,
		min:   0.0,
		max:   1.0,

		lists: lists,
	}
}

type FieldDocItrs []DocItr       // FieldDocItrs implements heap.Interface
func (so FieldDocItrs) Len() int { return len(so) }
func (so FieldDocItrs) Less(i, j int) bool {
	return (so[i]).DocId() < (so[j]).DocId()
}
func (so *FieldDocItrs) Pop() interface{} {
	old := *so
	n := len(old)
	item := old[n-1]
	*so = old[0 : n-1]
	return item
}
func (so *FieldDocItrs) Push(x interface{}) {
	*so = append(*so, x.(DocItr))
}
func (so FieldDocItrs) Swap(i, j int) {
	so[i], so[j] = so[j], so[i]
}

func (op *FieldDocItr) DocId() int64 {
	return op.docId
}
func (op *FieldDocItr) Score() float32 {
	return op.score
}
func (op *FieldDocItr) SetBounds(min, max float32) bool {
	op.min = min
	op.max = max
	anyMore := false
	for _, subOp := range op.lists {
		if subOp.SetBounds(min, max) {
			anyMore = true
		}
	}
	return anyMore
}
func (op *FieldDocItr) Next() bool {
	if len(op.lists) == 0 {
		return false
	}
	fmt.Printf("%+v\n", op)
	fmt.Printf("%+v\n", op.lists[0])
	minId := op.lists[0].DocId() + 1
	for op.lists[0].DocId() < minId {
		if op.lists[0].Next() {
			heap.Fix(&op.lists, 0)
		} else {
			heap.Remove(&op.lists, 0)
			if len(op.lists) == 0 {
				return false
			}
		}
	}
	op.docId = op.lists[0].DocId()
	op.score = op.lists[0].Score()
	return true
}

// Shifts operations forward until they all produce the same docId
func SyncOperations(operations []DocItr, toDocId int64) (docId int64, score bool) {
	syncAgain := true
	for syncAgain {
		syncAgain = false
		for _, subOp := range operations {
			docId := subOp.DocId()
			for docId < toDocId {
				if !subOp.Next() {
					return toDocId, false
				}
				docId = subOp.DocId()
			}
			if docId > toDocId {
				toDocId = docId
				syncAgain = true
				break
			}
		}
	}
	return toDocId, true
}
