package main

import (
	"container/heap"
	"fmt"
	"math"
	"time"
)

type FieldDocItr struct {
	field    string
	score    float32
	docId    int64
	min, max float32
	lists    FieldDocItrs
}

func NewFieldDocItr(field string, lists FieldDocItrs) *FieldDocItr {
	itr := &FieldDocItr{
		field: field,
		score: 0.0,
		docId: -1,
		lists: lists,
	}
	min, max := float32(math.Inf(1)), float32(math.Inf(-1))
	for _, docItr := range lists {
		curMin, curMax := docItr.GetBounds()
		if curMin < min {
			min = curMin
		}
		if curMax > max {
			max = curMax
		}
	}
	itr.min, itr.max = min, max
	return itr
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

func (op *FieldDocItr) Name() string { return "FieldDocItr" }
func (op *FieldDocItr) DocId() int64 {
	return op.docId
}
func (op *FieldDocItr) Score() float32 {
	return op.score
}
func (op *FieldDocItr) GetBounds() (min, max float32) {
	return op.min, op.max
}
func (op *FieldDocItr) SetBounds(min, max float32) bool {
	op.min = min
	op.max = max
	for {
		keepGoing := false
		anyMore := false
		for idx, subOp := range op.lists {
			if subOp.SetBounds(min, max) {
				anyMore = true
			} else {
				subOp.Close()
				lists := op.lists
				lists[idx] = lists[len(lists)-1]
				op.lists = lists[:len(lists)-1]
				keepGoing = true
				fmt.Printf("%08d Field elim @doc %08d, %05d remain (%s)\n", time.Now().UnixNano() % 100000000, op.docId, len(op.lists), op.field)
				break
			}
		}
		if !keepGoing {
			return anyMore
		}
		heap.Init(&op.lists)
	}
}

func (op *FieldDocItr) Close() {
	for _, list := range op.lists {
		list.Close()
	}
}

func (op *FieldDocItr) Next(minId int64) bool {
	if len(op.lists) == 0 {
		return false
	}
	for op.lists[0].DocId() < minId {
		if !op.lists[0].Next(minId) {
			heap.Remove(&op.lists, 0)
			if len(op.lists) == 0 {
				fmt.Printf("FieldDocItr Next(%v) %v END\n", minId, op.field)
				return false
			}
		} else {
			heap.Fix(&op.lists, 0)
		}
	}
	op.docId = op.lists[0].DocId()
	op.score = op.lists[0].Score()
	//fmt.Printf("FieldDocItr Next(%v) %v %v %v\n", minId, op.field, op.docId, op.score)
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
				if !subOp.Next(docId) {
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
