package main

import (
	"fmt"
)

type LinearComponent struct {
	coef   float32
	docItr DocItr
}

type LinearDocItr struct {
	score    float32
	docId    int64
	min, max float32
	parts    []LinearComponent
	bounds   map[string][2]float32
}

func NewLinearDocItr(parts []LinearComponent) *LinearDocItr {
	return &LinearDocItr{
		score: 0.0,
		docId: -1,
		min:   0.0,
		max:   1.0,
		parts: parts,
	}
}

func (op *LinearDocItr) Name() string { return "LinearDocItr" }
func (op *LinearDocItr) DocId() int64 {
	return op.docId
}
func (op *LinearDocItr) GetBounds() (min, max float32) { return op.min, op.max }
func (op *LinearDocItr) Score() float32 {
	return op.score
}
func (op *LinearDocItr) Next() bool {
	docId := op.docId + 1
	keepGoing := true
	var score float32
	for keepGoing {
		keepGoing = false
		score = float32(0.0)
		for _, part := range op.parts {
			var curDocId int64
			for {
				curDocId = part.docItr.DocId()
				if curDocId >= docId {
					break
				}
				if !part.docItr.Next() {
					return false
				}
			}
			if curDocId > docId {
				//fmt.Printf("Advancing doc id for cross field difference %v->%v\n", docId, curDocId)
				docId = curDocId
				keepGoing = true
				break
			}
			score += part.coef * part.docItr.Score()
			//fmt.Printf("new score at doc %v: %v\n", docId, score)
		}
	}
	fmt.Printf("LinearDocItr Next() %v (score: %v)\n", docId, score)
	op.docId = docId
	op.score = score
	return true
}
