package main

import ()

type Part struct {
	coef   float32
	docItr DocItr
}

type LinearDocItr struct {
	score    float32
	docId    int64
	min, max float32
	parts    []Part
}

func NewLinearDocItr(parts []Part) *LinearDocItr {
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
func (op *LinearDocItr) Score() float32 {
	return op.score
}
func (op *LinearDocItr) SetBounds(min, max float32) bool {
	op.min = min
	op.max = max
	//for idx, part := range op.parts {
	//part.SetBounds(min, max)
	//}
	return true
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
				if !part.docItr.Next() {
					return false
				}
				curDocId = part.docItr.DocId()
				if curDocId >= docId {
					break
				}
			}
			if curDocId > docId {
				docId = curDocId
				keepGoing = true
				break
			}
			score += part.coef * part.docItr.Score()
		}
	}
	op.docId = docId
	op.score = score
	return true
}
