package main

import (
	"math"
	"sort"
)

type LinearComponent struct {
	coef   float32
	docItr DocItr
	scoreRange float32
}

type LinearComponents []LinearComponent

func (a LinearComponents) Len() int           { return len(a) }
func (a LinearComponents) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a LinearComponents) Less(i, j int) bool { return a[i].scoreRange > a[j].scoreRange }


type LinearDocItr struct {
	score    float32
	docId    int64
	min, max float32
	parts    []LinearComponent
	bounds   map[string][2]float32
}

func NewLinearDocItr(parts LinearComponents) *LinearDocItr {
	min, max := float32(0.0), float32(0.0)
	for idx, part := range(parts) {
		curMin, curMax := part.docItr.GetBounds()
		curMin *= part.coef
		curMax *= part.coef
		parts[idx].scoreRange = float32(math.Abs(float64(curMax - curMin)))
		min += curMin
		max += curMax
	}
	sort.Sort(parts)
	return &LinearDocItr{
		score: 0.0,
		docId: -1,
		min:   min,
		max:   max,
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
	min, max := op.min, op.max
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
				docId = curDocId
				keepGoing = true
				break
			}
			score += part.coef * part.docItr.Score()
		}
		if ! keepGoing {
			if score < min || score > max {
				docId += 1
				keepGoing = true
			}
		}
	}
	op.docId = docId
	op.score = score
	return true
}
