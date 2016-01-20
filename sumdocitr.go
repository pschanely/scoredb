package main

import (
	"math"
	"sort"
)

type SumComponent struct {
	docItr     DocItr
	scoreRange float32
}

type SumComponents []SumComponent

func (a SumComponents) Len() int           { return len(a) }
func (a SumComponents) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SumComponents) Less(i, j int) bool { return a[i].scoreRange > a[j].scoreRange }

type SumDocItr struct {
	score    float32
	docId    int64
	min, max float32
	parts    []SumComponent
}

func NewSumDocItr(itrs []DocItr) *SumDocItr {
	min, max := float32(0.0), float32(0.0)
	components := make(SumComponents, len(itrs))
	for idx, part := range itrs {
		curMin, curMax := part.GetBounds()
		components[idx].docItr = part
		components[idx].scoreRange = float32(math.Abs(float64(curMax - curMin)))
		min += curMin
		max += curMax
	}
	sort.Sort(components)
	return &SumDocItr{
		score: 0.0,
		docId: -1,
		min:   min,
		max:   max,
		parts: components,
	}
}

func (op *SumDocItr) Name() string { return "SumDocItr" }
func (op *SumDocItr) DocId() int64 {
	return op.docId
}
func (op *SumDocItr) GetBounds() (min, max float32) { return op.min, op.max }
func (op *SumDocItr) Score() float32 {
	return op.score
}
func (op *SumDocItr) Close() {
	for _, part := range op.parts {
		part.docItr.Close()
	}
}
func (op *SumDocItr) Next(minId int64) bool {
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
				if curDocId >= minId {
					break
				}
				if !part.docItr.Next(minId) {
					return false
				}
			}
			if curDocId > minId {
				minId = curDocId
				keepGoing = true
				break
			}
			score += part.docItr.Score()
		}
		if !keepGoing {
			if score < min || score > max {
				minId += 1
				keepGoing = true
			}
		}
	}
	op.docId = minId
	op.score = score
	//fmt.Printf("SumDocItr Next(%v) [%v:%v] = %v score:%v\n", minId, op.min, op.max, op.docId, score)
	return true
}

func (op *SumDocItr) SetBounds(min, max float32) bool {
	//fmt.Printf("SumDocItr SetBounds %v %v\n", min, max)
	op.min = min
	op.max = max

	for curfield, component := range op.parts {
		newMin, newMax := min, max
		// subtract out the ranges of all the other components (the remaining range will be mine)
		for otherfactor, otherComponent := range op.parts {
			//Then subtract the other weights times their appropriate maxes or mins
			if curfield != otherfactor {
				otherMin, otherMax := otherComponent.docItr.GetBounds()
				newMin -= otherMax
				newMax -= otherMin
			}
		}
		curMin, curMax := component.docItr.GetBounds()
		if newMin < curMin {
			newMin = curMin
		}
		if newMax > curMax {
			newMax = curMax
		}
		if newMin != curMin || newMax != curMax {
			//fmt.Printf("SumDocItr SetBounds for component %v %v\n", newMin, newMax)
			component.docItr.SetBounds(newMin, newMax)
		}
	}
	return true
}
