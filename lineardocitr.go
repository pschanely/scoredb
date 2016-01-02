package main

import (
	//"fmt"
	"math"
	"sort"
)

type LinearComponent struct {
	coef       float32
	docItr     DocItr
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
}

func NewLinearDocItr(parts LinearComponents) *LinearDocItr {
	min, max := float32(0.0), float32(0.0)
	for idx, part := range parts {
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
func (op *LinearDocItr) Close() {
	for _, part := range op.parts {
		part.docItr.Close()
	}
}
func (op *LinearDocItr) Next(minId int64) bool {
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
			score += part.coef * part.docItr.Score()
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
	//fmt.Printf("LinearDocItr Next(%v) [%v:%v] = %v score:%v\n", minId, op.min, op.max, op.docId, score)
	return true
}

func (op *LinearDocItr) SetBounds(min, max float32) bool {
	//fmt.Printf("LinearDocItr SetBounds %v %v\n", min, max)
	op.min = min
	op.max = max

	for curfield, linComponent := range op.parts {
		//To start, divige the bottom value by the weight of the variable we're testing for
		curweight := linComponent.coef
		elimval := min
		for otherfactor, otherComponent := range op.parts {
			otherweight := otherComponent.coef
			//Then subtract the other weights times their appropriate maxes or mins
			if curfield != otherfactor {
				otherMin, otherMax := otherComponent.docItr.GetBounds()
				var minOrMax float32
				if otherweight < 0 {
					minOrMax = otherMin
				} else {
					minOrMax = otherMax
				}
				elimval -= otherweight * minOrMax
			}
		}
		elimval /= curweight
		//Assing the new value as a min if the wieght is positive, and as a max if the weight is negative.
		curMin, curMax := linComponent.docItr.GetBounds()
		if curweight > 0 {
			//fmt.Printf("LinearDocItr SetBounds min %v %v,%v\n", elimval, curMin, curMax)
			if elimval < curMin {
				elimval = curMin
			}
			linComponent.docItr.SetBounds(elimval, curMax)
		} else {
			//fmt.Printf("LinearDocItr SetBounds max %v %v,%v\n", elimval, curMin, curMax)
			if elimval > curMax {
				elimval = curMax
			}
			linComponent.docItr.SetBounds(curMin, elimval)
		}
	}
	return true
}
