package scoredb

import (
	"fmt"
	"sort"
)

type MinComponents []DocItr

func (a MinComponents) Len() int           { return len(a) }
func (a MinComponents) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a MinComponents) Less(i, j int) bool {
	min1, max1 := a[i].GetBounds()
	min2, max2 := a[j].GetBounds()
	return max1 - min1 > max2 - min2
}

type MinDocItr struct {
	score    float32
	docId    int64
	min, max float32
	parts    MinComponents
}

func NewMinDocItr(itrs []DocItr) *MinDocItr {
	min, max := float32(0.0), float32(0.0)
	components := make(MinComponents, len(itrs))
	for idx, part := range itrs {
		curMin, curMax := part.GetBounds()
		components[idx] = part
		if idx == 0 {
			min, max = curMin, curMax
		} else {
			min = Min(min, curMin)
			max = Min(max, curMax)
		}
	}
	sort.Sort(components)
	return &MinDocItr{
		score: 0.0,
		docId: -1,
		min:   min,
		max:   max,
		parts: components,
	}
}

func (op *MinDocItr) Name() string { return "MinDocItr" }
func (op *MinDocItr) Cur() (int64, float32) {
	return op.docId, op.score
}
func (op *MinDocItr) GetBounds() (min, max float32) { return op.min, op.max }
func (op *MinDocItr) Close() {
	for _, part := range op.parts {
		part.Close()
	}
}

func (op *MinDocItr) Next(minId int64) bool {
	min, max := op.min, op.max
	keepGoing := true
	var score float32
	for keepGoing {
		//fmt.Printf("MinDocItr Next itr (%v) [%v:%v] = %v score:%v\n", minId, op.min, op.max, op.docId, score)
		keepGoing = false
		score = PositiveInfinity
		for _, part := range op.parts {
			var curDocId int64
			var curScore float32
			for {
				curDocId, curScore = part.Cur()
				if curDocId >= minId {
					break
				}
				if ! part.Next(minId) {
					return false
				}
			}
			if curDocId > minId {
				minId = curDocId
				keepGoing = true
				break
			}
			score = Min(score, curScore)
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
	//fmt.Printf("MinDocItr Next(%v) [%v:%v] = %v score:%v\n", minId, op.min, op.max, op.docId, score)
	return true
}

func (op *MinDocItr) SetBounds(min, max float32) bool {
	fmt.Printf("MinDocItr SetBounds %v %v\n", min, max)
	op.min = min
	for _, component := range op.parts {
		curMin, curMax := component.GetBounds()
		if curMin < min {
			//fmt.Printf("MinDocItr SetBounds for component %v %v\n", min, curMax)
			if ! component.SetBounds(min, curMax) {
				return false
			}
		}
	}
	return true
}
