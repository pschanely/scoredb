package scoredb

import (
	"sort"
)

type ProductComponents []DocItr

func (a ProductComponents) Len() int           { return len(a) }
func (a ProductComponents) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ProductComponents) Less(i, j int) bool {
	min1, max1 := a[i].GetBounds()
	min2, max2 := a[j].GetBounds()
	return max1 - min1 > max2 - min2
}

type ProductDocItr struct {
	score    float32
	docId    int64
	min, max float32
	parts    ProductComponents
}

func NewProductDocItr(itrs []DocItr) *ProductDocItr {
	min, max := float32(0.0), float32(0.0)
	components := make(ProductComponents, len(itrs))
	for idx, part := range itrs {
		curMin, curMax := part.GetBounds()
		//fmt.Printf("Init %v %v %v\n", idx, curMin, curMax)
		components[idx] = part
		if idx == 0 {
			min, max = curMin, curMax
		} else {
			// assumes positive inputs:
			min *= curMin
			max *= curMax
		}
	}
	sort.Sort(components)
	return &ProductDocItr{
		score: 0.0,
		docId: -1,
		min:   min,
		max:   max,
		parts: components,
	}
}

func (op *ProductDocItr) Name() string { return "ProductDocItr" }
func (op *ProductDocItr) Cur() (int64, float32) {
	return op.docId, op.score
}
func (op *ProductDocItr) GetBounds() (min, max float32) { return op.min, op.max }
func (op *ProductDocItr) Close() {
	for _, part := range op.parts {
		part.Close()
	}
}
func (op *ProductDocItr) Next(minId int64) bool {
	min, max := op.min, op.max
	keepGoing := true
	var score float32
	for keepGoing {
		//fmt.Printf("ProductDocItr Next itr (%v) [%v:%v] = %v score:%v\n", minId, op.min, op.max, op.docId, score)
		keepGoing = false
		score = float32(1.0)
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
			score *= curScore
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
	//fmt.Printf("ProductDocItr Next(%v) [%v:%v] = %v score:%v\n", minId, op.min, op.max, op.docId, score)
	return true
}

func (op *ProductDocItr) SetBounds(min, max float32) bool {
	//fmt.Printf("ProductDocItr SetBounds %v %v\n", min, max)
	op.min = min
	op.max = max

	for curfield, component := range op.parts {
		newMin, newMax := min, max
		for otherfactor, otherComponent := range op.parts {
			// Then divide by the other maxes or mins
			if curfield != otherfactor {
				otherMin, otherMax := otherComponent.GetBounds()
				if otherMax == 0.0 {
					newMin = 0.0
				} else {
					newMin /= otherMax
				}
				if otherMin == 0.0 {
					newMax = PositiveInfinity
				} else {
					newMax /= otherMin
				}
			}
		}
		curMin, curMax := component.GetBounds()
		if newMin < curMin {
			newMin = curMin
		}
		if newMax > curMax {
			newMax = curMax
		}
		if newMin != curMin || newMax != curMax {
			//fmt.Printf("ProductDocItr SetBounds for component %v %v\n", newMin, newMax)
			component.SetBounds(newMin, newMax)
		}
	}
	return true
}
