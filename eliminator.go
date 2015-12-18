package main

import (
	"fmt"
)

//in boundvals and elimvals, index 0 is the min and index 1 is the max.
func Elim(eqmap map[string]float32, bval float32, boundvals map[string][2]float32) (elimvals map[string][2]float32) {
	elimvals = make(map[string][2]float32)
	for curfield, curweight := range eqmap {
		//To start, divige the bottom value by the weight of the variable we're testing for
		elimval := bval
		for otherfactor, otherweight := range eqmap {
			//Then subtract the other weights times their appropriate maxes or mins
			if curfield != otherfactor {
				minmax := 1
				if otherweight < 0 {
					minmax = 0
				}
				elimval -= otherweight * boundvals[otherfactor][minmax]
			}
		}
		elimval /= curweight
		//Assing the new value as a min if the wieght is positive, and as a max if the weight is negative.
		if curweight > 0 {
			if elimval < boundvals[curfield][0] {
				elimval = boundvals[curfield][0]
			}
			elimvals[curfield] = [2]float32{elimval, boundvals[curfield][1]}
		} else {
			if elimval > boundvals[curfield][1] {
				elimval = boundvals[curfield][1]
			}
			elimvals[curfield] = [2]float32{boundvals[curfield][0], elimval}
		}
	}
	return
}

func (op *LinearDocItr) SetBounds(min, max float32) bool {
	fmt.Printf("LinearDocItr SetBounds %v %v\n", min, max)
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
			if elimval < curMin {
				elimval = curMin
			}
			linComponent.docItr.SetBounds(elimval, curMax)
			fmt.Printf("SetBounds min %v %v\n", elimval, curMax)
		} else {
			if elimval > curMax {
				elimval = curMax
			}
			linComponent.docItr.SetBounds(curMin, elimval)
			fmt.Printf("SetBounds max %v %v\n", curMin, elimval)
		}
	}
	return true
}
