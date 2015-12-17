package main

//in boundvals and elimvals, index 0 is the min and index 1 is the max.
func Elim(eqmap map[string]float32, bval float32, boundvals map[string][2]float32) (elimvals map[string][2]float32) {
	elimvals = make(map[string][2]float32)
	for curfield, curweight := range eqmap {
		//To start, divige the bottom value by the weight of the variable we're testing for
		elimval := bval/curweight
		for otherfactor, otherweight := range eqmap {
			//Then subtract the other weights times their appropriate maxes or mins
			if curfield!=otherfactor {
				minmax :=1
				if otherweight < 0 {
					minmax = 0
				}
				elimval -= otherweight*boundvals[otherfactor][minmax]
			}
		}
		//Assing the new value as a min if the wieght is positive, and as a max if the weight is negative.
		if curweight > 0 {
			elimvals[curfield] = [2]float32 {elimval, boundvals[curfield][1]}		
		} else {
			elimvals[curfield] = [2]float32 {boundvals[curfield][0], elimval}
		}
	}
	return
}	