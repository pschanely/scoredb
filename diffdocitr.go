package scoredb

import ()

// (Absolute) difference between a value and a constant
type DiffDocItr struct {
	target float32
	itr DocItr
}

func Abs(val float32) float32 {
	if val < 0 {
		return -val
	} else {
		return val
	}
}

func Max(v1, v2 float32) float32 {
	if v1 < v2 {
		return v2
	} else {
		return v1
	}
}

func Min(v1, v2 float32) float32 {
	if v1 > v2 {
		return v2
	} else {
		return v1
	}
}

func (op *DiffDocItr) Name() string { return "DiffDocItr" }
func (op *DiffDocItr) Cur() (int64, float32) {
	docId, score := op.itr.Cur()
	return docId, Abs(score - op.target)
}
func (op *DiffDocItr) GetBounds() (min, max float32) {
	target := op.target
	min, max = op.itr.GetBounds()
	d1 := Abs(min - target)
	d2 := Abs(max - target)
	maxDist := Max(d1, d2)
	if min <= target && target <= max {
		return 0.0, maxDist
	} else {
		return Min(d1, d2), maxDist
	}
}
func (op *DiffDocItr) Close() {
	op.itr.Close()
}
func (op *DiffDocItr) Next(minId int64) bool {
	return op.itr.Next(minId)
}

func (op *DiffDocItr) SetBounds(min, max float32) bool {
	// min is not useful to us right now
	target := op.target
	return op.itr.SetBounds(target - max, target + max)
}
