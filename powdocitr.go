package scoredb

import (
	"math"
)

// Takes a constant power of a value.
// Important: for bounds caluclation reasons, assumes only positive values are provided as inputs!
type PowDocItr struct {
	exp, oneOverExp float32
	itr DocItr
}

func Pow(val, exp float32) float32 {
	return float32(math.Pow(float64(val), float64(exp)))
}

func (op *PowDocItr) Name() string { return "PowDocItr" }
func (op *PowDocItr) Cur() (int64, float32) {
	docId, score := op.itr.Cur()
	return docId, Pow(score, op.exp)
}
func (op *PowDocItr) Close() {
	op.itr.Close()
}
func (op *PowDocItr) Next(minId int64) bool {
	return op.itr.Next(minId)
}
func (op *PowDocItr) GetBounds() (min, max float32) {
	exp := op.exp
	min, max = op.itr.GetBounds()
	v1 := Pow(min, exp)
	v2 := Pow(max, exp)
	if v1 < v2 {
		return v1, v2
	} else {
		return v2, v1
	}
}
func (op *PowDocItr) SetBounds(min, max float32) bool {
	oneOverExp := op.oneOverExp
	v1 := Pow(min, oneOverExp) 
	v2 := Pow(max, oneOverExp)
	if v1 < v2 {
		return op.itr.SetBounds(v1, v2)
	} else {
		return op.itr.SetBounds(v2, v1)
	}
}
