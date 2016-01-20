package main

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
func (op *PowDocItr) DocId() int64 {
	return op.itr.DocId()
}
func (op *PowDocItr) Close() {
	op.itr.Close()
}
func (op *PowDocItr) Next(minId int64) bool {
	return op.itr.Next(minId)
}
func (op *PowDocItr) Score() float32 {
	return Pow(op.itr.Score(), op.exp)
}
func (op *PowDocItr) GetBounds() (min, max float32) {
	exp := op.exp
	min, max = op.itr.GetBounds()
	min = Pow(min, exp)
	max = Pow(max, exp)
	return min, max
}
func (op *PowDocItr) SetBounds(min, max float32) bool {
	oneOverExp := op.oneOverExp
	return op.itr.SetBounds(Pow(min, oneOverExp), Pow(max, oneOverExp))
}
