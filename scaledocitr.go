package main

import ()

// Multiplies a value by a constant
type ScaleDocItr struct {
	factor float32
	docItr DocItr
}

func (op *ScaleDocItr) Name() string { return "ScaleDocItr" }
func (op *ScaleDocItr) Cur() (int64, float32) {
	docId, score := op.docItr.Cur()
	return docId, score * op.factor
}
func (op *ScaleDocItr) GetBounds() (min, max float32) {
	min, max = op.docItr.GetBounds()
	factor := op.factor
	if factor >= 0 {
		return min * op.factor, max * op.factor
	} else {
		return max * op.factor, min * op.factor
	}
}
func (op *ScaleDocItr) Close() {
	op.docItr.Close()
}
func (op *ScaleDocItr) Next(minId int64) bool {
	return op.docItr.Next(minId)
}

func (op *ScaleDocItr) SetBounds(min, max float32) bool {
	factor := op.factor
	if factor >= 0 {
		return op.docItr.SetBounds(min/op.factor, max/op.factor)
	} else {
		return op.docItr.SetBounds(max/op.factor, min/op.factor)
	}
}
