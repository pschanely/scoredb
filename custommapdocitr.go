package scoredb

import (
)

// Remaps a value according to a user-specified mapping of values to scores
type CustomMapDocItr struct {
	points map[float32]float32
	deflt float32
	docItr DocItr
}

func (op *CustomMapDocItr) ComputeCustomFunc(val float32) float32 {
	score, ok := op.points[val]
	if ok {
		return score
	} else {
		return op.deflt
	}
}

func (op *CustomMapDocItr) Name() string { return "CustomMapDocItr" }
func (op *CustomMapDocItr) Cur() (int64, float32) {
	docId, score := op.docItr.Cur()
	return docId, op.ComputeCustomFunc(score)
}
func (op *CustomMapDocItr) GetBounds() (min, max float32) {
	insideMin, insideMax := op.docItr.GetBounds()
	outsideMin := op.deflt
	outsideMax := op.deflt
	for input, output := range op.points {
		if insideMin <= input && input <= insideMax {
			outsideMin = Min(outsideMin, output)
			outsideMax = Max(outsideMax, output)
		}
	}
	return outsideMin, outsideMax
}
func (op *CustomMapDocItr) Close() {
	op.docItr.Close()
}
func (op *CustomMapDocItr) Next(minId int64) bool {
	return op.docItr.Next(minId)
}

func (op *CustomMapDocItr) SetBounds(outsideMin, outsideMax float32) bool {
	if outsideMin <= op.deflt && op.deflt <= outsideMax {
		return true
	}

	insideMin, insideMax := PositiveInfinity, NegativeInfinity // start with impossible (inverted) range
	for input, output := range op.points {
		if outsideMin <= output && output <= outsideMax {
			insideMin = Min(insideMin, input)
			insideMax = Max(insideMax, input)
		}
	}
	return op.docItr.SetBounds(insideMin, insideMax)
}
