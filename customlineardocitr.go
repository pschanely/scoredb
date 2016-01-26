package scoredb

import (
	"sort"
)

type CustomPoint struct {
	X, Y float32
}

// Remaps a value according to a user-specified function that linearly interpolates 
// among a set of (x, y) points.
type CustomLinearDocItr struct {
	points []CustomPoint
	docItr DocItr
}

func ComputeCustomFunc(x float32, points []CustomPoint) float32 {
	numPoints := len(points)
	idx := sort.Search(numPoints, func(i int) bool {
		return points[i].X >= x 
	})
	if idx == 0 {
		return points[0].Y
	} else if idx == numPoints {
		return points[numPoints - 1].Y
	} else {
		p1 := points[idx - 1]
		p2 := points[idx]
		pctInto := (x - p1.X) / (p2.X - p1.X)
		return p2.Y * pctInto + p1.Y * (1.0 - pctInto)
	}
}

func (op *CustomLinearDocItr) Name() string { return "CustomLinearDocItr" }
func (op *CustomLinearDocItr) Cur() (int64, float32) {
	docId, score := op.docItr.Cur()
	return docId, ComputeCustomFunc(score, op.points)
}
func (op *CustomLinearDocItr) GetBounds() (min, max float32) {
	insideMin, insideMax := op.docItr.GetBounds()
	outsideMin := ComputeCustomFunc(insideMin, op.points)
	outsideMax := ComputeCustomFunc(insideMax, op.points)
	if outsideMin > outsideMax { // swap if required
		outsideMin, outsideMax = outsideMax, outsideMin
	}
	// functions need not be monotonic, check for peaks inside the X range
	for _, point := range op.points {
		if point.X <= insideMin {
			continue
		} else if point.X >= insideMax {
			break
		} else {
			y := point.Y
			outsideMax = Max(outsideMax, y)
			outsideMin = Min(outsideMin, y)
		}
	}
	return outsideMin, outsideMax
}
func (op *CustomLinearDocItr) Close() {
	op.docItr.Close()
}
func (op *CustomLinearDocItr) Next(minId int64) bool {
	return op.docItr.Next(minId)
}

func CheckIntersection(yValue float32, p1, p2 CustomPoint, insideMin, insideMax *float32) {
	var xIntersect float32
	// intersect descending:  y 3 at {3 3}-{6 1}: 0
	if p1.Y <= yValue && yValue <= p2.Y { // intersect while function is ascending
		earliness := (p2.Y - yValue) / (p2.Y - p1.Y)
		xIntersect = p1.X * earliness + p2.X * (1.0 - earliness)
	} else if p1.Y >= yValue && yValue >= p2.Y { // intersect while function is descending
		lateness := (p1.Y - yValue) / (p1.Y - p2.Y)
		xIntersect = p2.X * lateness + p1.X * (1.0 - lateness)
	} else {
		return
	}
	*insideMin = Min(xIntersect, *insideMin)
	*insideMax = Max(xIntersect, *insideMax)
}

func (op *CustomLinearDocItr) SetBounds(outsideMin, outsideMax float32) bool {
	insideMin, insideMax := PositiveInfinity, NegativeInfinity // start with impossible (inverted) range
	for idx := len(op.points) - 1; idx > 0; idx -- {
		p1 := op.points[idx - 1]
		p2 := op.points[idx]
		CheckIntersection(outsideMin, p1, p2, &insideMin, &insideMax)
		CheckIntersection(outsideMax, p1, p2, &insideMin, &insideMax)
		if outsideMin <= p2.Y && p2.Y <= outsideMax {
			insideMin = Min(insideMin, p2.X)
			insideMax = Max(insideMax, p2.X)
		}
	}
	firstPoint := op.points[0]
	if outsideMin <= firstPoint.Y && firstPoint.Y <= outsideMax {
		insideMin = NegativeInfinity
	}
	lastPoint := op.points[len(op.points) - 1]
	if outsideMin <= lastPoint.Y && lastPoint.Y <= outsideMax {
		insideMax = PositiveInfinity
	}
	return op.docItr.SetBounds(insideMin, insideMax)
}
