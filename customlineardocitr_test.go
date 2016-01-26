package scoredb

import (
	"math"
	"testing"
)


func BoundsEqualish(actualMin, actualMax, expectedMin, expectedMax float32) bool {
	tolerance := 0.0000001
	if (math.Abs(float64(actualMin - expectedMin)) > tolerance) {
		return false
	}
	if (math.Abs(float64(actualMax - expectedMax)) > tolerance) {
		return false
	}
	return true
}

func TestComputeCustomFunc(t *testing.T) {
	v := ComputeCustomFunc(1.0, []CustomPoint{CustomPoint{0.0, 0.0}, CustomPoint{3.0, 3.0}})
	if v != 1.0 {
		t.Fatalf("%v", v)
	}
	v = ComputeCustomFunc(-1, []CustomPoint{CustomPoint{0,0}, CustomPoint{3,3}})
	if v != 0.0 {
		t.Fatalf("%v", v)
	}
	v = ComputeCustomFunc(3, []CustomPoint{CustomPoint{0,0}, CustomPoint{3,3}})
	if v != 3.0 {
		t.Fatalf("%v", v)
	}
}

func TestCustomLinearDocItr(t *testing.T) {
	inside := NewMemoryScoreDocItr([]float32{-1, 0, 2, 8, 5, 9, 12})
	outside := CustomLinearDocItr{
		docItr: inside, 
		points:[]CustomPoint{
			CustomPoint{0, 0}, // kind of a zig-zag function...
			CustomPoint{3, 3},
			CustomPoint{6, 1},
			CustomPoint{9, 2},
		},
	}

	min, max := inside.GetBounds()
	if ! BoundsEqualish(min, max, -1, 12) {
		t.Fatalf("%v:%v", min, max)
	}
	min, max = outside.GetBounds()
	if ! BoundsEqualish(min, max, 0.0, 3.0) {
		t.Fatalf("%v:%v", min, max)
	}

	// should leave unchanged
	outside.SetBounds(0, 4)
	min, max = inside.GetBounds()
	if ! BoundsEqualish(min, max, -1, 12) {
		t.Fatalf("%v:%v", min, max)
	}
	min, max = outside.GetBounds()
	if ! BoundsEqualish(min, max, 0.0, 3.0) {
		t.Fatalf("%v:%v", min, max)
	}

	// nudge the start up some
	outside.SetBounds(0.5, 3)
	min, max = inside.GetBounds()
	if ! BoundsEqualish(min, max, 0.5, 12) {
		t.Fatalf("%v:%v", min, max)
	}
	min, max = outside.GetBounds()
	if ! BoundsEqualish(min, max, 0.5, 3.0) {
		t.Fatalf("%v:%v", min, max)
	}

	// chop off the end (leaves a hole in the middle of the function)
	outside.SetBounds(0.5, 1.5)
	min, max = inside.GetBounds()
	if ! BoundsEqualish(min, max, 0.5, 7.5) {
		t.Fatalf("%v:%v", min, max)
	}
	min, max = outside.GetBounds()
	if ! BoundsEqualish(min, max, 0.5, 3.0) {
		t.Fatalf("%v:%v", min, max)
	}

	// chop off most of the end
	outside.SetBounds(0.5, 0.9)
	min, max = inside.GetBounds()
	if ! BoundsEqualish(min, max, 0.5, 0.9) {
		t.Fatalf("%v:%v", min, max)
	}
	min, max = outside.GetBounds()
	if ! BoundsEqualish(min, max, 0.5, 0.9) {
		t.Fatalf("%v:%v", min, max)
	}

}

