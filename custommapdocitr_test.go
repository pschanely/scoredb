package scoredb

import (
	"testing"
)

func TestCustomMapDocItr(t *testing.T) {
	inside := NewMemoryScoreDocItr([]float32{-1, 0, 2, 8, 5, 9, 12})
	outside := CustomMapDocItr{
		docItr: inside,
		deflt:  0.0,
		points: map[float32]float32{ // kind of a zig-zag function...
			-2: -2,
			2:  2,
			5:  3,
			6:  1,
		},
	}

	min, max := inside.GetBounds()
	if !BoundsEqualish(min, max, -1, 12) {
		t.Fatalf("%v:%v", min, max)
	}
	min, max = outside.GetBounds()
	if !BoundsEqualish(min, max, 0.0, 3.0) {
		t.Fatalf("%v:%v", min, max)
	}

	// should leave unchanged
	outside.SetBounds(-2, 4)
	min, max = inside.GetBounds()
	if !BoundsEqualish(min, max, -1, 12) {
		t.Fatalf("%v:%v", min, max)
	}
	min, max = outside.GetBounds()
	if !BoundsEqualish(min, max, 0.0, 3.0) {
		t.Fatalf("%v:%v", min, max)
	}

	// nudge the start up some
	outside.SetBounds(0.25, 3)
	min, max = inside.GetBounds()
	if !BoundsEqualish(min, max, 2, 6) {
		t.Fatalf("%v:%v", min, max)
	}
	min, max = outside.GetBounds()
	if !BoundsEqualish(min, max, 0.0, 3.0) {
		t.Fatalf("%v:%v", min, max)
	}

	outside.SetBounds(0.5, 1.5)
	min, max = inside.GetBounds()
	if !BoundsEqualish(min, max, 6, 6) {
		t.Fatalf("%v:%v", min, max)
	}
	min, max = outside.GetBounds()
	if !BoundsEqualish(min, max, 0, 1.0) {
		t.Fatalf("%v:%v", min, max)
	}

}
