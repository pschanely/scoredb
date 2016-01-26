package scoredb

import (
	"testing"
)

func TestProductDocItr(t *testing.T) {
	i1 := NewMemoryScoreDocItr([]float32{0.2, 0.8, 0.5})
	i2 := NewMemoryScoreDocItr([]float32{1.0, 0.0, 0.7})
	itr := NewProductDocItr([]DocItr{i1, i2})

	if itr.min != 0.0 {
		t.Fatalf("%v", itr.min)
	}
	if itr.max != 0.8 {
		t.Fatalf("%v", itr.max)
	}

	itr.SetBounds(0.5, 1.0)

	min1, _ := i1.GetBounds()
	min2, _ := i2.GetBounds()

	if min1 != 0.5 {
		t.Fatalf("%v", min1)
	}
	if min2*0.2 == 0.5 {
		t.Fatalf("%v", min2)
	}
}
