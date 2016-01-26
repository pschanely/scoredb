package scoredb

import (
	"testing"
)

func TestFieldOp(t *testing.T) {
	l1 := NewMemoryDocItr(
		[]float32{1.0, 1.0, 0.5, 1.0, 0.5},
		[]int64{1, 5, 7, 8, 9},
	)
	l2 := NewMemoryDocItr(
		[]float32{1.0, 1.0},
		[]int64{2, 5},
	)
	fieldop := FieldDocItr{lists: FieldDocItrs{l1, l2}}
	if !fieldop.Next(0) {
		t.FailNow()
	}
	docId, _ := fieldop.Cur()
	if docId != 1 {
		t.FailNow()
	}
	if !fieldop.Next(2) {
		t.FailNow()
	}
	docId, _ = fieldop.Cur()
	if docId != 2 {
		t.FailNow()
	}
	if !fieldop.Next(3) {
		t.FailNow()
	}
	docId, _ = fieldop.Cur()
	if docId != 5 {
		t.FailNow()
	}
	if !fieldop.SetBounds(0.75, 1.0) {
		t.FailNow()
	}
	if !fieldop.Next(6) {
		t.FailNow()
	}
	docId, _ = fieldop.Cur()
	if docId != 8 {
		t.FailNow()
	}
	if fieldop.Next(9) {
		t.FailNow()
	}
}
