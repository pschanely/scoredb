package main

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
	if !fieldop.Next() {
		t.Error()
	}
	if fieldop.DocId() != 1 {
		t.Error()
	}
	if !fieldop.Next() {
		t.Error()
	}
	if fieldop.DocId() != 2 {
		t.Error()
	}
	if !fieldop.Next() {
		t.Error()
	}
	if fieldop.DocId() != 5 {
		t.Error()
	}
	if !fieldop.SetBounds(0.75, 1.0) {
		t.Error()
	}
	if !fieldop.Next() {
		t.Error()
	}
	if id := fieldop.DocId(); id != 8 {
		t.Error(id)
	}
	if fieldop.Next() {
		t.Error()
	}
}
