package main

import (
	"fmt"
	"testing"
)

func Isnt(r1, r2 []int64) bool {
	fmt.Printf("Is? %v %v\n", r1, r2)
	if len(r1) != len(r2) {
		return true
	}
	for idx, v1 := range r1 {
		if v1 != r2[idx] {
			return true
		}
	}
	return false
}

func TestFsScore(t *testing.T) {
	db := FsScoreDb{dataDir: "datatest1", nextId: 1}
	db.Index(map[string]float32{"age": 32, "height": 2.0})
	db.Index(map[string]float32{"age": 25, "height": 1.8})
	db.Index(map[string]float32{"age": 16, "height": 2.5})
	if Isnt([]int64{1, 2}, db.Query(2, map[string]float32{"age": 1.0, "height": 1.0})) {
		t.Error()
	}
	if Isnt([]int64{1}, db.Query(1, map[string]float32{"age": 1.0, "height": 1.0})) {
		t.Error()
	}
	if Isnt([]int64{3, 1}, db.Query(2, map[string]float32{"age": 0.1, "height": 10.0})) {
		t.Error()
	}
}

func TestFsScoreLarge(t *testing.T) {
	db := FsScoreDb{dataDir: "datatest2", nextId: 1}

	for i := 0; i < 100; i++ {
		db.Index(map[string]float32{"age": float32(1000 + 100 - i), "height": 100 + 1.0 + float32(i % 10) / 10.0})
	}

	if Isnt([]int64{1, 2}, db.Query(2, map[string]float32{"age": 1.0, "height": 0.1})) {
		t.Error("")
	}
}
