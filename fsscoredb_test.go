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
	/*
		db := FsScoreDb{dataDir:"datatest", nextId:1}
		db.Index(map[string]float32{"age":32, "height":2.0})
		db.Index(map[string]float32{"age":16, "height":1.5})
		if Isnt([]int64{1, 2}, db.Query(2, map[string]float32{"age":1.0, "height":1.0})) {t.Error()}
	*/
}
