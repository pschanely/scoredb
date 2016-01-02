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

func DoQuery(db *FsScoreDb, numResults int, weights map[string]float32) []int64 {
	scorer := make([]interface{}, len(weights)+1)
	scorer[0] = "+"
	idx := 1
	for key, weight := range weights {
		scorer[idx] = []interface{}{"scale", weight, []interface{}{"field", key}}
		idx += 1
	}
	result, _ := db.Query(Query{
		Limit:  numResults,
		Scorer: scorer,
	})
	return result.Ids
}

func TestFsScore(t *testing.T) {
	db := NewFsScoreDb("datatest1")
	fmt.Print(" =================== \n")
	_, err := db.Index(map[string]float32{"age": 32, "height": 2.0})
	if err != nil {
		t.Error(fmt.Sprintf("%v", err))
	}
	_, err = db.Index(map[string]float32{"age": 25, "height": 1.8})
	if err != nil {
		t.Error(fmt.Sprintf("%v", err))
	}
	_, err = db.Index(map[string]float32{"age": 16, "height": 2.5})
	if err != nil {
		t.Error(fmt.Sprintf("%v", err))
	}
	fmt.Print(" ------------------- \n")
	if Isnt([]int64{1, 2}, DoQuery(db, 2, map[string]float32{"age": 1.0, "height": 1.0})) {
		t.Error()
	}
	fmt.Print(" =================== \n")
	if Isnt([]int64{1}, DoQuery(db, 1, map[string]float32{"age": 1.0, "height": 1.0})) {
		t.Error()
	}
	if Isnt([]int64{3, 1}, DoQuery(db, 2, map[string]float32{"age": 0.1, "height": 10.0})) {
		t.Error()
	}
	if Isnt([]int64{3, 2}, DoQuery(db, 2, map[string]float32{"age": -1.0, "height": -1.0})) {
		t.Error()
	}
	if Isnt([]int64{2, 1, 3}, DoQuery(db, 3, map[string]float32{"age": 1.0, "height": -100.0})) {
		t.Error()
	}
	if Isnt([]int64{}, DoQuery(db, 0, map[string]float32{"age": 1.0, "height": 1.0})) {
		t.Error()
	}
}

func TestFsScoreLarge(t *testing.T) {
	db := NewFsScoreDb("datatest2")

	for i := 0; i < 100; i++ {
		db.Index(map[string]float32{"age": float32(1000 + 100 - i), "height": 100 + 1.0 + float32(i%10)/10.0})
	}

	if Isnt([]int64{1, 2}, DoQuery(db, 2, map[string]float32{"age": 1.0, "height": 0.1})) {
		t.Error("")
	}
}
