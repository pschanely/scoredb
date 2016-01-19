package main

import (
	"fmt"
	"testing"
)

func CallAndCheck(db Db, t *testing.T, r1 []int64, limit int, scorer []interface{}) {
	r2, err := db.Query(Query{Limit:limit, Scorer:scorer})
	fmt.Printf("Is? %v %v (err:%v)\n", r1, r2, err)
	if (err != nil) {
		t.FailNow()
	}
	if len(r1) != len(r2.Ids) {
		t.FailNow()
	}
	for idx, v1 := range r1 {
		if v1 != r2.Ids[idx] {
			t.FailNow()
		}
	}
}

func TestFsScore(t *testing.T) {
	db := BaseDb{BaseStreamingDb{NewFsScoreDb("datatest1")}}
	DbBasicsTest(db, t)
}

func TestFsScoreLarge(t *testing.T) {
	db := BaseDb{BaseStreamingDb{NewFsScoreDb("datatest2")}}

	for i := 0; i < 2; i++ {
		db.Index(map[string]float32{"age": float32(1000 + 100 - i), "height": 100 + 1.0 + float32(i%10)/10.0})
	}

	CallAndCheck(db, t, []int64{1, 2}, 2, []interface{}{"sum",
		[]interface{}{"field", "age"},
		[]interface{}{"field", "height"}})
}

