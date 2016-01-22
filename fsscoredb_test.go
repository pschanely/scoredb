package scoredb

import (
	"testing"
)

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

