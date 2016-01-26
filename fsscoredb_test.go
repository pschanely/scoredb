package scoredb

import (
	"fmt"
	"testing"
)

func TestFsScore(t *testing.T) {
	testdir := RmAllTestData()("fsscoredb.1")
	defer RmAllTestData()
	db := BaseDb{StreamingDb: BaseStreamingDb{NewFsScoreDb(testdir)}, IdDb: NewMemoryIdDb()}
	DbBasicsTest(db, t)
}

func TestFsScoreLarge(t *testing.T) {
	testdir := RmAllTestData()("fsscoredb.2")
	defer RmAllTestData()
	db := BaseDb{StreamingDb: BaseStreamingDb{NewFsScoreDb(testdir)}, IdDb: NewMemoryIdDb()}

	for i := 0; i < 100; i++ {
		db.Index(fmt.Sprintf("r%d", i), map[string]float32{"age": float32(1000 + 100 - i), "height": 100 + 1.0 + float32(i%10)/10.0})
	}

	CallAndCheck(db, t, []string{"r0", "r1"}, 2, []interface{}{"sum",
		[]interface{}{"field", "age"},
		[]interface{}{"field", "height"}})
}
