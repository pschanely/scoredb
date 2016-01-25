package scoredb

import (
	"testing"
)

func TestFlatDb(t *testing.T) {
	testdir := RmAllTestData()("datatest_flat")
	defer RmAllTestData()
	db := BaseDb{StreamingDb: BaseStreamingDb{NewFlatFsScoreDb(testdir)}, IdDb: NewMemoryIdDb()}
	DbBasicsTest(db, t)
}

