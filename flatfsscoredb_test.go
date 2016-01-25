package scoredb

import (
	"testing"
)

func TestFlatDb(t *testing.T) {
	db := BaseDb{StreamingDb: BaseStreamingDb{NewFlatFsScoreDb("datatest_flat_1")}, IdDb: NewMemoryIdDb()}
	DbBasicsTest(db, t)
}

