package scoredb

import (
	"testing"
)

func TestFlatDb(t *testing.T) {
	db := BaseDb{BaseStreamingDb{NewFlatFsScoreDb("datatest_flat_1")}}
	DbBasicsTest(db, t)
}

