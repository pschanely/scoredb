package scoredb

import (
	"testing"
)

func TestMemoryScoreDb(t *testing.T) {
	db := BaseDb{BaseStreamingDb{NewMemoryScoreDb()}}
	DbBasicsTest(db, t)
}

