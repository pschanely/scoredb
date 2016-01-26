package scoredb

import (
	"testing"
)

func TestMemoryScoreDb(t *testing.T) {
	db := BaseDb{StreamingDb: BaseStreamingDb{NewMemoryScoreDb()}, IdDb: NewMemoryIdDb()}
	DbBasicsTest(db, t)
}
