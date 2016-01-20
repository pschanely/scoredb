package main

import (
	"testing"
)

func TestMemoryScoreDb(t *testing.T) {
	db := BaseDb{BaseStreamingDb{NewMemoryScoreDb()}}
	DbBasicsTest(db, t)
}

