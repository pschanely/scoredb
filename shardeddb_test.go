package main

import (
	"testing"
)

func TestShardedDb(t *testing.T) {
	db := BaseDb{ShardedDb{
		Shards: []StreamingDb{
			BaseStreamingDb{NewFsScoreDb("datatest_shard_1")},
			BaseStreamingDb{NewFsScoreDb("datatest_shard_2")},
		},
	}}
	DbBasicsTest(db, t)
}

