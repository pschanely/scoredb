package scoredb

import (
	"testing"
)

func TestShardedDb(t *testing.T) {
	idDb, err := NewBoltIdDb("datatest_shard_ids")
	if err != nil {
		t.Fatal(err)
	}
	db := BaseDb{
		StreamingDb: ShardedDb{
			Shards: []StreamingDb{
				BaseStreamingDb{NewFsScoreDb("datatest_shard_1")},
				BaseStreamingDb{NewFsScoreDb("datatest_shard_2")},
			},
		},
		IdDb: idDb,
	}
	DbBasicsTest(db, t)
}

