package scoredb

import (
	"testing"
)

func TestShardedDb(t *testing.T) {
	pathmaker := RmAllTestData()
	defer RmAllTestData()
	idDb, err := NewBoltIdDb(pathmaker("shard_ids"))
	if err != nil {
		t.Fatal(err)
	}
	db := BaseDb{
		StreamingDb: ShardedDb{
			Shards: []StreamingDb{
				BaseStreamingDb{NewFsScoreDb(pathmaker("shard_1"))},
				BaseStreamingDb{NewFsScoreDb(pathmaker("shard_2"))},
			},
		},
		IdDb: idDb,
	}
	DbBasicsTest(db, t)
}

