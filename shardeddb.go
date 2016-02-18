package scoredb

import (
	"fmt"
	"math"
	"math/rand"
)

type ShardedDb struct {
	Shards []StreamingDb
}

var reservedShardBits = uint(14)

func NewShardedDb(shards []StreamingDb) (*ShardedDb, error) {
	maxShards := (1 << reservedShardBits) - 1
	if len(shards) >= 1<<reservedShardBits {
		return nil, fmt.Errorf("Too many shards (%d); maximum number of shards is %d", len(shards), maxShards)
	}
	return &ShardedDb{Shards: shards}, nil
}

func ShardIdToExt(idInShard int64, shardNum int) int64 {
	return (int64(shardNum) << uint(64-reservedShardBits)) | idInShard
}

func (db ShardedDb) BulkIndex(records []map[string]float32) ([]int64, error) {
	numShards := len(db.Shards)
	// TODO do something more complex some day?  Parallelize it like the query side?
	shardNum := rand.Intn(numShards)
	results, err := db.Shards[shardNum].BulkIndex(records)
	if err != nil {
		return nil, err
	}
	for idx, v := range results {
		results[idx] = ShardIdToExt(v, shardNum)
	}
	return results, nil
}

func (db ShardedDb) QueryItr(scorer []interface{}) (DocItr, error) {
	parts := make([]DocItr, len(db.Shards))
	for idx, shard := range db.Shards {
		itr, err := shard.QueryItr(scorer)
		if err != nil {
			return nil, err
		}
		parts[idx] = itr
	}
	return NewParallelDocItr(parts), nil
}

type CandidateResult struct {
	DocId     int64
	Score     float32
	WorkerNum int
}

type Bounds struct {
	min, max float32
}

type ParallelDocItr struct {
	score         float32
	docId         int64
	NumAlive      int
	Bounds        Bounds
	ResultChannel chan CandidateResult
	Comms         []chan Bounds
}

func RunItr(itr DocItr, myWorkerNum int, resultChannel chan CandidateResult, boundsChannel chan Bounds) {
	bounds := Bounds{min: float32(math.Inf(-1)), max: float32(math.Inf(1))}
	docId := int64(-1)
	var score float32
	for {
		if !itr.Next(docId + 1) {
			break
		}
		docId, score = itr.Cur()
		if score <= bounds.min || score >= bounds.max {
			continue
		}
		resultChannel <- CandidateResult{DocId: docId, Score: score, WorkerNum: myWorkerNum}
		/*
			select {
			case newBounds, ok := <- boundsChannel:
				if ok {
					if bounds != newBounds {
						bounds = newBounds
						itr.SetBounds(bounds.min, bounds.max)
					}
				}
			}
		*/

		newBounds := <-boundsChannel

		if bounds != newBounds {
			bounds = newBounds
			itr.SetBounds(bounds.min, bounds.max)
		}

	}
	itr.Close()
	resultChannel <- CandidateResult{DocId: -1}
}

func NewParallelDocItr(parts []DocItr) *ParallelDocItr {
	op := ParallelDocItr{
		score:         0.0,
		docId:         -1,
		NumAlive:      len(parts),
		Bounds:        Bounds{min: float32(math.Inf(-1)), max: float32(math.Inf(1))},
		ResultChannel: make(chan CandidateResult),
		Comms:         make([](chan Bounds), len(parts)),
	}
	for idx, part := range parts {
		part := part
		curMin, curMax := part.GetBounds()
		op.Bounds.min = Min(op.Bounds.min, curMin)
		op.Bounds.max = Max(op.Bounds.max, curMax)
		boundsChannel := make(chan Bounds)
		op.Comms[idx] = boundsChannel
		go RunItr(part, idx, op.ResultChannel, boundsChannel)
	}
	return &op
}

func (op *ParallelDocItr) Name() string {
	return "ParallelDocItr"
}

func (op *ParallelDocItr) SetBounds(min, max float32) bool {
	op.Bounds.min, op.Bounds.max = min, max
	return true
}

func (op *ParallelDocItr) GetBounds() (min, max float32) {
	return op.Bounds.min, op.Bounds.max
}

func (op *ParallelDocItr) Next(minId int64) bool {
	for {
		result := <-op.ResultChannel
		if result.DocId == -1 {
			op.NumAlive -= 1
			if op.NumAlive <= 0 {
				return false
			}
		} else {
			workerNum := result.WorkerNum
			if result.Score > op.Bounds.min && result.Score < op.Bounds.max {
				op.docId = ShardIdToExt(result.DocId, workerNum)
				op.score = result.Score
				op.Comms[workerNum] <- op.Bounds
				return true
			} else {
				op.Comms[workerNum] <- op.Bounds
			}
		}
	}
}

func (op *ParallelDocItr) Close() {} // unsure...

func (op *ParallelDocItr) Cur() (int64, float32) {
	return op.docId, op.score
}
