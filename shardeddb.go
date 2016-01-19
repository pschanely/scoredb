package main

import (
	"math"
	"math/rand"
)


type ShardedDb struct {
	Shards  []StreamingDb
}

func (db ShardedDb) BulkIndex(records []map[string]float32) ([]int64, error) {
	//numShards := len(db.Shards)
	shardNum := rand.Intn(len(db.Shards))
	return db.Shards[shardNum].BulkIndex(records)
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
	DocId int64
	Score float32
	WorkerNum int
}

type Bounds struct {
	min, max float32
}

type ParallelDocItr struct {
	score    float32
	docId    int64
	NumAlive int
	Bounds   Bounds
	ResultChannel chan CandidateResult
	Comms    []chan Bounds
}


func RunItr(itr DocItr, myWorkerNum int, resultChannel chan CandidateResult, boundsChannel chan Bounds) {
	bounds := Bounds{min:float32(math.Inf(-1)), max:float32(math.Inf(1))}
	docId := int64(-1)
	for {
		if ! itr.Next(docId + 1) {
			break
		}
		score := itr.Score()
		docId = itr.DocId()
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
		
		newBounds := <- boundsChannel

		if bounds != newBounds {
			bounds = newBounds
			itr.SetBounds(bounds.min, bounds.max)
		}

	}
	itr.Close()
	resultChannel <- CandidateResult{DocId:-1}
}

func NewParallelDocItr(parts []DocItr) *ParallelDocItr {
	op := ParallelDocItr{
		score: 0.0,
		docId: -1,
		NumAlive: len(parts),
		Bounds: Bounds{min:float32(math.Inf(-1)), max:float32(math.Inf(1))},
		ResultChannel: make(chan CandidateResult),
		Comms: make([](chan Bounds), len(parts)),
	}
	for idx, part := range parts {
		part := part
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
		result := <- op.ResultChannel
		if result.DocId == -1 {
			op.NumAlive -= 1
			if op.NumAlive <= 0 {
				return false
			}
		} else {
			if result.Score > op.Bounds.min && result.Score < op.Bounds.max {
				op.docId = result.DocId
				op.score = result.Score
				op.Comms[result.WorkerNum] <- op.Bounds
				return true
			} else {
				op.Comms[result.WorkerNum] <- op.Bounds
			}
		}
	}
}

func (op *ParallelDocItr) Close() {} // unsure...

func (op *ParallelDocItr) DocId() int64 { return op.docId }

func (op *ParallelDocItr) Score() float32 { return op.score }

