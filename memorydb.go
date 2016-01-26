package scoredb

import (
	"fmt"
	"math"
)

func NewMemoryIdDb() MemoryIdDb {
	return MemoryIdDb{make(map[int64]string)}
}

type MemoryIdDb struct {
	bindings map[int64]string
}

func (db MemoryIdDb) Put(scoreIds []int64, clientIds []string) error {
	for idx, scoreId := range scoreIds {
		db.bindings[scoreId] = clientIds[idx]
	}
	return nil
}

func (db MemoryIdDb) Get(scoreIds []int64) ([]string, error) {
	result := make([]string, len(scoreIds))
	for idx, scoreId := range scoreIds {
		clientId, ok := db.bindings[scoreId]
		if !ok {
			return nil, fmt.Errorf("Unable to find client id for internal id %d", scoreId)

		}
		result[idx] = clientId
	}
	return result, nil
}

type MemoryScoreDb struct {
	Fields map[string][]float32
	nextId int64
}

func NewMemoryScoreDb() *MemoryScoreDb {
	return &MemoryScoreDb{
		Fields: make(map[string][]float32),
		nextId: 1,
	}
}

func (db *MemoryScoreDb) BulkIndex(records []map[string]float32) ([]int64, error) {
	fields := db.Fields
	ids := make([]int64, len(records))
	for idx, record := range records {
		ids[idx] = db.nextId
		db.nextId += 1
		for key, value := range record {
			_, ok := fields[key]
			if !ok {
				fields[key] = make([]float32, 0, 64)
			}
			fields[key] = append(fields[key], value)
		}
	}
	return ids, nil
}

func (db *MemoryScoreDb) FieldDocItr(fieldName string) DocItr {
	scores := db.Fields[fieldName]
	return NewMemoryScoreDocItr(scores)
}

func NewMemoryScoreDocItr(scores []float32) *MemoryScoreDocItr {
	min, max := float32(math.Inf(1)), float32(math.Inf(-1))
	for _, score := range scores {
		if score < min {
			min = score
		}
		if score > max {
			max = score
		}
	}
	return &MemoryScoreDocItr{
		scores: scores,
		idx:    -1,
		min:    min,
		max:    max,
	}
}

type MemoryScoreDocItr struct {
	scores   []float32
	idx      int
	min, max float32
}

func (op *MemoryScoreDocItr) Name() string { return "MemoryScoreDocItr" }
func (op *MemoryScoreDocItr) Cur() (int64, float32) {
	idx := op.idx
	if idx < 0 || idx >= len(op.scores) {
		return -1, 0.0
	}
	return int64(idx + 1), op.scores[idx]

}
func (op *MemoryScoreDocItr) GetBounds() (min, max float32) {
	return op.min, op.max
}
func (op *MemoryScoreDocItr) SetBounds(min, max float32) bool {
	op.min = Max(op.min, min)
	op.max = Min(op.max, max)
	return true
}

func (op *MemoryScoreDocItr) Close() {
}

func (op *MemoryScoreDocItr) Next(minId int64) bool {
	if minId == 0 {
		minId = 1
	}
	op.idx = int(minId - 1)
	return op.idx < len(op.scores)
}
