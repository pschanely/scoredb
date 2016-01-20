package main

import (
	"math"
)

type MemoryScoreDb struct {
	Fields map[string][]float32
	nextId  int64
}

func NewMemoryScoreDb() *MemoryScoreDb {
	return &MemoryScoreDb{
		Fields: make(map[string][]float32),
		nextId: 0,
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
			if ! ok {
				fields[key] = make([]float32, 0, 64)
			}
			fields[key] = append(fields[key], value)
		}
	}
	return ids, nil
}

func (db *MemoryScoreDb) FieldDocItr(fieldName string) DocItr {
	scores := db.Fields[fieldName]
	return &MemoryScoreDocItr{scores:scores, idx:-1}
}

type MemoryScoreDocItr struct {
	scores []float32
	idx int
}

func (op *MemoryScoreDocItr) Name() string { return "MemoryScoreDocItr" }

func (op *MemoryScoreDocItr) DocId() int64 {
	idx := op.idx
	if idx < 0 || idx >= len(op.scores) { return -1 }
	return int64(idx + 1)
}
func (op *MemoryScoreDocItr) Score() float32 {
	idx := op.idx
	if idx < 0 || idx >= len(op.scores) { return 0.0 }
	return op.scores[idx]
}
func (op *MemoryScoreDocItr) GetBounds() (min, max float32) {
	return float32(math.Inf(-1)), float32(math.Inf(1))
}
func (op *MemoryScoreDocItr) SetBounds(min, max float32) bool {
	return true
}

func (op *MemoryScoreDocItr) Close() {
}

func (op *MemoryScoreDocItr) Next(minId int64) bool {
	if (minId == 0) {
		minId = 1
	}
	op.idx = int(minId - 1)
	return op.idx < len(op.scores)
}
