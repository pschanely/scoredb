package main

type StubDb struct {
	idx int64
}

func (sdb *StubDb) Index(record map[string]float32) int64 {
	sdb.idx += 1
	return sdb.idx
}

func (sdb *StubDb) BulkIndex(records []map[string]float32) []int64 {
	ids := make([]int64, len(records))
	for i, _ := range records {
		sdb.idx++
		ids[i] = sdb.idx
	}
	return ids
}

func (sdb *StubDb) Query(numResults int, weights map[string]float32) []int64 {
	return []int64{7, 42}
}
