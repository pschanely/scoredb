package main

type StubDb struct {
	idx int64
}

func (sdb *StubDb) Index(record map[string]float32) (int64, error) {
	sdb.idx += 1
	return sdb.idx, nil
}

func (sdb *StubDb) BulkIndex(records []map[string]float32) ([]int64, error) {
	ids := make([]int64, len(records))
	for i, _ := range records {
		sdb.idx++
		ids[i] = sdb.idx
	}
	return ids, nil
}

func (sdb *StubDb) Query(numResults int, weights map[string]float32) ([]int64, error) {
	return []int64{7, 42}, nil
}
