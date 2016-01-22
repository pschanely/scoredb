package scoredb

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

func (db *StubDb) Query(query Query) (QueryResult, error) {
	return QueryResult{Ids: []int64{7, 42}}, nil
}

func (db *StubDb) LinearQuery(numResults int, coefs map[string]float32) []int64 {
	return []int64{7, 42}
}
