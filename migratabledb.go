package scoredb

import ()

type MigratableDb struct {
	Current Db
}

func (db *MigratableDb) BulkIndex(records []Record) error {
	return db.Current.BulkIndex(records)
}

func (db *MigratableDb) Index(id string, values map[string]float32) error {
	return db.Current.Index(id, values)
}

func (db *MigratableDb) Query(query Query) (QueryResult, error) {
	return db.Current.Query(query)
}
