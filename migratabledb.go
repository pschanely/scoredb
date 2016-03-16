package scoredb

import (
	"fmt"
)

type MigratableDb struct {
	Current Db
	NextDbs chan Db
}

func (db *MigratableDb) CheckCurrent() {
	select {
	case newDb, ok := <-db.NextDbs:
		if ok {
			db.Current = newDb
		}
	default:
	}
}

func (db *MigratableDb) BulkIndex(records []Record) error {
	db.CheckCurrent()
	return db.Current.BulkIndex(records)
}

func (db *MigratableDb) Index(id string, values map[string]float32) error {
	db.CheckCurrent()
	return db.Current.Index(id, values)
}

func (db *MigratableDb) Query(query Query) (QueryResult, error) {
	db.CheckCurrent()
	return db.Current.Query(query)
}
