package scoredb

import (
	"fmt"
	"time"
)

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
	fmt.Printf("Query versus %v at %v", db.Current, time.Now().Unix())
	return db.Current.Query(query)
}
