package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

func RunBenchmark(db Db, csvFilename string) error {
	fp, err := os.Open(csvFilename)
	if err != nil {
		return err
	}
	defer fp.Close()

	bufReader := bufio.NewReader(fp)
	csvReader := csv.NewReader(bufReader)

	header, err := csvReader.Read()
	if err == io.EOF {
		return fmt.Errorf("Missing csv header")
	} else if err != nil {
		return fmt.Errorf("Error reading csv header")
	}

	// TODO ensure we have at least one value?

	colMap := make(map[int]string, len(header))
	for colIdx, colName := range header {
		colMap[colIdx] = colName
	}

	log.Println("Indexing ...")

	recordGroupSize := 100000
	recordGroup := make([]map[string]float32, recordGroupSize)
	curGroupSize := 0

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("Error reading csv contents")
		}
		record := make(map[string]float32, len(row))
		for fieldIdx, fieldValue := range row {
			recordKey, ok := colMap[fieldIdx]
			if !ok {
				// if we don't have header mappings, skip
				break
			}
			val64, err := strconv.ParseFloat(fieldValue, 32)
			if err != nil {
				continue
			}
			val32 := float32(val64)
			record[recordKey] = val32
		}
		if len(record) > 0 {
			// indexing one at a time
			// id := db.Index(record)
			// recordIndexIds = append(recordIndexIds, id)

			recordGroup[curGroupSize] = record
			curGroupSize++
			if curGroupSize == recordGroupSize {
				db.BulkIndex(recordGroup)
				curGroupSize = 0
			}
		}
	}
	if curGroupSize > 0 {
		finalRecords := make([]map[string]float32, curGroupSize)
		copy(finalRecords, recordGroup)
		db.BulkIndex(finalRecords)
	}

	log.Println("Indexing ... done")

	nResults := 10
	log.Println("Querying ...")
	results, err := db.Query(Query{
		Limit: nResults,
		Scorer: []interface{}{
			"+",
			[]interface{}{"field", "age"},
			[]interface{}{"field", "weekly_work_hours"},
		},
	})

	fmt.Printf("Found %d results\n", len(results.Ids))

	log.Println("Querying ... done")

	// the db is indexed now ... need to measure the query times now
	// using some standard queries?

	return nil
}
