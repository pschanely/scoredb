package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

type LinearCombinationBackend interface {
	BulkIndex(records []map[string]float32) ([]int64, error)
	LinearQuery(numResults int, coefs map[string]float32) []int64
}

func (db BaseDb) LinearQuery(numResults int, weights map[string]float32) []int64 {
	scorer := make([]interface{}, len(weights)+1)
	scorer[0] = "sum"
	idx := 1
	for key, weight := range weights {
		scorer[idx] = []interface{}{"scale", weight, []interface{}{"field", key}}
		idx += 1
	}
	result, _ := db.Query(Query{
		Limit:  numResults,
		Scorer: scorer,
	})
	return result.Ids
}

func RunBenchmark(db LinearCombinationBackend, csvFilename string, bucketSize int) ([]int64, [][]int64, error) {
	fp, err := os.Open(csvFilename)
	if err != nil {
		return nil, nil, err
	}
	defer fp.Close()

	bufReader := bufio.NewReader(fp)
	csvReader := csv.NewReader(bufReader)

	header, err := csvReader.Read()
	if err == io.EOF {
		return nil, nil, fmt.Errorf("Missing csv header")
	} else if err != nil {
		return nil, nil, fmt.Errorf("Error reading csv header")
	}

	// TODO ensure we have at least one value?

	colMap := make(map[int]string, len(header))
	for colIdx, colName := range header {
		colMap[colIdx] = colName
	}

	indexTimes := []int64{}
	queryTimes := [][]int64{}
	nResults := 10
	weights := []map[string]float32{
		map[string]float32{
			"age":   100.0,
			"wages": 1.0,
		},
		map[string]float32{
			"age":   1000.0,
			"wages": 1.0,
		},
		map[string]float32{
			"age":   10000.0,
			"wages": 1.0,
		},
		map[string]float32{
			"sex":               40.0,
			"weekly_work_hours": 1.0,
		},
		map[string]float32{
			"fertility": 10.0,
			"age":       1.0,
		},
		map[string]float32{
			"fertility":         5.0,
			"age":               1.0,
			"weekly_work_hours": 1.0,
		},
		map[string]float32{
			"sex":               20.0,
			"fertility":         5.0,
			"age":               1.0,
			"weekly_work_hours": 1.0,
		},
	}

	recordGroup := make([]map[string]float32, bucketSize)
	curGroupSize := 0

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, fmt.Errorf("Error reading csv contents")
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
			if curGroupSize == bucketSize {
				t0 := time.Now().UnixNano()
				db.BulkIndex(recordGroup)
				indexTimes = append(indexTimes, time.Now().UnixNano() - t0)
				queryRoundTimes := make([]int64, len(weights))
				
				for idx, query := range weights {
					fmt.Printf("%08d Q start\n", time.Now().UnixNano() % 100000000)
					t0 := time.Now().UnixNano()
					results := db.LinearQuery(nResults, query)
					queryTime := time.Now().UnixNano() - t0
					fmt.Printf("%08d Q results: %v\n", time.Now().UnixNano() % 100000000, results)
					queryRoundTimes[idx] = queryTime
				}
				curGroupSize = 0
				queryTimes = append(queryTimes, queryRoundTimes)
			}
		}
	}
	if curGroupSize > 0 {
		finalRecords := make([]map[string]float32, curGroupSize)
		copy(finalRecords, recordGroup)
		db.BulkIndex(finalRecords)
	}

	return indexTimes, queryTimes, nil
}
