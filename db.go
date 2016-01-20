package main

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
)

type Query struct {
	Offset int
	Limit  int

	// mixed, nested arrays of strings and numbers describing a function; for example: ["sum", ["field", "age"], ["field", "height"]]
	Scorer []interface{}
}

type DocScore struct {
	DocId int64
	Score float32
}

type QueryResult struct {
	Ids []int64
}


// Three layers of database interfaces, each one wrapping the next:

type Db interface { // Outermost interface; clients use this
	BulkIndex(records []map[string]float32) ([]int64, error)
	Index(record map[string]float32) (int64, error)
	Query(query Query) (QueryResult, error)
}

type StreamingDb interface { // Uses a DocItr based query, useful for middleware that alters or combines result streams
	BulkIndex(records []map[string]float32) ([]int64, error)
	QueryItr(Scorer []interface{}) (DocItr, error)
}

type DbBackend interface { // the minimal interface to implement storage (filesystem, memory, etc)
	BulkIndex(records []map[string]float32) ([]int64, error)
	FieldDocItr(field string) DocItr
}


// BaseDb : The usual way to bridge a Db to a StreamingDb

type BaseDb struct {
	StreamingDb StreamingDb
}

func (db BaseDb) BulkIndex(records []map[string]float32) ([]int64, error) {
	return db.StreamingDb.BulkIndex(records)
}

func (db BaseDb) Index(record map[string]float32) (int64, error) {
	ids, err := db.StreamingDb.BulkIndex([]map[string]float32{record})
	if err == nil {
		return ids[0], nil
	} else {
		return -1, err
	}
}

type BaseDbResultSet []DocScore

func (h BaseDbResultSet) Len() int           { return len(h) }
func (h BaseDbResultSet) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h BaseDbResultSet) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *BaseDbResultSet) Push(x interface{}) {
	*h = append(*h, x.(DocScore))
}
func (h *BaseDbResultSet) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (db BaseDb) Query(query Query) (QueryResult, error) {
	itr, err := db.StreamingDb.QueryItr(query.Scorer)
	if err != nil {
		return QueryResult{}, err
	}
	offset, limit := query.Offset, query.Limit
	if limit == 0 { // we short circuit this case because the code below assumes at least one result
		return QueryResult{Ids: []int64{}}, nil
	}
	numResults := offset + limit
	resultData := make(BaseDbResultSet, 0, numResults+1)
	results := &resultData
	heap.Init(results)
	minScore, maxScore := float32(math.Inf(-1)), float32(math.Inf(1))
	docId := int64(-1)
	var score float32
	for itr.Next(docId + 1) {
		docId, score = itr.Cur()
		if score > minScore {
			heap.Push(results, DocScore{DocId: docId, Score: score})
			if results.Len() > numResults {
				heap.Pop(results)
				minScore = resultData[0].Score
				itr.SetBounds(minScore, maxScore)
			}
		}
	}
	itr.Close()

	for offset > 0 && len(resultData) > 0 {
		heap.Pop(results)
		offset -= 1
	}

	numResults = results.Len()
	var resultIds = make([]int64, numResults)
	for idx, _ := range resultIds {
		resultIds[numResults-(idx+1)] = heap.Pop(results).(DocScore).DocId
	}
	return QueryResult{Ids:resultIds}, nil
}

func ToFloat32(val interface{}) (float32, error) {
	switch typed := val.(type) {
	case float32: 
		return typed, nil
	case float64: 
		return float32(typed), nil
	default:
		return 0.0, errors.New(fmt.Sprintf("Invalid value ('%s') given, must be floating point number", val))
	}
}


// BaseStreamingDb : The usual way to bridge a StreamingDb to a DbBackend

type BaseStreamingDb struct {
	Backend DbBackend
}

func (db BaseStreamingDb) BulkIndex(records []map[string]float32) ([]int64, error) {
	return db.Backend.BulkIndex(records)
}

func (db BaseStreamingDb) QueryItr(scorer []interface{}) (DocItr, error) {
	args := scorer[1:]
	switch scorer[0].(string) {
	case "sum":
		fieldItrs := make([]DocItr, len(args))
		for idx, v := range args {
			itr, err := db.QueryItr(v.([]interface{}))
			if err != nil {
				return nil, err
			}
			fieldItrs[idx] = itr
		}
		return NewSumDocItr(fieldItrs), nil
	case "scale":
		if len(args) != 2 {
			return nil, errors.New("Wrong number of arguments to scale function")
		}
		itr, err := db.QueryItr(args[1].([]interface{}))
		if err != nil {
			return nil, err
		}
		weight := args[0]
		switch typed := weight.(type) {
		case float32: 
			return &ScaleDocItr{typed, itr}, nil
		case float64: 
			return &ScaleDocItr{float32(typed), itr}, nil
		default:
			return nil, errors.New(fmt.Sprintf("Invalid weight ('%s') given to scale function, must be floating point number", weight))
		}
	case "diff":
		if len(args) != 2 {
			return nil, errors.New("Wrong number of arguments to diff function")
		}
		target, err := ToFloat32(args[0])
		if err != nil {
			return nil, err
		}
		itr, err := db.QueryItr(args[1].([]interface{}))
		if err != nil {
			return nil, err
		}
		return &DiffDocItr{
			target: target,
			itr: itr,
		}, nil
	case "pow":
		if len(args) != 2 {
			return nil, errors.New("Wrong number of arguments to pow function")
		}
		exp, err := ToFloat32(args[1])
		if err != nil {
			return nil, err
		}
		itr, err := db.QueryItr(args[0].([]interface{}))
		if err != nil {
			return nil, err
		}
		return &PowDocItr{
			itr: itr,
			exp: exp,
		}, nil
	case "geo_distance":
		if len(args) != 4 {
			return nil, errors.New("Wrong number of arguments to geo_distance function")
		}
		lat, err := ToFloat32(args[0])
		if err != nil {
			return nil, err
		}
		lon, err := ToFloat32(args[1])
		if err != nil {
			return nil, err
		}
		latFieldName := args[2].(string)
		lonFieldName := args[3].(string)
		latItr := &DiffDocItr{target: lat, itr: db.Backend.FieldDocItr(latFieldName)}
		lonItr := &DiffDocItr{target: lon, itr: db.Backend.FieldDocItr(lonFieldName)}
		// bias longitude distances by approximate latitude (matters less at poles)
		multiplier := float32(math.Cos(float64(lat) * math.Pi / 180.0))
		biasedLonItr := &ScaleDocItr{multiplier, lonItr}
		// square each component
		latSquaredItr := &PowDocItr{exp: 2.0, itr: latItr}
		lonSquaredItr := &PowDocItr{exp: 2.0, itr: biasedLonItr}
		// sum and square root
		distanceItr := &PowDocItr{exp: 0.5, itr: NewSumDocItr([]DocItr{latSquaredItr, lonSquaredItr})}
		// multiply distance by radius of earth (in km)
		earthRadius := float32(6371.0)
		return &ScaleDocItr{earthRadius, distanceItr}, nil
	case "field":
		if len(args) != 1 {
			return nil, errors.New("Wrong number of arguments to field function")
		}
		key := args[0].(string)
		return db.Backend.FieldDocItr(key), nil
	default:
		return nil, errors.New(fmt.Sprintf("Scoring function '%s' is not recognized", scorer[0]))
	}
}


