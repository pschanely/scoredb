package scoredb

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
)

type Query struct {
	Offset int
	Limit  int
	MinScore float32

	// mixed, nested arrays of strings and numbers describing a function; for example: ["sum", ["field", "age"], ["field", "height"]]
	Scorer []interface{}
}

type DocScore struct {
	DocId int64
	Score float32
}

type Record struct {
	Id     string
	Values map[string]float32
}

type QueryResult struct {
	Ids []string
	Scores []float32
}

// Three layers of database interfaces, each one wrapping the next:

type Db interface { // Outermost interface; clients use this
	BulkIndex(records []Record) error
	Index(id string, values map[string]float32) error
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

type IdBackend interface { // stores a mapping from scoredb's identifiers to the clients'
	Put(scoreIds []int64, clientIds []string) error
	Get(scoreIds []int64) ([]string, error)
}

type BaseDb struct {
	StreamingDb StreamingDb
	IdDb        IdBackend
}

func (db BaseDb) BulkIndex(records []Record) error {
	clientIds := make([]string, len(records))
	values := make([]map[string]float32, len(records))
	for idx, rec := range records {
		values[idx] = rec.Values
		clientIds[idx] = rec.Id
	}
	scoreIds, err := db.StreamingDb.BulkIndex(values)
	if err != nil {
		return err
	}
	return db.IdDb.Put(scoreIds, clientIds)
}

func (db BaseDb) Index(id string, values map[string]float32) error {
	return db.BulkIndex([]Record{Record{Id: id, Values: values}})
}

func CandidateIsLess(r1, r2 DocScore) bool {
	s1, s2 := r1.Score, r2.Score
	if s1 < s2 {
		return true
	} else if s1 > s2 {
		return false
	} else {
		return r1.DocId < r2.DocId
	}
}

type BaseDbResultSet []DocScore

func (h BaseDbResultSet) Len() int           { return len(h) }
func (h BaseDbResultSet) Less(i, j int) bool { return CandidateIsLess(h[i], h[j]) }
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
	minScore, offset, limit := query.MinScore, query.Offset, query.Limit
	if limit == 0 { // we short circuit this case because the code below assumes at least one result
		return QueryResult{Ids: []string{}}, nil
	}
	//fmt.Printf("> %+v\n", query);
	numResults := offset + limit
	resultData := make(BaseDbResultSet, 0, numResults+1)
	results := &resultData
	heap.Init(results)
	minCandidate := DocScore{Score: float32(math.Inf(-1))}
	maxScore := float32(math.Inf(1))
	docId := int64(-1)
	var score float32
	for itr.Next(docId + 1) {
		docId, score = itr.Cur()
		if score < minScore {
			continue
		}
		candidate := DocScore{DocId:docId, Score: score}
		if CandidateIsLess(minCandidate, candidate) {
			heap.Push(results, candidate)
			if results.Len() > numResults {
				heap.Pop(results)
				minCandidate = resultData[0]
				itr.SetBounds(minCandidate.Score, maxScore)
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
	var resultScores = make([]float32, numResults)
	for idx, _ := range resultIds {
		rec := heap.Pop(results).(DocScore)
		i := numResults-(idx+1)
		resultIds[i] = rec.DocId
		resultScores[i] = rec.Score
	}
	//fmt.Printf("< %+v\n", resultIds);
	//fmt.Printf("< %+v\n", resultScores);

	clientIds, err := db.IdDb.Get(resultIds)
	if err != nil {
		return QueryResult{}, err
	}
	return QueryResult{Ids: clientIds, Scores: resultScores}, nil
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
	case "product":
		fieldItrs := make([]DocItr, len(args))
		for idx, v := range args {
			itr, err := db.QueryItr(v.([]interface{}))
			if err != nil {
				return nil, err
			}
			fieldItrs[idx] = itr
		}
		return NewProductDocItr(fieldItrs), nil
	case "min":
		fieldItrs := make([]DocItr, len(args))
		for idx, v := range args {
			itr, err := db.QueryItr(v.([]interface{}))
			if err != nil {
				return nil, err
			}
			fieldItrs[idx] = itr
		}
		return NewMinDocItr(fieldItrs), nil
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
			itr:    itr,
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

	case "custom_linear":
		if len(args) != 2 {
			return nil, errors.New("Wrong number of arguments to custom_linear function")
		}

		inputPoints := args[0].([]interface{})
		points := make([]CustomPoint, len(inputPoints))
		for idx, inputPoint := range inputPoints {
			pair := inputPoint.([]interface{})
			if len(pair) != 2 {
				return nil, fmt.Errorf("Invalid (x,y) point in custom_linear; found: '%v' instead", pair)
			}
			xPoint, err := ToFloat32(pair[0])
			if err != nil {
				return nil, err
			}
			yPoint, err := ToFloat32(pair[1])
			if err != nil {
				return nil, err
			}
			points[idx] = CustomPoint{xPoint, yPoint}
		}

		itr, err := db.QueryItr(args[1].([]interface{}))
		if err != nil {
			return nil, err
		}

		return &CustomLinearDocItr{
			points: points,
			docItr: itr,
		}, nil

	case "geo_distance":
		if len(args) != 4 {
			return nil, errors.New("Wrong number of arguments to geo_distance function")
		}
		lat, err := ToFloat32(args[0])
		if err != nil {
			return nil, err
		}
		lng, err := ToFloat32(args[1])
		if err != nil {
			return nil, err
		}
		latFieldName := args[2].(string)
		lngFieldName := args[3].(string)
		latItr := &DiffDocItr{target: lat, itr: db.Backend.FieldDocItr(latFieldName)}
		lngItr := &DiffDocItr{target: lng, itr: db.Backend.FieldDocItr(lngFieldName)}
		// bias longitude distances by approximate latitude (matters less at poles)
		multiplier := float32(math.Cos(float64(lat) * math.Pi / 180.0))
		biasedLngItr := &ScaleDocItr{multiplier, lngItr}
		// square each component
		latSquaredItr := NewPowDocItr(latItr, 2.0)
		lngSquaredItr := NewPowDocItr(biasedLngItr, 2.0)
		// sum and square root
		distanceItr := NewPowDocItr(NewSumDocItr([]DocItr{latSquaredItr, lngSquaredItr}), 0.5)
		// convert degrees distance to radians and multiply by radius of the earth (in km)
		earthRadius := float32(6371.0 * math.Pi / 180.0)
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
