package scoredb

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
)

type ScoreDbServer struct {
	db Db
}

func serializeIds(ids []int64) (string, error) {
	b, err := json.Marshal(ids)
	if err != nil {
		return "", err
	}
	s := string(b)
	return s, nil
}

func (sds *ScoreDbServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// simple dispatch
	p := req.URL.Path
	if p == "/index" {
		sds.ServeIndex(w, req)
	} else if p == "/query" {
		sds.ServeQuery(w, req)
	} else if p == "/csvindex" {
		sds.ServeCsvIndex(w, req)
	} else {
		http.NotFound(w, req)
	}
}

func (sds *ScoreDbServer) ServeIndex(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.NotFound(w, req)
		return
	}
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "Could not read request body", 400)
		return
	}
	var record map[string]float32
	err = json.Unmarshal(b, &record)
	if err != nil {
		http.Error(w, "Could not parse json", 400)
		return
	}
	recordId, err := sds.db.Index(record)
	if err != nil {
		http.Error(w, "Could not index data", 500)
		return
	}

	fmt.Fprintf(w, "%d\n", recordId)
}

func QueryIntVal(queryParams url.Values, key string, defaultValue int) (int, error) {
	vals, ok := queryParams[key]
	if !ok || len(vals) == 0 {
		return defaultValue, nil
	}
	return strconv.Atoi(vals[0])
}

func (sds *ScoreDbServer) ServeQuery(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		http.NotFound(w, req)
		return
	}

	queryParams := req.URL.Query()

	offset, err := QueryIntVal(queryParams, "offset", 0)
	if err != nil {
		http.Error(w, "Invalid value for offset", 400)
		return
	}

	limit, err := QueryIntVal(queryParams, "limit", 10)
	if err != nil {
		http.Error(w, "Invalid value for limit", 400)
		return
	}

	scorerStrings, ok := queryParams["score"]
	if !ok || len(scorerStrings) == 0 {
		http.Error(w, "No score function was specified", 400)
		return
	}
	scorer := new([]interface{})
	err = json.Unmarshal([]byte(scorerStrings[0]), scorer)
	if err != nil {
		http.Error(w, "Score parameter is not valid JSON", 400)
		return
	}

	query := Query{
		Offset: offset,
		Limit:  limit,
		Scorer: *scorer,
	}

	results, err := sds.db.Query(query)
	response, err := json.Marshal(results)
	if err != nil {
		http.Error(w, "Internal Error in ScoreDB; please report", 500)
		return
	}
	fmt.Fprintf(w, "%s\n", response)
}

func (sds *ScoreDbServer) ServeCsvIndex(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.NotFound(w, req)
		return
	}

	bufReader := bufio.NewReader(req.Body)
	csvReader := csv.NewReader(bufReader)

	header, err := csvReader.Read()
	if err == io.EOF {
		http.Error(w, "no header line", 400)
		return
	} else if err != nil {
		http.Error(w, "Error reading request body", 400)
		return
	}

	// TODO ensure we have at least one value?

	colMap := make(map[int]string, len(header))
	for colIdx, colName := range header {
		colMap[colIdx] = colName
	}

	ids := make([]int64, 0)

	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			http.Error(w, "Error reading request body", 400)
			return
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
			id, err := sds.db.Index(record)
			if err != nil {
				http.Error(w, "Error indexing", 500)
				return
			}
			ids = append(ids, id)
		}
	}

	idsSerialized, err := serializeIds(ids)
	if err != nil {
		// this shouldn't happen
		panic(err)
	}
	fmt.Fprintf(w, "%s\n", idsSerialized)
}

func ServeHttp(addr string, db Db) error {
	scoreDbServer := ScoreDbServer{db}
	return http.ListenAndServe(addr, &scoreDbServer)
}
