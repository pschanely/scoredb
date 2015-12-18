package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
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
	recordId := sds.db.Index(record)

	fmt.Fprintf(w, "%d\n", recordId)
}

func (sds *ScoreDbServer) ServeQuery(w http.ResponseWriter, req *http.Request) {
	if req.Method != "GET" {
		http.NotFound(w, req)
		return
	}

	// default to limit 10
	n := 10

	record := make(map[string]float32)

	queryParams := req.URL.Query()
	for reqKey, reqVal := range queryParams {
		if strings.HasPrefix(reqKey, "key_") {
			key := strings.TrimPrefix(reqKey, "key_")
			// assume we always have at least one val?
			// otherwise we wouldn't have gotten an iteration?
			firstVal := reqVal[0]
			val64, err := strconv.ParseFloat(firstVal, 32)
			if err == nil {
				val32 := float32(val64)
				record[key] = val32
			}
		} else if reqKey == "n" {
			firstVal := reqVal[0]
			val64, err := strconv.ParseInt(firstVal, 10, 0)
			if err != nil {
				n = int(val64)
			}
		}
	}

	if len(record) == 0 {
		http.Error(w, "No record keys found", 400)
		return
	}

	ids := sds.db.Query(n, record)
	idsSerialized, err := serializeIds(ids)
	if err != nil {
		// this shouldn't happen
		panic(err)
	}
	fmt.Fprintf(w, "%s\n", idsSerialized)
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
			id := sds.db.Index(record)
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
