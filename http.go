package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type ScoreDbServer struct {
	db Db
}

func (sds *ScoreDbServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// simple dispatch
	p := req.URL.Path
	if p == "/index" {
		sds.ServeIndex(w, req)
	} else if p == "/query" {
		sds.ServeQuery(w, req)
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
	b, err := json.Marshal(ids)
	if err != nil {
		// this shouldn't happen
		panic(err)
	}
	s := string(b)
	fmt.Fprintf(w, "%s\n", s)
}

func ServeHttp(addr string, db Db) error {
	scoreDbServer := ScoreDbServer{db}
	return http.ListenAndServe(addr, &scoreDbServer)
}
