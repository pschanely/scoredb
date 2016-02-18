package scoredb

import (
	"encoding/json"
	"fmt"
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

func QueryIntVal(queryParams url.Values, key string, defaultValue int) (int, error) {
	vals, ok := queryParams[key]
	if !ok || len(vals) == 0 {
		return defaultValue, nil
	}
	return strconv.Atoi(vals[0])
}

func (sds *ScoreDbServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	p := req.URL.Path
	if p[0] == '/' {
		p = p[1:]
	}

	if req.Method == "PUT" {

		b, err := ioutil.ReadAll(req.Body)
		if err != nil {
			http.Error(w, "Could not read request body", 400)
			return
		}
		var records []Record
		if len(p) > 0 {
			var values map[string]float32
			err = json.Unmarshal(b, &values)
			if err == nil {
				records = append(records, Record{Id: p, Values: values})
			}
		} else {
			err = json.Unmarshal(b, &records)
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("Could not parse json: %v", err), 400)
			return
		}
		err = sds.db.BulkIndex(records)
		if err != nil {
			http.Error(w, "Could not index data", 500)
			return
		}

	} else if req.Method == "GET" && len(p) == 0 {

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
		if err != nil {
			fmt.Printf("Internal error. %+v:  %v\n", query, err)
			http.Error(w, "Internal Error in ScoreDB; please report", 500)
			return
		}
		response, err := json.Marshal(results)
		if err != nil {
			fmt.Printf("Internal error. %+v:  %v\n", query, err)
			http.Error(w, "Internal Error in ScoreDB; please report", 500)
			return
		}
		fmt.Fprintf(w, "%s\n", response)

	} else {

		http.NotFound(w, req)
		return

	}
}

func ServeHttp(addr string, db Db) error {
	scoreDbServer := ScoreDbServer{db}
	return http.ListenAndServe(addr, &scoreDbServer)
}
