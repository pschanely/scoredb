package scoredb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
)

type EsScoreDb struct {
	BaseURL, Index string
}

func (db *EsScoreDb) BulkIndex(records []Record) error {
	var jsonbuf bytes.Buffer
	for _, rec := range records {
		jsonbuf.WriteString(fmt.Sprintf("{\"index\":{\"_id\":\"%s\"}}\n", rec.Id))
		buf, err := json.Marshal(rec.Values)
		if err != nil {
			return err
		}
		jsonbuf.Write(buf)
		jsonbuf.WriteString("\n")
	}
	payload := jsonbuf.String()
	url := db.BaseURL + db.Index + "/external/_bulk"
	//fmt.Printf("Bulk: %v @ %v\n", payload, url)
	resp, err := http.Post(url, "application/json", strings.NewReader(payload))
	if err != nil {
		panic(err)
	}
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	//fmt.Printf("Bulk resp: %+v\n", string(body))
	var parsedResponse struct{ Errors bool }
	err = json.Unmarshal(body, &parsedResponse)
	if err != nil {
		panic(err)
	}
	if parsedResponse.Errors {
		panic(string(body))
	}

	db.RefreshIndex()

	return nil
}

type EsQueryResponse struct {
	Hits struct {
		Hits []struct {
			Id string `json:"_id"`
		} `json:"hits"`
	} `json:"hits"`
}

func (db *EsScoreDb) LinearQuery(numResults int, weights map[string]float32) []string {
	var scorefactors bytes.Buffer
	first := true
	for key, val := range weights {
		if !first {
			scorefactors.WriteString(",")
		} else {
			first = false
		}
		scorefactors.WriteString(fmt.Sprintf(`{"field_value_factor":{"field":"%s","factor":%f}}`, key, val))
	}
	data := fmt.Sprintf(`{
    "size":%d,
    "fields":[],
    "query":{
      "function_score":{
        "functions":[%s],
        "score_mode": "sum"
      }
    }
  }`, numResults, scorefactors.String())
	resp, err := http.Post(db.BaseURL+db.Index+"/external/_search?pretty", "application/json", strings.NewReader(data))
	if err != nil {
		panic(err)
	}
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	//fmt.Println(string(body))
	queryResp := EsQueryResponse{}
	err = json.Unmarshal(body, &queryResp)
	if err != nil {
		panic(err)
	}
	hits := queryResp.Hits.Hits
	resultIds := make([]string, len(hits))
	for idx, rec := range hits {
		resultIds[idx] = rec.Id
	}
	return resultIds
}

func (db *EsScoreDb) DeleteIndex() {
	req, _ := http.NewRequest("DELETE", db.BaseURL+db.Index, nil)
	resp, _ := http.DefaultClient.Do(req)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	fmt.Println("Delete Index: " + string(body))
}

func (db *EsScoreDb) CreateIndex() {
	payload := "{\"settings\": {\"index\": {\"number_of_shards\" : 1}}}"
	req, _ := http.NewRequest("PUT", db.BaseURL+db.Index, strings.NewReader(payload))
	resp, _ := http.DefaultClient.Do(req)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	fmt.Println("Create Index: " + string(body))
}

func (db *EsScoreDb) RefreshIndex() {
	req, _ := http.NewRequest("POST", db.BaseURL+db.Index+"/_refresh", nil)
	resp, _ := http.DefaultClient.Do(req)
	ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	//fmt.Println("Refresh Index: " + string(body))
}

func (db *EsScoreDb) ParseQuery(query string) map[string]float32 {
	fields := strings.Split(query, ",")
	coefs := make(map[string]float32)
	for _, f := range fields {
		fieldparts := strings.Split(f, "=")
		if len(fieldparts) != 2 {
			log.Fatalf("ERROR: malformed query\n")
		}
		val, _ := strconv.ParseFloat(fieldparts[1], 32)
		coefs[fieldparts[0]] = float32(val)
	}
	return coefs
}

/*
var (
	deleteflag = flag.Bool("delete", false, "delete data from elasticsearch")
	queryflag  = flag.String("query", "", "column_name=weighting_factor,...")
	urlflag    = flag.String("esurl", "http://localhost:9200/", "URL to elasticsearch instance with trailing slash")
	indexflag  = flag.String("index", "scoredb", "Elasticsearch index name")
)

func main() {
	flag.Parse()
	db := NewEsScoreDb{BaseUrl: *urlflag, Index: *indexflag}
	if *deleteflag {
		db.DeleteData()
	} else if len(*queryflag) > 0 {
		db.LinearQuery(10, db.ParseQuery(*queryflag))
	} else {
		fmt.Println("need to use --query querystring, or --delete")
	}
}
*/
