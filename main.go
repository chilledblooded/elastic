package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"runtime/debug"
	"strings"

	"github.com/elastic/go-elasticsearch"
	"github.com/gorilla/mux"
)

func main() {
	err := http.ListenAndServe(":8888", getMux())
	if err != nil {
		log.Panicln("Error running server")
	}
}
func getMux() *mux.Router {
	r := mux.NewRouter()
	r.Handle("/elastic", RecoveryMid(http.HandlerFunc(elasticSearchHandler))).Methods("POST")
	return r
}

//RecoveryMid function will recover from the panic situation.
//If any fatal error or panic occurs it will recover error.
func RecoveryMid(app http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Println(err)
				stack := debug.Stack()
				log.Println(string(stack))
				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		app.ServeHTTP(w, r)
	}
}

func elasticSearchHandler(w http.ResponseWriter, r *http.Request) {
	var body RequestBody
	var sort, addresses, index []string
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		log.Println("unable to decode request body :: ", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	//this will have the response returned from elastic search
	var elasticResponse map[string]interface{}
	var es *elasticsearch.Client
	if len(body.Addresses) != 0 {
		addresses = stringToArray(body.Addresses)
	}
	if len(body.Sort) != 0 {
		sort = stringToArray(body.Sort)
	}
	if len(body.Index) != 0 {
		index = stringToArray(body.Index)
	}
	if len(body.Username) == 0 && len(body.Password) == 0 && len(body.Addresses) == 0 {
		es, err = elasticsearch.NewDefaultClient()
		if err != nil {
			log.Println("unable to create es client object :: ", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		cfg := elasticsearch.Config{
			Addresses: addresses,
			Username:  body.Username,
			Password:  body.Password,
		}
		es, err = elasticsearch.NewClient(cfg)
		if err != nil {
			log.Println("unable to create es client object :: ", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body.ElasticQuery); err != nil {
		log.Println("Error encoding elastic search query : ", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Perform the search request.
	res, err := es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex(index...),
		es.Search.WithBody(&buf),
		es.Search.WithSort(sort...),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
		es.Search.WithSize(body.Size),
	)
	if err != nil {
		log.Println("Error getting response from elastic search cluster : ", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer res.Body.Close()
	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Printf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
		buf := new(bytes.Buffer)
		buf.ReadFrom(res.Body)
		http.Error(w, buf.String(), http.StatusInternalServerError)
		return
	}
	if err := json.NewDecoder(res.Body).Decode(&elasticResponse); err != nil {
		log.Println("Error parsing the response body of elastic search : ", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	b, err := json.Marshal(elasticResponse)
	if err != nil {
		log.Println("error in json marshaling :: ", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("error in getting data"))
		return
	}
	w.Write(b)
}

//RequestBody is the structure to store body of request
type RequestBody struct {
	Username     string      `json:"username"`
	Password     string      `json:"password"`
	Addresses    string      `json:"addresses"`
	ElasticQuery interface{} `json:"elasticquery"`
	Index        string      `json:"index"`
	Sort         string      `json:"sort"`
	Size         int         `json:"size"`
}

func stringToArray(input string) []string {
	return strings.Split(input, ",")
}
