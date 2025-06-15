package main

import (
	"encoding/json"
	"flag"
	"github.com/mysteriousgophers/architecture-lab-4/datastore"
	"github.com/mysteriousgophers/architecture-lab-4/httptools"
	"github.com/mysteriousgophers/architecture-lab-4/signal"
	"io/ioutil"
	"log"
	"net/http"
)

var port = flag.Int("port", 8083, "server port")

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Request struct {
	Value string `json:"value"`
}

func main() {
	h := new(http.ServeMux)
	dir, err := ioutil.TempDir("", "temp-dir")
	if err != nil {
		log.Fatal(err)
	}
	Db, err := datastore.NewDb(dir, 250)
	defer Db.Close()

	h.HandleFunc("/db/", func(rw http.ResponseWriter, req *http.Request) {
		url := req.URL.String()
		key := url[4:]

		switch req.Method {
		case "GET":
			value, err := Db.Get(key)
			if err != nil {
				rw.WriteHeader(http.StatusNotFound)
				return
			}
			rw.WriteHeader(http.StatusOK)
			rw.Header().Set("content-type", "application/json")
			_ = json.NewEncoder(rw).Encode(Response{
				Key:   key,
				Value: value,
			})
		case "POST":
			var body Request

			err := json.NewDecoder(req.Body).Decode(&body)
			if err != nil {
				rw.WriteHeader(http.StatusBadRequest)
			}

			err = Db.Put(key, body.Value)
			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				return
			}
			rw.WriteHeader(http.StatusCreated)
		default:
			rw.WriteHeader(http.StatusBadRequest)
		}
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
