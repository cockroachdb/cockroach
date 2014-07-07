// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)

package kv

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/storage"
)

const (
	// KVKeyPrefix is the prefix for RESTful endpoints used to
	// interact directly with the key-value datastore.
	KVKeyPrefix = "/db/"
)

// A RESTServer provides a RESTful HTTP API to interact with
// an underlying key-value store.
type RESTServer struct {
	db DB // Key-value database client
}

// NewRESTServer allocates and returns a new server.
func NewRESTServer(db DB) *RESTServer {
	return &RESTServer{db: db}
}

// HandleAction arbitrates requests to the appropriate function
// based on the request’s HTTP method.
func (s *RESTServer) HandleAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		s.handleGetAction(w, r)
	case "PUT", "POST":
		s.handlePutAction(w, r)
	case "DELETE":
		s.handleDeleteAction(w, r)
	default:
		http.Error(w, "Bad Request", http.StatusBadRequest)
	}
}

func dbKey(path string) (storage.Key, error) {
	result, err := url.QueryUnescape(strings.TrimPrefix(path, KVKeyPrefix))
	if err == nil {
		k := storage.Key(result)
		if len(k) == 0 {
			return nil, fmt.Errorf("empty key not allowed")
		}
		return k, nil
	}
	return nil, err
}

func (s *RESTServer) handlePutAction(w http.ResponseWriter, r *http.Request) {
	key, err := dbKey(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	pr := <-s.db.Put(&storage.PutRequest{Key: key, Value: storage.Value{Bytes: b}})
	if pr.Error != nil {
		http.Error(w, pr.Error.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *RESTServer) handleGetAction(w http.ResponseWriter, r *http.Request) {
	key, err := dbKey(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	gr := <-s.db.Get(&storage.GetRequest{Key: key})
	if gr.Error != nil {
		http.Error(w, gr.Error.Error(), http.StatusInternalServerError)
		return
	}
	// An empty key will not be nil, but have zero length.
	if gr.Value.Bytes == nil {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "%s", string(gr.Value.Bytes))
}

func (s *RESTServer) handleDeleteAction(w http.ResponseWriter, r *http.Request) {
	key, err := dbKey(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	dr := <-s.db.Delete(&storage.DeleteRequest{Key: key})
	if dr.Error != nil {
		http.Error(w, dr.Error.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
