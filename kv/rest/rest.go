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
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)

package rest

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
)

const (
	// APIPrefix is the prefix for RESTful endpoints used to
	// interact directly with the key-value datastore.
	APIPrefix = "/kv/"
	// EntryPrefix is the prefix for endpoints that interact with individual key-value pairs directly.
	EntryPrefix = APIPrefix + "entry/"
	// RangePrefix is the prefix for endpoints that interact with a range of key-value pairs.
	RangePrefix = APIPrefix + "range/"
	// CounterPrefix is the prefix for the endpoint that increments a key by a given amount.
	CounterPrefix = APIPrefix + "counter/"
)

// Function signture for an HTTP handler that only takes a writer and a request
type actionHandler func(*Server, http.ResponseWriter, *http.Request)

// Function signture for an HTTP handler that takes a writer and a request and a storage key
type actionKeyHandler func(*Server, http.ResponseWriter, *http.Request, engine.Key)

// Maps various path + HTTP method combos to specific server methods
var routingTable = map[string]map[string]actionHandler{
	EntryPrefix: {
		"GET":    makeActionWithKey((*Server).handleEntryGetAction),
		"PUT":    makeActionWithKey((*Server).handleEntryPutAction),
		"POST":   makeActionWithKey((*Server).handleEntryPutAction),
		"DELETE": makeActionWithKey((*Server).handleEntryDeleteAction),
		"HEAD":   makeActionWithKey((*Server).handleEntryHeadAction),
	},
	RangePrefix: {
	// TODO(zbrock + matthew) not supported yet!
	},
	CounterPrefix: {
		"GET":  (*Server).handleIncrementAction,
		"POST": (*Server).handleIncrementAction,
	},
}

// A Server provides a RESTful HTTP API to interact with
// an underlying key-value store.
type Server struct {
	db storage.DB // Key-value database client
}

// NewRESTServer allocates and returns a new server.
func NewRESTServer(db storage.DB) *Server {
	return &Server{db: db}
}

// HandleAction arbitrates requests to the appropriate function
// based on the request’s HTTP method.
func (s *Server) HandleAction(w http.ResponseWriter, r *http.Request) {
	for endPoint, epRoutes := range routingTable {
		if strings.HasPrefix(r.URL.Path, endPoint) {
			epHandler := epRoutes[r.Method]
			if epHandler == nil {
				http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
				return
			}
			epHandler(s, w, r)
			return
		}
	}
	http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	return
}

func makeActionWithKey(act actionKeyHandler) actionHandler {
	return func(s *Server, w http.ResponseWriter, r *http.Request) {
		key, err := dbKey(r.URL.Path, EntryPrefix)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		act(s, w, r, key)
	}
}

func dbKey(path, apiPrefix string) (engine.Key, error) {
	result, err := url.QueryUnescape(strings.TrimPrefix(path, apiPrefix))
	if err == nil {
		k := engine.Key(result)
		if len(k) == 0 {
			return nil, errors.New("empty key not allowed")
		}
		return k, nil
	}
	return nil, err
}

func (s *Server) handleIncrementAction(w http.ResponseWriter, r *http.Request) {
	key, err := dbKey(r.URL.Path, CounterPrefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// GET Requests are just an increment with 0 value.
	var inputVal int64

	if r.Method == "POST" {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()
		inputVal, err = strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			http.Error(w, "Could not parse int64 for increment", http.StatusBadRequest)
			return
		}
	}

	gr := <-s.db.Increment(&proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
		Increment: inputVal,
	})
	if gr.Error != nil {
		http.Error(w, gr.GoError().Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "%d", gr.NewValue)
}

func (s *Server) handleEntryPutAction(w http.ResponseWriter, r *http.Request, key engine.Key) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	pr := <-s.db.Put(&proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
		Value: proto.Value{Bytes: b},
	})
	if pr.Error != nil {
		http.Error(w, pr.GoError().Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleEntryGetAction(w http.ResponseWriter, r *http.Request, key engine.Key) {
	gr := <-s.db.Get(&proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
	})
	if gr.Error != nil {
		http.Error(w, gr.GoError().Error(), http.StatusInternalServerError)
		return
	}
	// An empty key will not be nil, but have zero length.
	if gr.Value.Bytes == nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	fmt.Fprintf(w, "%s", string(gr.Value.Bytes))
}

func (s *Server) handleEntryHeadAction(w http.ResponseWriter, r *http.Request, key engine.Key) {
	cr := <-s.db.Contains(&proto.ContainsRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
	})
	if cr.Error != nil {
		http.Error(w, cr.GoError().Error(), http.StatusInternalServerError)
		return
	}
	if !cr.Exists {
		http.Error(w, "", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleEntryDeleteAction(w http.ResponseWriter, r *http.Request, key engine.Key) {
	dr := <-s.db.Delete(&proto.DeleteRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
	})
	if dr.Error != nil {
		http.Error(w, dr.GoError().Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
