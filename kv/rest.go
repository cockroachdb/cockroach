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

package kv

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// RESTPrefix is the prefix for RESTful endpoints used to
	// interact directly with the key-value datastore.
	RESTPrefix = "/kv/"
	// EntryPrefix is the prefix for endpoints that interact with individual key-value pairs directly.
	EntryPrefix = RESTPrefix + "entry/"
	// RangePrefix is the prefix for endpoints that interact with a range of key-value pairs.
	RangePrefix = RESTPrefix + "range"
	// CounterPrefix is the prefix for the endpoint that increments a key by a given amount.
	CounterPrefix = RESTPrefix + "counter/"
)

// Function signture for an HTTP handler that only takes a writer and a request
type actionHandler func(*Server, http.ResponseWriter, *http.Request)

// Function signture for an HTTP handler that takes a writer and a request and a storage key
type actionKeyHandler func(*Server, http.ResponseWriter, *http.Request, proto.Key)

// HTTP methods, defined in RFC 2616.
const (
	methodGet    = "GET"
	methodPut    = "PUT"
	methodPost   = "POST"
	methodDelete = "DELETE"
	methodHead   = "HEAD"
)

// The routingTable maps various path + HTTP method combos to specific
// server methods.
var routingTable = map[string]map[string]actionHandler{
	// TODO(andybons): For Entry and Counter prefixes, return JSON-ified
	// response with option for “raw” value by passing ?raw=true.
	EntryPrefix: {
		methodGet:    keyedAction(EntryPrefix, (*Server).handleGetAction),
		methodPut:    keyedAction(EntryPrefix, (*Server).handlePutAction),
		methodPost:   keyedAction(EntryPrefix, (*Server).handlePutAction),
		methodDelete: keyedAction(EntryPrefix, (*Server).handleDeleteAction),
		methodHead:   keyedAction(EntryPrefix, (*Server).handleHeadAction),
	},
	RangePrefix: {
		// TODO(andybons): HEAD action handler.
		methodGet:    (*Server).handleRangeAction,
		methodDelete: (*Server).handleRangeAction,
	},
	CounterPrefix: {
		methodHead:   keyedAction(CounterPrefix, (*Server).handleHeadAction),
		methodGet:    keyedAction(CounterPrefix, (*Server).handleCounterAction),
		methodPost:   keyedAction(CounterPrefix, (*Server).handleCounterAction),
		methodDelete: keyedAction(CounterPrefix, (*Server).handleDeleteAction),
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

// ServeHTTP satisfies the http.Handler interface and arbitrates requests
// to the appropriate function based on the request’s HTTP method.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
}

// writeJSON marshals v to JSON and writes the result to w with
// the given status code.
func writeJSON(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Errorf("could not json encode response: %v", err)
	}
}

// keyedAction wraps the given actionKeyHandler func in a closure that
// extracts the key from the request and passes it on to the handler.
// The closure is then returned for later execution.
func keyedAction(pathPrefix string, act actionKeyHandler) actionHandler {
	return func(s *Server, w http.ResponseWriter, r *http.Request) {
		key, err := dbKey(r.URL.Path, pathPrefix)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		act(s, w, r, key)
	}
}

func dbKey(path, apiPrefix string) (proto.Key, error) {
	result, err := url.QueryUnescape(strings.TrimPrefix(path, apiPrefix))
	if err == nil {
		k := proto.Key(result)
		if len(k) == 0 {
			return nil, errors.New("empty key not allowed")
		}
		return k, nil
	}
	return nil, err
}

const (
	rangeParamStart = "start"
	rangeParamEnd   = "end"
	rangeParamLimit = "limit"
)

func (s *Server) handleRangeAction(w http.ResponseWriter, r *http.Request) {
	// TODO(andybons): Allow the client to specify range parameters via
	// request headers as well, allowing query parameters to override the
	// range headers if necessary.
	// http://www.restapitutorial.com/media/RESTful_Best_Practices-v1_1.pdf
	startKey := proto.Key(r.FormValue(rangeParamStart))
	endKey := proto.Key(r.FormValue(rangeParamEnd))
	if len(startKey) == 0 {
		http.Error(w, "start key must be non-empty", http.StatusBadRequest)
		return
	}
	if len(endKey) == 0 {
		endKey = startKey
	}
	if endKey.Less(startKey) {
		http.Error(w, "end key must be greater than start key", http.StatusBadRequest)
		return
	}
	// A limit of zero implies no limit.
	limit, err := strconv.ParseInt(r.FormValue(rangeParamLimit), 10, 64)
	if len(r.FormValue(rangeParamLimit)) > 0 && err != nil {
		http.Error(w, "error parsing limit: "+err.Error(), http.StatusBadRequest)
		return
	}
	if limit < 0 {
		http.Error(w, "limit must be non-negative", http.StatusBadRequest)
		return
	}
	reqHeader := proto.RequestHeader{
		Key:    startKey,
		EndKey: endKey,
		User:   storage.UserRoot,
	}
	var results interface{}
	if r.Method == methodGet {
		scanReq := &proto.ScanRequest{RequestHeader: reqHeader}
		if limit > 0 {
			scanReq.MaxResults = limit
		}
		results = <-s.db.Scan(scanReq)
	} else if r.Method == methodDelete {
		deleteReq := &proto.DeleteRangeRequest{RequestHeader: reqHeader}
		if limit > 0 {
			deleteReq.MaxEntriesToDelete = limit
		}
		results = <-s.db.DeleteRange(deleteReq)
	}
	if results == nil {
		panic("results from range operation cannot be nil")
	}
	writeJSON(w, http.StatusOK, results)
}

func (s *Server) handleCounterAction(w http.ResponseWriter, r *http.Request, key proto.Key) {
	// GET Requests are just an increment with 0 value.
	var inputVal int64

	if r.Method == methodPost {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()
		inputVal, err = strconv.ParseInt(string(b), 10, 64)
		if err != nil {
			http.Error(w, "could not parse int64 for increment: "+err.Error(), http.StatusBadRequest)
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
	status := http.StatusOK
	if gr.Error != nil {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, gr)
}

func (s *Server) handlePutAction(w http.ResponseWriter, r *http.Request, key proto.Key) {
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
	status := http.StatusOK
	if pr.Error != nil {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, pr)
}

func (s *Server) handleGetAction(w http.ResponseWriter, r *http.Request, key proto.Key) {
	gr := <-s.db.Get(&proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
	})
	status := http.StatusOK
	if gr.Error != nil {
		status = http.StatusInternalServerError
	}
	// An empty key will not be nil, but have zero length.
	if gr.Value == nil {
		status = http.StatusNotFound
	}
	writeJSON(w, status, gr)
}

func (s *Server) handleHeadAction(w http.ResponseWriter, r *http.Request, key proto.Key) {
	cr := <-s.db.Contains(&proto.ContainsRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
	})
	status := http.StatusOK
	if cr.Error != nil {
		status = http.StatusInternalServerError
	}
	if !cr.Exists {
		status = http.StatusNotFound
	}
	writeJSON(w, status, cr)
}

func (s *Server) handleDeleteAction(w http.ResponseWriter, r *http.Request, key proto.Key) {
	dr := <-s.db.Delete(&proto.DeleteRequest{
		RequestHeader: proto.RequestHeader{
			Key:  key,
			User: storage.UserRoot,
		},
	})
	status := http.StatusOK
	if dr.Error != nil {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, dr)
}
