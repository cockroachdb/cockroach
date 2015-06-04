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
	"strconv"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/julienschmidt/httprouter"
)

const (
	// RESTPrefix is the prefix for RESTful endpoints used to
	// interact directly with the key-value datastore.
	RESTPrefix = "/kv/rest/"
	// EntryPrefix is the prefix for endpoints that interact with individual
	// key-value pairs directly.
	EntryPrefix = RESTPrefix + "entry/"
	// EntryPattern is the pattern used to route requests for individual keys.
	EntryPattern = EntryPrefix + ":key"
	// RangePrefix is the prefix for endpoints that interact with a range of key-value pairs.
	RangePrefix = RESTPrefix + "range"
	// CounterPrefix is the prefix for the endpoint that increments a key by a given amount.
	CounterPrefix = RESTPrefix + "counter/"
	// CounterPattern is the pattern used to route requests for incrementing
	// counters.
	CounterPattern = CounterPrefix + ":key"
)

// HTTP methods, defined in RFC 2616.
const (
	methodGet    = "GET"
	methodPut    = "PUT"
	methodPost   = "POST"
	methodDelete = "DELETE"
	methodHead   = "HEAD"
)

// Parameters used for range requests.
const (
	rangeParamStart = "start"
	rangeParamEnd   = "end"
	rangeParamLimit = "limit"
)

// A RESTServer provides a RESTful HTTP API to interact with
// an underlying key-value store.
type RESTServer struct {
	router *httprouter.Router
	db     *client.DB // Key-value database client
}

// NewRESTServer allocates and returns a new server.
func NewRESTServer(db *client.DB) *RESTServer {
	server := &RESTServer{
		router: httprouter.New(),
		db:     db,
	}

	// Empty Keys should return an error messages instead of a 404.
	server.router.GET(EntryPrefix, server.handleEmptyKey)
	server.router.PUT(EntryPrefix, server.handleEmptyKey)
	server.router.POST(EntryPrefix, server.handleEmptyKey)
	server.router.DELETE(EntryPrefix, server.handleEmptyKey)
	server.router.HEAD(EntryPrefix, server.handleEmptyKey)

	server.router.GET(EntryPattern, server.handleGetAction)
	server.router.PUT(EntryPattern, server.handlePutAction)
	server.router.POST(EntryPattern, server.handlePutAction)
	server.router.DELETE(EntryPattern, server.handleDeleteAction)
	server.router.HEAD(EntryPattern, server.handleGetAction)

	server.router.GET(RangePrefix, server.handleRangeAction)
	server.router.DELETE(RangePrefix, server.handleRangeAction)

	// Empty Keys should return an error messages instead of a 404.
	server.router.HEAD(CounterPrefix, server.handleEmptyKey)
	server.router.GET(CounterPrefix, server.handleEmptyKey)
	server.router.POST(CounterPrefix, server.handleEmptyKey)
	server.router.DELETE(CounterPrefix, server.handleEmptyKey)

	server.router.HEAD(CounterPattern, server.handleGetAction)
	server.router.GET(CounterPattern, server.handleCounterAction)
	server.router.POST(CounterPattern, server.handleCounterAction)
	server.router.DELETE(CounterPattern, server.handleDeleteAction)

	return server
}

// ServeHTTP implements the http.Handler interface.
func (s *RESTServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
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

// handleEmptyKey returns a specific error instead of a generic 404 if no key
// is entered.
func (s *RESTServer) handleEmptyKey(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	http.Error(w, errors.New("empty key not allowed").Error(), http.StatusBadRequest)
}

// handleRangeAction deals with all range requests.
func (s *RESTServer) handleRangeAction(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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
	var results proto.Response
	if r.Method == methodGet {
		scanReq := &proto.ScanRequest{RequestHeader: reqHeader}
		if limit > 0 {
			scanReq.MaxResults = limit
		}
		results = &proto.ScanResponse{}
		err = s.db.InternalKV().Run(client.Call{Args: scanReq, Reply: results})
	} else if r.Method == methodDelete {
		deleteReq := &proto.DeleteRangeRequest{RequestHeader: reqHeader}
		if limit > 0 {
			deleteReq.MaxEntriesToDelete = limit
		}
		results = &proto.DeleteRangeResponse{}
		err = s.db.InternalKV().Run(client.Call{Args: deleteReq, Reply: results})
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, results)
}

// handleCounterAction deals with all counter increment requests.
func (s *RESTServer) handleCounterAction(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// GET Requests are just an increment with 0 value.
	key := proto.Key(ps.ByName("key"))
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

	ir := &proto.IncrementResponse{}
	if err := s.db.InternalKV().Run(client.Call{
		Args: &proto.IncrementRequest{
			RequestHeader: proto.RequestHeader{
				Key:  key,
				User: storage.UserRoot,
			},
			Increment: inputVal,
		},
		Reply: ir}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, ir)
}

// handlePutAction deals with all key put requests.
func (s *RESTServer) handlePutAction(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := proto.Key(ps.ByName("key"))
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	pr := &proto.PutResponse{}
	if err := s.db.InternalKV().Run(client.Call{
		Args: &proto.PutRequest{
			RequestHeader: proto.RequestHeader{
				Key:  key,
				User: storage.UserRoot,
			},
			Value: proto.Value{Bytes: b},
		},
		Reply: pr}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, pr)
}

// handleGetAction deals with all key get requests.
func (s *RESTServer) handleGetAction(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := proto.Key(ps.ByName("key"))
	gr := &proto.GetResponse{}
	if err := s.db.InternalKV().Run(client.Call{
		Args: &proto.GetRequest{
			RequestHeader: proto.RequestHeader{
				Key:  key,
				User: storage.UserRoot,
			},
		}, Reply: gr}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// An empty key will not be nil, but have zero length.
	status := http.StatusOK
	if gr.Value == nil {
		status = http.StatusNotFound
	}
	writeJSON(w, status, gr)
}

// handleDeleteAction deals with all key delete requests.
func (s *RESTServer) handleDeleteAction(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	key := proto.Key(ps.ByName("key"))
	dr := &proto.DeleteResponse{}
	if err := s.db.InternalKV().Run(client.Call{
		Args: &proto.DeleteRequest{
			RequestHeader: proto.RequestHeader{
				Key:  key,
				User: storage.UserRoot,
			},
		}, Reply: dr}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, dr)
}
