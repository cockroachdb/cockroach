// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package ts

import (
	"io/ioutil"
	"net/http"

	"github.com/cockroachdb/cockroach/util"
	"github.com/julienschmidt/httprouter"
)

const (
	// URLPrefix is the prefix for all time series endpoints hosted by the
	// server.
	URLPrefix = "/ts/"
	// URLQuery is the relative URL which should accept query requests.
	URLQuery = URLPrefix + "query"
)

// Server handles incoming external requests related to time series data.
type Server struct {
	db     *DB
	router *httprouter.Router
}

// NewServer instantiates a new Server which services requests with data from
// the supplied DB.
func NewServer(db *DB) *Server {
	server := &Server{
		db:     db,
		router: httprouter.New(),
	}

	server.router.POST(URLQuery, server.handleQuery)
	return server
}

// ServeHTTP implements the http.Handler interface.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// handleQuery handles an incoming HTTP query request. Each query requests data
// for one or more metrics over a specific time span. Query requests have a
// significant body and thus are POST requests.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	request := TimeSeriesQueryRequest{}

	// Unmarshal query request.
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := util.UnmarshalRequest(r, reqBody, &request, util.AllEncodings); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(request.Queries) == 0 {
		http.Error(w, "time series query requests must specify at least one query.", http.StatusBadRequest)
		return
	}

	response := TimeSeriesQueryResponse{
		Results: make([]TimeSeriesQueryResponse_Result, 0, len(request.Queries)),
	}
	for _, q := range request.Queries {
		datapoints, sources, err := s.db.Query(q, Resolution10s, request.StartNanos, request.EndNanos)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result := TimeSeriesQueryResponse_Result{
			Query:      q,
			Datapoints: datapoints,
		}
		// TODO(tamird): Remove this (and all other) explicit setting of defaults.
		// It is currently required because the client side doesn't know about
		// proto defaults.
		result.SourceAggregator = q.GetSourceAggregator().Enum()
		result.Downsampler = q.GetDownsampler().Enum()
		result.Derivative = q.GetDerivative().Enum()

		result.Sources = sources
		response.Results = append(response.Results, result)
	}

	// Marshal and return response.
	b, contentType, err := util.MarshalResponse(r, &response, util.AllEncodings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	if _, err := w.Write(b); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
