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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Andrew Bonventre (andybons@gmail.com)

package structured

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/storage/engine"
)

// StructuredKeyPrefix is the prefix for RESTful endpoints used to
// interact with structured data schemas.
const StructuredKeyPrefix = "/schema/"

// A RESTServer provides a RESTful HTTP API to interact with
// structured data schemas.
//
// A resource is represented using the following URL scheme:
//   /schema/<schema key>/<table key>/<primary key>?[<name>=<value>,<name>=<value>...][limit=<int>][offset=<int>]
//
// Some examples:
//   /schema/pdb/us/531 -> <data for user 531>
//   /schema/pdb/us/?email=andybons@gmail.com -> <data for user with email andybons@gmail.com>
//
// A user can provide just the top-level schema key in order to introspect the schema layout:
//   /schema/pdb -> <schema with key pdb>
//
// Results are always returned within a JSON-serialized array (even for only one result)
// to provide uniformity for responses and to accommodate for multiple entries that may
// satisfy a query. If the limit param is not provided, a maximum of 50 items is returned.
type RESTServer struct {
	db *DB // Structured database client
}

// NewRESTServer allocates and returns a new server.
func NewRESTServer(db *DB) *RESTServer {
	return &RESTServer{db: db}
}

const (
	componentSchemaKey = iota
	componentTableKey
	componentPrimaryKey

	paramLimit  = "limit"
	paramOffset = "offset"
)

type resourceRequest struct {
	schemaKey, tableKey, primaryKey string
	limit, offset                   int
	params                          map[string][]string
}

// key returns the Key value corresponding to the resource request
// that can be used to look up values within the monolithic kv store.
// If a valid key cannot be constructed, an error is returned.
func (r *resourceRequest) key() (engine.Key, error) {
	return engine.Key{}, nil
}

// newResourceRequest allocates and returns a resourceRequest
// with the available information within req.
func newResourceRequest(req *http.Request) (*resourceRequest, error) {
	path := req.URL.Path
	if strings.HasPrefix(path, StructuredKeyPrefix) {
		path = path[len(StructuredKeyPrefix):]
	} else {
		return nil, fmt.Errorf("invalid path: %q", path)
	}
	components := strings.Split(path, "/")
	if len(components) == 0 || len(components) > 3 {
		return nil, fmt.Errorf("invalid path: %q", req.URL.Path)
	}
	if err := req.ParseForm(); err != nil {
		return nil, fmt.Errorf("error parsing form values: %v", err)
	}
	resReq := &resourceRequest{params: req.Form}
	for i := 0; i < len(components); i++ {
		switch i {
		case componentSchemaKey:
			resReq.schemaKey = components[i]
		case componentTableKey:
			resReq.tableKey = components[i]
		case componentPrimaryKey:
			resReq.primaryKey = components[i]
		}
	}
	// While these could be deduced via the params field downstream,
	// they are parsed into their own fields here as a convenience.
	if _, ok := resReq.params[paramLimit]; ok {
		var err error
		if resReq.limit, err = strconv.Atoi(resReq.params[paramLimit][0]); err != nil {
			return nil, fmt.Errorf("error parsing limit param: %v", err)
		}
		delete(resReq.params, paramLimit)
	}
	if _, ok := resReq.params[paramOffset]; ok {
		var err error
		if resReq.offset, err = strconv.Atoi(resReq.params[paramOffset][0]); err != nil {
			return nil, fmt.Errorf("error parsing offset param: %v", err)
		}
		delete(resReq.params, paramOffset)
	}
	if len(resReq.params) == 0 {
		resReq.params = nil
	}
	return resReq, nil
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
		errStr := fmt.Sprintf("unhandled HTTP method %s: %s", r.Method, http.StatusText(http.StatusBadRequest))
		http.Error(w, errStr, http.StatusBadRequest)
	}
}

func (s *RESTServer) handleGetAction(w http.ResponseWriter, r *http.Request) {
	panic("not implemented")
}

func (s *RESTServer) handlePutAction(w http.ResponseWriter, r *http.Request) {
	panic("not implemented")
}

func (s *RESTServer) handleDeleteAction(w http.ResponseWriter, r *http.Request) {
	panic("not implemented")
}
