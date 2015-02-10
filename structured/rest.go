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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/util/log"
)

// StructuredKeyPrefix is the prefix for RESTful endpoints used to
// interact with structured data schemas.
const StructuredKeyPrefix = "/schema"

// A RESTServer provides a RESTful HTTP API to interact with
// structured data schemas.
//
// A resource is represented using the following URL scheme:
//   /schema[/<schema key>][/<table key>][/<primary key>][?<name>=<value>,<name>=<value>...][limit=<int>][offset=<int>]
//
// Some examples:
//   /schema/pdb/us/531 -> <data for user 531>
//   /schema/pdb/us/?email=andybons@gmail.com -> <data for user with email andybons@gmail.com>
//
// A user can provide just the top-level schema key in order to introspect the schema layout:
//   /schema/pdb -> <schema with key pdb>
//
// Or simply provide /schema to list all schemas within the datastore.
//
// Results are always returned within a JSON-serialized array (even for only one result)
// to provide uniformity for responses and to accommodate for multiple entries that may
// satisfy a query. If the limit param is not provided, a maximum of 50 items is returned.
// TODO(andybons): Paging?
type RESTServer struct {
	db DB // Structured database client
}

// NewRESTServer allocates and returns a new server.
func NewRESTServer(db DB) *RESTServer {
	return &RESTServer{db: db}
}

const (
	componentSchemaKey = iota
	componentTableKey
	componentPrimaryKey

	paramLimit  = "limit"
	paramOffset = "offset"
)

// TODO(andybons): need to account for other fields like secondary index
// keys, full-text indexes, geo-spatial indexes, etc.
type resourceRequest struct {
	schemaKey, tableKey, primaryKey string
	limit, offset                   int
	params                          map[string][]string
}

// pathSpec is only used in error messages to users of the
// REST API in the event that an invalid path is specified.
const pathSpec = "/schema[/<schema key>][/<table key>][/<primary key>]?[<name>=<value>,<name>=<value>...][limit=<int>][offset=<int>]"

// newResourceRequest allocates and returns a resourceRequest
// by parsing the HTTP request path and parameters.
func newResourceRequest(req *http.Request) (*resourceRequest, error) {
	path := req.URL.Path
	if strings.HasPrefix(path, StructuredKeyPrefix) {
		path = path[len(StructuredKeyPrefix):]
		if len(path) > 0 && path[0] == '/' {
			path = path[1:]
		}
	} else {
		return nil, fmt.Errorf("incorrect specification of path; %s: %q", pathSpec, path)
	}
	components := strings.Split(path, "/")
	if len(components) > 3 {
		return nil, fmt.Errorf("incorrect specification of path; %s: %q", pathSpec, req.URL.Path)
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
		param := resReq.params[paramLimit][0]
		if resReq.limit, err = strconv.Atoi(param); err != nil {
			return nil, fmt.Errorf("error parsing limit param %q: %v", param, err)
		}
		delete(resReq.params, paramLimit)
	}
	if _, ok := resReq.params[paramOffset]; ok {
		var err error
		param := resReq.params[paramOffset][0]
		if resReq.offset, err = strconv.Atoi(param); err != nil {
			return nil, fmt.Errorf("error parsing offset param %q: %v", param, err)
		}
		delete(resReq.params, paramOffset)
	}
	if len(resReq.params) == 0 {
		resReq.params = nil
	}
	return resReq, nil
}

// getResource returns the results from querying the given DB
// for the desired resourceRequest. If no results are found,
// a nil error is returned.
func (r *resourceRequest) getResource(db DB) ([]interface{}, error) {
	// TODO(andybons): for now, only schemas are supported.
	// TODO(andybons): return a list of schemas in the case
	// of an empty resourceRequest.
	schema, err := db.GetSchema(r.schemaKey)
	if err != nil {
		return nil, err
	}
	results := []interface{}{}
	if schema != nil {
		results = append(results, schema)
	}
	return results, nil
}

func (r *resourceRequest) putResource(db DB, v interface{}) error {
	switch t := v.(type) {
	case *Schema:
		return db.PutSchema(t)
	default:
		return fmt.Errorf("type %T not supported", t)
	}
}

func (r *resourceRequest) deleteResource(db DB, v interface{}) error {
	switch t := v.(type) {
	case *Schema:
		return db.DeleteSchema(t)
	default:
		return fmt.Errorf("type %T not supported", t)
	}
}

type resourceResponse struct {
	Meta struct {
		StatusCode int    `json:"status_code"`
		Error      string `json:"error,omitempty"`
	} `json:"meta"`
	Data []interface{} `json:"data"`
}

func newResourceResponse(statusCode int, data []interface{}, err error) *resourceResponse {
	r := &resourceResponse{Data: data}
	r.Meta.StatusCode = statusCode
	if err != nil {
		r.Meta.Error = err.Error()
	}
	return r
}

const (
	methodGet    = "GET"
	methodPut    = "PUT"
	methodPost   = "POST"
	methodDelete = "DELETE"
)

var supportedMethods = map[string]struct{}{
	methodGet:    {},
	methodPut:    {},
	methodPost:   {},
	methodDelete: {},
}

// ServeHTTP arbitrates requests to the appropriate function
// based on the request’s HTTP method.
func (s *RESTServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, supported := supportedMethods[r.Method]; !supported {
		errStr := fmt.Sprintf("unhandled HTTP method %s: %s", r.Method, http.StatusText(http.StatusBadRequest))
		http.Error(w, errStr, http.StatusBadRequest)
		return
	}
	resReq, err := newResourceRequest(r)
	if err != nil {
		writeResourceResponse(w, http.StatusBadRequest, nil, err)
		return
	}
	var results []interface{}
	switch r.Method {
	case methodGet:
		results, err = resReq.getResource(s.db)
		if len(results) == 0 {
			writeResourceResponse(w, http.StatusNotFound, nil, nil)
			return
		}
	case methodPut, methodPost:
		var sch Schema
		if err := json.NewDecoder(r.Body).Decode(&sch); err != nil {
			writeResourceResponse(w, http.StatusInternalServerError, nil, err)
			return
		}
		err = resReq.putResource(s.db, &sch)
	case methodDelete:
		err = resReq.deleteResource(s.db, &Schema{Key: resReq.schemaKey})
	}
	if err != nil {
		writeResourceResponse(w, http.StatusInternalServerError, nil, err)
		return
	}
	writeResourceResponse(w, http.StatusOK, results, nil)
}

func writeResourceResponse(w http.ResponseWriter, statusCode int, data []interface{}, err error) {
	resp := newResourceResponse(statusCode, data, err)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(resp.Meta.StatusCode)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Warningf("unable to encode response: %v", err)
		return
	}
}
