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
)

// StructuredKeyPrefix is the prefix for RESTful endpoints used to
// interact with structured data schemas.
const StructuredKeyPrefix = "/schema/"

// A RESTServer provides a RESTful HTTP API to interact with
// structured data schemas.
//
// A resource is represented using the following URL scheme:
//   /schema/<schema key>/<table key>/<primary key>??[<name>=<value>,<name>=<value>...][limit=<int>][offset=<int>]
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
