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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Andrew Bonventre (andybons@gmail.com)

package structured

import (
	"fmt"
	"net/http"
)

// StructuredKeyPrefix is the prefix for RESTful endpoints used to
// interact with structured data schemas.
const StructuredKeyPrefix = "/structured/"

// A RESTServer provides a RESTful HTTP API to interact with
// structured data schemas.
//
// A resource is represented using the following URL scheme:
//   /structured/<schema key>/<table key>/<primary key>?<name>=<value>
// Some examples:
//   /structured/pdb/us/531 -> <data for user 531>
//   /structured/pdb/us/?email=andybons@gmail.com -> <data for user with email andybons@gmail.com>
//
// Depending on the name/value tuple provided, multiple results may be
// returned. All results are returned as JSON-serialized objects.
//
// TODO(andybons): Provide a limit/continuation field for paging.
// TODO(andybons): Allow the user to provide more than one name/value pair.
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
