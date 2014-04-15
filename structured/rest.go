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

package structured

import (
	"fmt"
	"net/http"
)

const (
	// StructuredKeyPrefix is the prefix for RESTful endpoints used to
	// interact with structured data schemas.
	StructuredKeyPrefix = "/structured/"
)

type RESTServer struct {
	db *DB // Structured database client
}

func NewRESTServer(db *DB) *RESTServer {
	return &RESTServer{db: db}
}

func (s *RESTServer) HandleAction(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		//s.handleGetAction(w, r)
	case "PUT", "POST":
		//s.handlePutAction(w, r)
	case "DELETE":
		//s.handleDeleteAction(w, r)
	default:
		errStr := fmt.Sprintf("unhandled HTTP method %s: %s", r.Method, http.StatusText(http.StatusBadRequest))
		http.Error(w, errStr, http.StatusBadRequest)
	}
}
