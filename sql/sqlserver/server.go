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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sqlserver

import (
	"errors"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/sqlwire"
	"github.com/cockroachdb/cockroach/util"
)

var allowedEncodings = []util.EncodingType{util.JSONEncoding, util.ProtoEncoding}

var allMethods = map[string]sqlwire.Method{
	sqlwire.Execute.String(): sqlwire.Execute,
}

// createArgsAndReply returns allocated request and response pairs
// according to the specified method. Note that createArgsAndReply
// only knows about public methods and explicitly returns nil for
// internal methods. Do not change this behavior without also fixing
// Server.ServeHTTP.
func createArgsAndReply(method string) (sqlwire.Request, sqlwire.Response) {
	if m, ok := allMethods[method]; ok {
		switch m {
		case sqlwire.Execute:
			return &sqlwire.SQLRequest{}, &sqlwire.SQLResponse{}
		}
	}
	return nil, nil
}

// A Server provides an HTTP server endpoint serving the SQL API.
// It accepts either JSON or serialized protobuf content types.
type Server struct {
	clientDB *client.DB
}

// NewServer allocates and returns a new Server.
func NewServer(db *client.DB) *Server {
	return &Server{clientDB: db}
}

// ServeHTTP serves the SQL API by treating the request URL path
// as the method, the request body as the arguments, and sets the
// response body as the method reply. The request body is unmarshalled
// into arguments based on the Content-Type request header. Protobuf
// and JSON-encoded requests are supported. The response body is
// encoded according to the request's Accept header, or if not
// present, in the same format as the request's incoming Content-Type
// header.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method := r.URL.Path
	if !strings.HasPrefix(method, sqlwire.Endpoint) {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	method = strings.TrimPrefix(method, sqlwire.Endpoint)
	args, reply := createArgsAndReply(method)
	if args == nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	// Unmarshal the request.
	reqBody, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := util.UnmarshalRequest(r, reqBody, args, allowedEncodings); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Send the SQLRequest for SQL execution.
	if httpStatus, err := s.execute(args, reply); err != nil {
		http.Error(w, err.Error(), httpStatus)
		return
	}

	// Marshal the response.
	body, contentType, err := util.MarshalResponse(r, reply, allowedEncodings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	w.Write(body)
}

func (s *Server) execute(args sqlwire.Request, reply sqlwire.Response) (int, error) {
	return http.StatusNotImplemented, errors.New("Not implemented")
}
