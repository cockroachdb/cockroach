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

package kv

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
)

const (
	// DBPrefix is the prefix for the key-value database endpoint used
	// to interact with the key-value datastore via HTTP RPC.
	DBPrefix = client.KVDBEndpoint
)

// A DBServer provides an HTTP server endpoint serving the key-value API.
// It accepts either JSON or serialized protobuf content types.
type DBServer struct {
	sender client.KVSender
}

// NewDBServer allocates and returns a new DBServer.
func NewDBServer(sender client.KVSender) *DBServer {
	return &DBServer{sender: sender}
}

// createArgsAndReply returns allocated request and response pairs
// according to the specified method. Note that only public methods
// are handled by the public HTTP server.
func createArgsAndReply(method string) (proto.Request, proto.Response, error) {
	switch method {
	case proto.Contains:
		return &proto.ContainsRequest{}, &proto.ContainsResponse{}, nil
	case proto.Get:
		return &proto.GetRequest{}, &proto.GetResponse{}, nil
	case proto.Put:
		return &proto.PutRequest{}, &proto.PutResponse{}, nil
	case proto.ConditionalPut:
		return &proto.ConditionalPutRequest{}, &proto.ConditionalPutResponse{}, nil
	case proto.Increment:
		return &proto.IncrementRequest{}, &proto.IncrementResponse{}, nil
	case proto.Delete:
		return &proto.DeleteRequest{}, &proto.DeleteResponse{}, nil
	case proto.DeleteRange:
		return &proto.DeleteRangeRequest{}, &proto.DeleteRangeResponse{}, nil
	case proto.Scan:
		return &proto.ScanRequest{}, &proto.ScanResponse{}, nil
	case proto.BeginTransaction:
		return &proto.BeginTransactionRequest{}, &proto.BeginTransactionResponse{}, nil
	case proto.EndTransaction:
		return &proto.EndTransactionRequest{}, &proto.EndTransactionResponse{}, nil
	case proto.AccumulateTS:
		return &proto.AccumulateTSRequest{}, &proto.AccumulateTSResponse{}, nil
	case proto.ReapQueue:
		return &proto.ReapQueueRequest{}, &proto.ReapQueueResponse{}, nil
	case proto.EnqueueUpdate:
		return &proto.EnqueueUpdateRequest{}, &proto.EnqueueUpdateResponse{}, nil
	case proto.EnqueueMessage:
		return &proto.EnqueueMessageRequest{}, &proto.EnqueueMessageResponse{}, nil
	case proto.AdminSplit:
		return &proto.AdminSplitRequest{}, &proto.AdminSplitResponse{}, nil
	}
	return nil, nil, util.Errorf("unhandled public method %s", method)
}

// ServeHTTP serves the key-value API by treating the request URL path
// as the method, the request body as the arguments, and sets the
// response body as the method reply. The request body is unmarshalled
// into arguments based on the Content-Type request header. Protobuf,
// JSON, and YAML requests are supported. The response body is encoded
// according the the request's Accept header, or if not present, in
// the same format as the request's incoming Content-Type header.
func (s *DBServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method := r.URL.Path
	if !strings.HasPrefix(method, DBPrefix) {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	method = proto.CanonicalMethod(strings.TrimPrefix(method, DBPrefix))
	if !proto.IsPublic(method) {
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
	args, reply, err := createArgsAndReply(method)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := util.UnmarshalRequest(r, reqBody, args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create a call and invoke through sender.
	call := &client.Call{
		Method: method,
		Args:   args,
		Reply:  reply,
	}
	s.sender.Send(call)

	// Marshal the response.
	body, contentType, err := util.MarshalResponse(r, reply)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	fmt.Fprintf(w, "%s", string(body))
}
