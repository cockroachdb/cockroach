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

var allowedEncodings = []util.EncodingType{util.JSONEncoding, util.ProtoEncoding}

// verifyRequest checks for illegal inputs in request proto and
// returns an error indicating which, if any, were found.
func verifyRequest(args proto.Request) error {
	switch t := args.(type) {
	case *proto.EndTransactionRequest:
		if t.InternalCommitTrigger != nil {
			return util.Errorf("EndTransaction request from public KV API contains commit trigger: %+v", t.GetInternalCommitTrigger())
		}
	}
	return nil
}

// createArgsAndReply returns allocated request and response pairs
// according to the specified method. Note that createArgsAndReply
// only knows about public methods and explicitly returns nil for
// internal methods. Do not change this behavior without also fixing
// DBServer.ServeHTTP.
func createArgsAndReply(method string) (proto.Request, proto.Response) {
	switch method {
	case proto.Contains:
		return &proto.ContainsRequest{}, &proto.ContainsResponse{}
	case proto.Get:
		return &proto.GetRequest{}, &proto.GetResponse{}
	case proto.Put:
		return &proto.PutRequest{}, &proto.PutResponse{}
	case proto.ConditionalPut:
		return &proto.ConditionalPutRequest{}, &proto.ConditionalPutResponse{}
	case proto.Increment:
		return &proto.IncrementRequest{}, &proto.IncrementResponse{}
	case proto.Delete:
		return &proto.DeleteRequest{}, &proto.DeleteResponse{}
	case proto.DeleteRange:
		return &proto.DeleteRangeRequest{}, &proto.DeleteRangeResponse{}
	case proto.Scan:
		return &proto.ScanRequest{}, &proto.ScanResponse{}
	case proto.EndTransaction:
		return &proto.EndTransactionRequest{}, &proto.EndTransactionResponse{}
	case proto.ReapQueue:
		return &proto.ReapQueueRequest{}, &proto.ReapQueueResponse{}
	case proto.EnqueueUpdate:
		return &proto.EnqueueUpdateRequest{}, &proto.EnqueueUpdateResponse{}
	case proto.EnqueueMessage:
		return &proto.EnqueueMessageRequest{}, &proto.EnqueueMessageResponse{}
	case proto.Batch:
		return &proto.BatchRequest{}, &proto.BatchResponse{}
	case proto.AdminSplit:
		return &proto.AdminSplitRequest{}, &proto.AdminSplitResponse{}
	case proto.AdminMerge:
		return &proto.AdminMergeRequest{}, &proto.AdminMergeResponse{}
	}
	return nil, nil
}

// A DBServer provides an HTTP server endpoint serving the key-value API.
// It accepts either JSON or serialized protobuf content types.
type DBServer struct {
	sender client.KVSender
}

// NewDBServer allocates and returns a new DBServer.
func NewDBServer(sender client.KVSender) *DBServer {
	return &DBServer{sender: sender}
}

// ServeHTTP serves the key-value API by treating the request URL path
// as the method, the request body as the arguments, and sets the
// response body as the method reply. The request body is unmarshalled
// into arguments based on the Content-Type request header. Protobuf
// and JSON-encoded requests are supported. The response body is
// encoded according the the request's Accept header, or if not
// present, in the same format as the request's incoming Content-Type
// header.
func (s *DBServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method := r.URL.Path
	if !strings.HasPrefix(method, DBPrefix) {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	method = strings.TrimPrefix(method, DBPrefix)
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

	// Verify the request for public API.
	if err := verifyRequest(args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create a call and invoke through sender.
	s.sender.Send(client.Call{Args: args, Reply: reply})

	// Marshal the response.
	body, contentType, err := util.MarshalResponse(r, reply, allowedEncodings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(body)
}
