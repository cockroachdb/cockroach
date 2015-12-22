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
// Author: Peter Mattis (peter@cockroachlabs.com)
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

var allowedEncodings = []util.EncodingType{util.JSONEncoding, util.ProtoEncoding}

// An Server provides both an HTTP and RPC server endpoint serving the SQL API.
// The HTTP endpoint accepts either JSON or serialized protobuf content types.
type Server struct {
	context *base.Context
	*Executor
}

// MakeServer creates a Server.
func MakeServer(ctx *base.Context, db client.DB, gossip *gossip.Gossip, leaseMgr *LeaseManager, stopper *stop.Stopper) Server {
	return Server{context: ctx, Executor: newExecutor(db, gossip, leaseMgr, stopper)}
}

// ServeHTTP serves the SQL API by treating the request URL path
// as the method, the request body as the arguments, and sets the
// response body as the method reply. The request body is unmarshalled
// into arguments based on the Content-Type request header. Protobuf
// and JSON-encoded requests are supported. The response body is
// encoded according to the request's Accept header, or if not
// present, in the same format as the request's incoming Content-Type
// header.
func (s Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	method := r.URL.Path
	if !strings.HasPrefix(method, driver.Endpoint) {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	// Check TLS settings.
	authenticationHook, err := security.ProtoAuthHook(s.context.Insecure, r.TLS)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	method = strings.TrimPrefix(method, driver.Endpoint)
	if method != driver.Execute.String() {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	// Unmarshal the request.
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var args driver.Request
	if err := util.UnmarshalRequest(r, reqBody, &args, allowedEncodings); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Check request user against client certificate user.
	if err := authenticationHook(&args, true /* public */); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	reply, code, err := s.Execute(args)
	if err != nil {
		http.Error(w, err.Error(), code)
	}

	// Marshal the response.
	body, contentType, err := util.MarshalResponse(r, &reply, allowedEncodings)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(util.ContentTypeHeader, contentType)
	if _, err := w.Write(body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// RegisterRPC registers the SQL RPC endpoint.
func (s Server) RegisterRPC(rpcServer *rpc.Server) error {
	return rpcServer.RegisterPublic(driver.RPCMethod, s.executeCmd, &driver.Request{})
}

func (s Server) executeCmd(argsI proto.Message) (proto.Message, error) {
	args := argsI.(*driver.Request)
	reply, _, err := s.Execute(*args)
	return &reply, err
}
