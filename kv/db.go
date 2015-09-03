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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

var allExternalMethods = [...]proto.Request{
	proto.Get:            &proto.GetRequest{},
	proto.Put:            &proto.PutRequest{},
	proto.ConditionalPut: &proto.ConditionalPutRequest{},
	proto.Increment:      &proto.IncrementRequest{},
	proto.Delete:         &proto.DeleteRequest{},
	proto.DeleteRange:    &proto.DeleteRangeRequest{},
	proto.Scan:           &proto.ScanRequest{},
	proto.ReverseScan:    &proto.ReverseScanRequest{},
	proto.EndTransaction: &proto.EndTransactionRequest{},
	proto.Batch:          &proto.BatchRequest{},
	proto.AdminSplit:     &proto.AdminSplitRequest{},
	proto.AdminMerge:     &proto.AdminMergeRequest{},
}

// A DBServer provides an HTTP server endpoint serving the key-value API.
// It accepts either JSON or serialized protobuf content types.
type DBServer struct {
	context *base.Context
	sender  client.Sender
}

// NewDBServer allocates and returns a new DBServer.
func NewDBServer(ctx *base.Context, sender client.Sender) *DBServer {
	return &DBServer{context: ctx, sender: sender}
}

// RegisterRPC registers the RPC endpoints.
func (s *DBServer) RegisterRPC(rpcServer *rpc.Server) error {
	for i, reqType := range allExternalMethods {
		if reqType == nil {
			continue
		}

		method := proto.Method(i)

		if err := rpcServer.Register(fmt.Sprintf("Server.%s", method), s.executeCmd, reqType); err != nil {
			return err
		}
	}
	return nil
}

// executeCmd creates a proto.Call struct and sends it via our local sender.
func (s *DBServer) executeCmd(argsI gogoproto.Message) (gogoproto.Message, error) {
	args := argsI.(proto.Request)
	reply := args.CreateReply()
	if err := verifyRequest(args); err != nil {
		return nil, err
	}
	s.sender.Send(context.TODO(), proto.Call{Args: args, Reply: reply})
	return reply, nil
}

// verifyRequest checks for illegal inputs in request proto and
// returns an error indicating which, if any, were found.
func verifyRequest(args proto.Request) error {
	switch t := args.(type) {
	case *proto.EndTransactionRequest:
		return verifyEndTransaction(t)
	case *proto.BatchRequest:
		for _, reqUnion := range t.Requests {
			req := reqUnion.GetValue()

			if et, ok := req.(*proto.EndTransactionRequest); ok {
				if err := verifyEndTransaction(et); err != nil {
					return err
				}
			}

			method := req.(proto.Request).Method()

			if int(method) > len(allExternalMethods) || allExternalMethods[method] == nil {
				return util.Errorf("Batch contains an internal request %s", method)
			}
		}
	}
	return nil
}

func verifyEndTransaction(req *proto.EndTransactionRequest) error {
	if req.InternalCommitTrigger != nil {
		return util.Errorf("EndTransaction request from external KV API contains commit trigger: %+v", req.InternalCommitTrigger)
	}
	return nil
}
