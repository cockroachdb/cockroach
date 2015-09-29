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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
)

var allExternalMethods = [...]roachpb.Request{
	roachpb.Get:            &roachpb.GetRequest{},
	roachpb.Put:            &roachpb.PutRequest{},
	roachpb.ConditionalPut: &roachpb.ConditionalPutRequest{},
	roachpb.Increment:      &roachpb.IncrementRequest{},
	roachpb.Delete:         &roachpb.DeleteRequest{},
	roachpb.DeleteRange:    &roachpb.DeleteRangeRequest{},
	roachpb.Scan:           &roachpb.ScanRequest{},
	roachpb.ReverseScan:    &roachpb.ReverseScanRequest{},
	roachpb.EndTransaction: &roachpb.EndTransactionRequest{},
	roachpb.AdminSplit:     &roachpb.AdminSplitRequest{},
	roachpb.AdminMerge:     &roachpb.AdminMergeRequest{},
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
	const method = "Server.Batch"
	return rpcServer.Register(method, s.executeCmd, &roachpb.BatchRequest{})
}

// executeCmd interprets the given message as a *proto.BatchRequest and sends it
// via the local sender.
func (s *DBServer) executeCmd(argsI proto.Message) (proto.Message, error) {
	ba := argsI.(*roachpb.BatchRequest)
	if err := verifyRequest(ba); err != nil {
		return nil, err
	}
	br, pErr := s.sender.Send(context.TODO(), *ba)
	if pErr != nil {
		br = &roachpb.BatchResponse{}
	}
	if br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(s.sender, br))
	}
	br.Error = pErr
	return br, nil
}

// verifyRequest checks for illegal inputs in request proto and
// returns an error indicating which, if any, were found.
func verifyRequest(ba *roachpb.BatchRequest) error {
	for _, reqUnion := range ba.Requests {
		req := reqUnion.GetInner()

		if et, ok := req.(*roachpb.EndTransactionRequest); ok {
			if err := verifyEndTransaction(et); err != nil {
				return err
			}
		}

		method := req.Method()

		if int(method) > len(allExternalMethods) || allExternalMethods[method] == nil {
			return util.Errorf("Batch contains an internal request %s", method)
		}
	}
	return nil
}

func verifyEndTransaction(req *roachpb.EndTransactionRequest) error {
	if req.InternalCommitTrigger != nil {
		return util.Errorf("EndTransaction request from external KV API contains commit trigger: %+v", req.InternalCommitTrigger)
	}
	return nil
}
