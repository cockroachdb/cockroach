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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"fmt"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

var allExternalMethods = [...]roachpb.Request{
	roachpb.Get:                &roachpb.GetRequest{},
	roachpb.Put:                &roachpb.PutRequest{},
	roachpb.ConditionalPut:     &roachpb.ConditionalPutRequest{},
	roachpb.Increment:          &roachpb.IncrementRequest{},
	roachpb.Delete:             &roachpb.DeleteRequest{},
	roachpb.DeleteRange:        &roachpb.DeleteRangeRequest{},
	roachpb.Scan:               &roachpb.ScanRequest{},
	roachpb.ReverseScan:        &roachpb.ReverseScanRequest{},
	roachpb.BeginTransaction:   &roachpb.BeginTransactionRequest{},
	roachpb.EndTransaction:     &roachpb.EndTransactionRequest{},
	roachpb.AdminSplit:         &roachpb.AdminSplitRequest{},
	roachpb.AdminMerge:         &roachpb.AdminMergeRequest{},
	roachpb.AdminTransferLease: &roachpb.AdminTransferLeaseRequest{},
	roachpb.CheckConsistency:   &roachpb.CheckConsistencyRequest{},
	roachpb.RangeLookup:        &roachpb.RangeLookupRequest{},
	roachpb.WriteBatch:         &roachpb.WriteBatchRequest{},
	roachpb.Export:             &roachpb.ExportRequest{},
}

// A DBServer provides an HTTP server endpoint serving the key-value API.
// It accepts either JSON or serialized protobuf content types.
type DBServer struct {
	context *base.Config
	sender  client.Sender
	stopper *stop.Stopper
}

// NewDBServer allocates and returns a new DBServer.
func NewDBServer(ctx *base.Config, sender client.Sender, stopper *stop.Stopper) *DBServer {
	return &DBServer{
		context: ctx,
		sender:  sender,
		stopper: stopper,
	}
}

// Batch implements the roachpb.KVServer interface.
func (s *DBServer) Batch(
	ctx context.Context, args *roachpb.BatchRequest,
) (br *roachpb.BatchResponse, err error) {
	// TODO(marc,bdarnell): this code is duplicated in server/node.go,
	// which should be fixed.
	defer func() {
		// We always return errors via BatchResponse.Error so structure is
		// preserved; plain errors are presumed to be from the RPC
		// framework and not from cockroach.
		if err != nil {
			if br == nil {
				br = &roachpb.BatchResponse{}
			}
			if br.Error != nil {
				panic(fmt.Sprintf(
					"attempting to return both a plain error (%s) and roachpb.Error (%s)", err, br.Error))
			}
			br.Error = roachpb.NewError(err)
			err = nil
		}
	}()
	// TODO(marc): grpc's authentication model (which gives credential access in
	// the request handler) doesn't really fit with the current design of the
	// security package (which assumes that TLS state is only given at connection
	// time) - that should be fixed.
	if peer, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo); ok {
			certUser, err := security.GetCertificateUser(&tlsInfo.State)
			if err != nil {
				return nil, err
			}
			if certUser != security.NodeUser {
				return nil, errors.Errorf("user %s is not allowed", certUser)
			}
		}
	}

	if err = verifyRequest(args); err != nil {
		return br, err
	}

	err = s.stopper.RunTask(func() {
		var pErr *roachpb.Error
		// TODO(wiz): This is required to be a different context from the one
		// provided by grpc since it has to last for the entire transaction and not
		// just this one RPC call. See comment for (*TxnCoordSender).hearbeatLoop.
		br, pErr = s.sender.Send(context.TODO(), *args)
		if pErr != nil {
			br = &roachpb.BatchResponse{}
		}
		if br.Error != nil {
			panic(roachpb.ErrorUnexpectedlySet(s.sender, br))
		}
		br.Error = pErr
	})
	return br, err
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

		if int(method) >= len(allExternalMethods) || allExternalMethods[method] == nil {
			return errors.Errorf("Batch contains an internal request %s", method)
		}
	}
	return nil
}

func verifyEndTransaction(req *roachpb.EndTransactionRequest) error {
	if req.InternalCommitTrigger != nil {
		return errors.Errorf("EndTransaction request from external KV API contains commit trigger: %+v", req.InternalCommitTrigger)
	}
	return nil
}
