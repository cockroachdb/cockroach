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

package client

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/pkg/errors"
)

type sender struct {
	roachpb.ExternalClient
}

// NewSender returns an implementation of Sender which exposes the Key-Value
// database provided by a Cockroach cluster by connecting via RPC to a
// Cockroach node.
func NewSender(ctx *rpc.Context, target string) (Sender, error) {
	// We don't use ctx.GRPCDial because this is an external client connection
	// and we don't want to run the heartbeat service which will close the
	// connection if the transport fails.
	dialOpt, err := ctx.GRPCDialOption()
	if err != nil {
		return nil, err
	}
	conn, err := grpc.Dial(target, dialOpt)
	if err != nil {
		return nil, err
	}
	ctx.Stopper.AddCloser(stop.CloserFn(func() {
		_ = conn.Close() // we're closing, ignore the error
	}))
	return sender{roachpb.NewExternalClient(conn)}, nil
}

// Send implements the Sender interface.
func (s sender) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	br, err := s.Batch(ctx, &ba, grpc.FailFast(false))
	if err != nil {
		return nil, roachpb.NewError(errors.Wrap(err, "roachpb.Batch RPC failed"))
	}
	pErr := br.Error
	br.Error = nil
	return br, pErr
}
