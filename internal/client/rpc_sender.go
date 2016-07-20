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
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/pkg/errors"
)

const healthyTimeout = 2 * time.Second

type sender struct {
	remoteAddr string
	conn       *grpc.ClientConn
	client     roachpb.ExternalClient
	rpcContext *rpc.Context
}

// NewSender returns an implementation of Sender which exposes the Key-Value
// database provided by a Cockroach cluster by connecting via RPC to a
// Cockroach node.
func NewSender(ctx *rpc.Context, remoteAddr string) (Sender, error) {
	conn, err := ctx.GRPCDial(remoteAddr)
	if err != nil {
		return nil, err
	}
	return &sender{
		remoteAddr: remoteAddr,
		conn:       conn,
		client:     roachpb.NewExternalClient(conn),
		rpcContext: ctx,
	}, nil
}

// Send implements the Sender interface.
func (s *sender) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	if err := s.rpcContext.WaitForConnected(s.remoteAddr, healthyTimeout); err != nil {
		return nil, roachpb.NewError(errors.Wrap(err, "roachpb.Batch RPC"))
	}

	br, err := s.client.Batch(ctx, &ba)
	if err != nil {
		return nil, roachpb.NewError(errors.Wrap(err, "roachpb.Batch RPC failed"))
	}
	pErr := br.Error
	br.Error = nil
	return br, pErr
}
