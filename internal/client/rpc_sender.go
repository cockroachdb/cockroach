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
)

const healthyTimeout = 2 * time.Second

type sender struct {
	conn   *grpc.ClientConn
	client roachpb.ExternalClient
}

// NewSender returns an implementation of Sender which exposes the Key-Value
// database provided by a Cockroach cluster by connecting via RPC to a
// Cockroach node.
func NewSender(ctx *rpc.Context, target string) (Sender, error) {
	conn, err := ctx.GRPCDial(target)
	if err != nil {
		return nil, err
	}
	return &sender{
		conn:   conn,
		client: roachpb.NewExternalClient(conn),
	}, nil
}

// Send implements the Sender interface.
func (s *sender) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, healthyTimeout)
	defer cancel()

	c := s.conn
	for state, err := c.State(); state != grpc.Ready; state, err = c.WaitForStateChange(ctxWithTimeout, state) {
		if err != nil {
			return nil, roachpb.NewErrorf("roachpb.Batch RPC failed: %s", err)
		}
		if state == grpc.Shutdown {
			return nil, roachpb.NewErrorf("roachpb.Batch RPC failed as client connection was closed")
		}
	}

	br, err := s.client.Batch(ctx, &ba)
	if err != nil {
		return nil, roachpb.NewErrorf("roachpb.Batch RPC failed: %s", err)
	}
	pErr := br.Error
	br.Error = nil
	return br, pErr
}
