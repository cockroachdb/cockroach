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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type sender struct {
	roachpb.ExternalClient
}

// NewSender returns an implementation of Sender which exposes the Key-Value
// database provided by a Cockroach cluster by connecting via RPC to a
// Cockroach node.
//
// This must not be used by server.Server or any of its components, only by
// clients talking to a Cockroach cluster through the external interface.
func NewSender(conn *grpc.ClientConn) Sender {
	return sender{roachpb.NewExternalClient(conn)}
}

// Send implements the Sender interface.
func (s sender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	br, err := s.Batch(ctx, &ba)
	if err != nil {
		return nil, roachpb.NewError(roachpb.NewSendError(err.Error()))
	}
	pErr := br.Error
	br.Error = nil
	return br, pErr
}
