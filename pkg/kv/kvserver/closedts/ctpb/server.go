// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ctpb

import "context"

// InboundClient is an interface that narrows ClosedTimestamp_GetServer down to what's
// actually required.
type InboundClient interface {
	Send(*Entry) error
	Recv() (*Reaction, error)
	Context() context.Context
}

// Server is the interface implemented by types that want to serve incoming
// closed timestamp update streams.
type Server interface {
	Get(InboundClient) error
}

// ServerShim is a wrapper around Server that provides the wider interface that
// gRPC expects.
type ServerShim struct{ Server }

var _ ClosedTimestampServer = (*ServerShim)(nil)

// Get implements ClosedTimestampServer by passing through to the wrapped Server.
func (s ServerShim) Get(client ClosedTimestamp_GetServer) error {
	return s.Server.Get(client)
}

var _ InboundClient = ClosedTimestamp_GetServer(nil)
