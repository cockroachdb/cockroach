// Copyright 2018 The Cockroach Authors.
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

package ctpb

import (
	"context"
	"errors"

	"google.golang.org/grpc/metadata"
)

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

// InboundClientShim extends a ctpb.InboundClient to a ClosedTimestamp_GetServer
// by returning errors from all added methods where possible, and no-oping the
// rest.
type InboundClientShim struct {
	InboundClient
}

// SetHeader is a shim implementation for ClosedTimestamp_GetServer
// that always returns an error.
func (s InboundClientShim) SetHeader(metadata.MD) error {
	return errors.New("unimplemented")
}

// SendHeader is a shim implementation for ClosedTimestamp_GetServer
// that always returns an error.
func (s InboundClientShim) SendHeader(metadata.MD) error {
	return errors.New("unimplemented")
}

// SetTrailer is a shim implementation for ClosedTimestamp_GetServer
// that ignores the argument.
func (s InboundClientShim) SetTrailer(metadata.MD) {}

// SendMsg is a shim implementation for ClosedTimestamp_GetServer
// that always returns an error.
func (s InboundClientShim) SendMsg(m interface{}) error {
	return errors.New("unimplemented")
}

// RecvMsg is a shim implementation for ClosedTimestamp_GetServer
// that always returns an error.
func (s InboundClientShim) RecvMsg(m interface{}) error {
	return errors.New("unimplemented")
}
