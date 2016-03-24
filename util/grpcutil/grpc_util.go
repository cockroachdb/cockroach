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
// Author: Tamir Duberstein (tamird@gmail.com)

package grpcutil

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/transport"

	"github.com/cockroachdb/cockroach/util"
)

const (
	// RPCVersion is used in all GRPC context's metadata to ensure that
	// messages from different versions don't conflict.
	RPCVersion = "1"
	// RPCVersionKey is the key used to store and retrieve the version from an
	// RPC context's metadata.
	RPCVersionKey = "ver"
)

// IsGRPCRequest returns true if r came from a grpc client.
//
// Its logic is a partial recreation of gRPC's internal checks, see
// https://github.com/grpc/grpc-go/blob/01de3de/transport/handler_server.go#L61:L69
func IsGRPCRequest(r *http.Request) bool {
	return r.ProtoMajor == 2 && strings.Contains(r.Header.Get(util.ContentTypeHeader), "application/grpc")
}

// IsClosedConnection returns true if err is an error produced by gRPC on closed connections.
func IsClosedConnection(err error) bool {
	if err == context.Canceled ||
		grpc.Code(err) == codes.Canceled ||
		grpc.ErrorDesc(err) == grpc.ErrClientConnClosing.Error() ||
		strings.Contains(err.Error(), "is closing") {
		return true
	}
	if streamErr, ok := err.(transport.StreamError); ok && streamErr.Code == codes.Canceled {
		return true
	}
	return util.IsClosedConnection(err)
}

// AddVersionNumber adds the current RPC version number to a context for use
// by GRPC.
func AddVersionNumber(ctx context.Context) context.Context {
	md := metadata.Pairs(RPCVersionKey, RPCVersion)
	return metadata.NewContext(ctx, md)
}

var errNoMetadata = errors.New("no metadata found in the context")
var errNoVersionNumber = errors.New("no version number in the RPC context")

// CheckVersionNumber checks that the number supplied in an GRPC context is
// the correct one.
func CheckVersionNumber(ctx context.Context) error {
	md, ok := metadata.FromContext(ctx)
	if !ok {
		return errNoMetadata
	}
	versionNumber, ok := md[RPCVersionKey]
	if !ok {
		return errNoVersionNumber
	}
	if len(versionNumber) != 1 {
		return fmt.Errorf("expected only 1 RPC context version number, got %d", len(versionNumber))
	}
	if versionNumber[0] != RPCVersion {
		return fmt.Errorf("RPC version numbers do not match expected: %s, received: %s", RPCVersion, versionNumber[0])
	}
	return nil
}
