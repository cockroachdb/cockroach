// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ConnectionClass is the identifier of a group of RPC client sessions that are
// allowed to share an underlying TCP connections; RPC sessions with different
// connection classes are guaranteed to use separate gRPC client connections.
//
// RPC sessions that share a connection class are arbitrated using the gRPC flow
// control logic, see google.golang.org/grpc/internal/transport. The lack of
// support of prioritization in the current gRPC implementation is the reason
// why we are separating different priority flows across separate TCP
// connections. Future gRPC improvements may enable further simplification
// here. See https://github.com/grpc/grpc-go/issues/1448 for progress on gRPC's
// adoption of HTTP2 priorities.
type ConnectionClass int8

const (
	// DefaultClass is the default ConnectionClass and should be used for most
	// client traffic.
	DefaultClass ConnectionClass = iota
	// SystemClass is the ConnectionClass used for system traffic.
	SystemClass

	// NumConnectionClasses is the number of valid ConnectionClass values.
	NumConnectionClasses int = iota
)

var systemClassKeyPrefixes = []roachpb.RKey{
	roachpb.RKey(keys.Meta1Prefix),
	roachpb.RKey(keys.NodeLivenessPrefix),
}

// ConnectionClassForKey determines the ConnectionClass which should be used
// for traffic addressed to the RKey.
func ConnectionClassForKey(key roachpb.RKey) ConnectionClass {
	// An empty RKey addresses range 1 and warrants SystemClass.
	if len(key) == 0 {
		return SystemClass
	}
	for _, prefix := range systemClassKeyPrefixes {
		if bytes.HasPrefix(key, prefix) {
			return SystemClass
		}
	}
	return DefaultClass
}
