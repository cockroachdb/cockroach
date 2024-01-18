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
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcpb"
)

// ConnectionClass is the identifier of a group of RPC client sessions that are
// allowed to share an underlying TCP connection; RPC sessions with different
// connection classes are guaranteed to use separate gRPC client connections.
//
// See rpcpb.ConnectionClass comment for more details.
//
// TODO(pav-kv): remove the aliases, they are only used for code compatibility.
// While doing so, audit all sources of RPC traffic and sum them up.
type ConnectionClass = rpcpb.ConnectionClass

const (
	// DefaultClass is the default ConnectionClass used for most client traffic.
	DefaultClass = rpcpb.ConnectionClass_DEFAULT
	// SystemClass is the ConnectionClass used for system traffic.
	SystemClass = rpcpb.ConnectionClass_SYSTEM
	// RangefeedClass is the ConnectionClass used for rangefeeds.
	RangefeedClass = rpcpb.ConnectionClass_RANGEFEED
	// RaftClass is the ConnectionClass used for raft traffic.
	RaftClass = rpcpb.ConnectionClass_RAFT

	// NumConnectionClasses is the number of valid ConnectionClass values.
	NumConnectionClasses = int(rpcpb.ConnectionClass_NEXT)
)

var systemClassKeyPrefixes = []roachpb.RKey{
	roachpb.RKey(keys.Meta1Prefix),
	roachpb.RKey(keys.NodeLivenessPrefix),
}

// isSystemKey returns true if the given key belongs to a range eligible for
// SystemClass connection.
//
// Generally, not all system ranges are eligible. For example, the timeseries
// ranges are not, because they can be busy and disrupt other system traffic. We
// try to make SystemClass responsive by keeping it small.
func isSystemKey(key roachpb.RKey) bool {
	// An empty RKey addresses range 1 and warrants SystemClass.
	if len(key) == 0 {
		return true
	}
	for _, prefix := range systemClassKeyPrefixes {
		if bytes.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// ConnectionClassForKey determines the ConnectionClass which should be used for
// traffic addressed to the range starting at the given key. Returns SystemClass
// for system ranges, or the given "default" class otherwise. Typically, the
// default depends on the type of traffic, such as RangefeedClass or RaftClass.
func ConnectionClassForKey(key roachpb.RKey, def ConnectionClass) ConnectionClass {
	if isSystemKey(key) {
		return SystemClass
	}
	return def
}
