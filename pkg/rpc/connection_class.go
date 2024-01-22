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
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

// useDefaultConnectionClass defines whether all the traffic that does not
// qualify for SYSTEM connection class, should be sent via the DEFAULT class.
//
// This overrides the default behaviour in which some types of traffic have a
// dedicated connection class, such as RAFT.
//
// This option reverts the behaviour to pre-v24.1 state, and is an escape hatch
// in case something breaks with the separate connection classes (could be
// anything, e.g. an environment has limits on concurrent TCP connections).
var useDefaultConnectionClass = envutil.EnvOrDefaultBool(
	"COCKROACH_RPC_USE_DEFAULT_CONNECTION_CLASS", false)

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
//
// This also takes the `COCKROACH_RPC_USE_DEFAULT_CONNECTION_CLASS` env variable
// into account, and returns DefaultClass instead of `def` if it is set.
func ConnectionClassForKey(key roachpb.RKey, def ConnectionClass) ConnectionClass {
	if isSystemKey(key) {
		return SystemClass
	}
	return ConnectionClassOrDefault(def)
}

// ConnectionClassOrDefault returns the passed-in connection class if it is
// known/supported by this server. Otherwise, it returns DefaultClass.
//
// This also takes the `COCKROACH_RPC_USE_DEFAULT_CONNECTION_CLASS` env variable
// into account, and returns DefaultClass instead of `c` if it is set.
//
// This helper should be used when a ConnectionClass is not "trusted", such as
// when it was received over RPC from another node. In a mixed-version state, it
// is possible that one node supports a connection class, and another does not.
//
// If the simple behaviour of falling back to DefaultClass is not acceptable, a
// version gate must be used to transition between versions correctly.
func ConnectionClassOrDefault(c ConnectionClass) ConnectionClass {
	if c == SystemClass {
		return c
	} else if useDefaultConnectionClass || int(c) >= NumConnectionClasses {
		return DefaultClass
	}
	return c
}
