// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
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

// systemClassSpan is the key span for all ranges considered to be a critical
// system range. Generally, not all system ranges are critical. For example, the
// timeseries ranges are not, because they can be busy and disrupt other
// system traffic. We try to make SystemClass responsive by keeping it small.
//
// All the ranges that include keys between /Meta1 and/ System/tsd are considered
// part of the system class. We keep things simple by checking if the start key
// of a range is within the system span to determine if we should use the
// SystemClass. This eliminates most of the coupling between how system ranges
// are structured (i.e. #NoSplits and # staticSplits lists) and how they
// split/merge except at the boundaries. Specifically it's safe to use
// keys.TimeseriesPrefix because nothing >= to this key will merge into this span.
var systemClassSpan = roachpb.Span{Key: keys.Meta1Prefix, EndKey: keys.TimeseriesPrefix}

// ConnectionClassForKey determines the ConnectionClass which should be used for
// traffic addressed to the range starting at the given key. Returns SystemClass
// for system ranges, or the given "default" class otherwise. Typically, the
// default depends on the type of traffic, such as RangefeedClass or RaftClass.
//
// This also takes the `COCKROACH_RPC_USE_DEFAULT_CONNECTION_CLASS` env variable
// into account, and returns DefaultClass instead of `def` if it is set.
func ConnectionClassForKey(key roachpb.RKey, def ConnectionClass) ConnectionClass {
	// We have to check for special condition of 0 length key, to catch Range 1.
	// Once https://github.com/cockroachdb/cockroach/issues/95055 is fixed, we can
	// remove this check.
	if len(key) == 0 || systemClassSpan.ContainsKey(key.AsRawKey()) {
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
