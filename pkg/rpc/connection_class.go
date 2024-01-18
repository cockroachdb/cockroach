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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
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
	// RangefeedClass is the ConnectionClass used for rangefeeds.
	RangefeedClass
	// RaftClass is the ConnectionClass used for raft traffic.
	RaftClass

	// NumConnectionClasses is the number of valid ConnectionClass values.
	NumConnectionClasses int = iota
)

var (
	classOverridesEnv = envutil.EnvOrDefaultString("COCKROACH_USE_DEFAULT_CONNECTION_CLASS", "")

	// classOverrides contains a mapping from ConnectionClass to ConnectionClass.
	classOverrides = parseClassOverrides(classOverridesEnv)
)

// parseClassOverrides parses the list of connection class overrides. See
// classOverrides variable comment for syntax.
func parseClassOverrides(str string) map[ConnectionClass]ConnectionClass {
	overrides := make(map[ConnectionClass]ConnectionClass)
	for str += ","; len(str) != 0; {
		pos := strings.IndexByte(str, ',') // always finds a position
		name := str[:pos]
		if class, ok := connectionClassFromName[name]; ok {
			overrides[class] = DefaultClass
		}
		// NB: unknown classes are skipped for extensibility.
		str = str[pos+1:]
	}
	return overrides
}

// ConnectionClassOverride returns the RPC connection class that the passed in
// class overrides to.
func ConnectionClassOverride(c ConnectionClass) ConnectionClass {
	if override, ok := classOverrides[c]; ok {
		return override
	}
	return c
}

// ConnectionClassForTag returns the RPC connection class override associated
// with the given tag, or def if there is no override.
//
// This mechanism allows custom reconfigurations that can not be done with
// ConnectionClassOverride(). Example:
//
//   - Changefeed initial scan RPCs use BulkDataClass
//   - As well as the rangefeed traffic
//   - ConnectionClassOverride(BulkDataClass) is an override common to both
//   - ConnectionClassForTag("changefeed") overrides only the changefeed RPCs
//
// TODO(pav-kv): Think about cases when there are two overlapping overrides. One
// needs to take precedence.
func ConnectionClassForTag(tag string, def ConnectionClass) ConnectionClass {
	if pos := strings.Index(classOverridesEnv, tag); pos != -1 {
		// NB: currently we only support overrides to default class. In the future,
		// we may need to take the override from a map.
		return DefaultClass
	}
	return def
}

// connectionClassName maps classes to their name.
var connectionClassName = map[ConnectionClass]string{
	DefaultClass:   "default",
	SystemClass:    "system",
	RangefeedClass: "rangefeed",
	RaftClass:      "raft",
}

var connectionClassFromName = map[string]ConnectionClass{
	"def":  DefaultClass,
	"sys":  SystemClass,
	"rf":   RangefeedClass,
	"raft": RaftClass,
}

// String implements the fmt.Stringer interface.
func (c ConnectionClass) String() string {
	return connectionClassName[c]
}

// SafeValue implements the redact.SafeValue interface.
func (ConnectionClass) SafeValue() {}

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
