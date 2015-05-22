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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/proto"
)

// separator is used to separate the non-prefix components of a
// Gossip key, facilitating the automated generation of regular
// expressions for various prefixes.
// It must not be contained in any of the other keys defined here.
const separator = ":"

// Constants for gossip keys.
const (
	// KeyClusterID is the unique UUID for this Cockroach cluster.
	// The value is a string UUID for the cluster.
	// The cluster ID is gossiped by all nodes that contain a replica
	// of the first range, and it serves as a check for basic gossip
	// connectivity. The Gossip.Connected channel is closed when we see
	// this key.
	KeyClusterID = "cluster-id"

	// KeyConfigAccounting is the accounting configuration map.
	KeyConfigAccounting = "accounting"

	// KeyConfigPermission is the permission configuration map.
	KeyConfigPermission = "permissions"

	// KeyConfigZone is the zone configuration map.
	KeyConfigZone = "zones"

	// KeyCapacityPrefix is the key prefix for gossiping available store
	// capacity. The suffix is composed of: <node ID>-<store ID>.  The
	// value is a storage.StoreDescriptor struct.
	KeyCapacityPrefix = "capacity"

	// KeyNodeCount is the count of gossip nodes in the network. The
	// value is an int64 containing the count of nodes in the cluster.
	// TODO(spencer): should remove this and instead just count the
	//   number of node ids being gossiped.
	KeyNodeCount = "node-count"

	// KeyNodeIDPrefix is the key prefix for gossiping node id
	// addresses. The actual key is suffixed with the decimal
	// representation of the node id and the value is the host:port
	// string address of the node. E.g. node:1 => 127.0.0.1:24001
	KeyNodeIDPrefix = "node"

	// KeySentinel is a key for gossip which must not expire or else the
	// node considers itself partitioned and will retry with bootstrap hosts.
	// The sentinel is gossiped by the node that holds the leader lease for the
	// first range.
	KeySentinel = "sentinel"

	// KeyFirstRangeDescriptor is the descriptor for the "first"
	// range. The "first" range contains the meta1 key range, the first
	// level of the bi-level key addressing scheme. The value is a slice
	// of storage.Replica structs.
	KeyFirstRangeDescriptor = "first-range"
)

// MakeKey creates a canonical key under which to gossip a piece of
// information. The first argument will typically be one of the key constants
// defined in this package.
func MakeKey(components ...string) string {
	return strings.Join(components, separator)
}

// MakePrefixPattern returns a regular expression pattern that
// matches precisely the Gossip keys created by invocations of
// MakeKey with multiple arguments for which the first argument
// is equal to the given prefix.
func MakePrefixPattern(prefix string) string {
	return regexp.QuoteMeta(prefix+separator) + ".*"
}

// MakeNodeIDKey returns the gossip key for node ID info.
func MakeNodeIDKey(nodeID proto.NodeID) string {
	return MakeKey(KeyNodeIDPrefix, nodeID.String())
}

// MakeCapacityKey returns the gossip key for the given store's capacity.
func MakeCapacityKey(nodeID proto.NodeID, storeID proto.StoreID) string {
	return MakeKey(KeyCapacityPrefix, nodeID.String(), "-", storeID.String())
}
