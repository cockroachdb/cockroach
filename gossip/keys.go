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

import "strconv"

// Constants for gossip keys.
const (
	// KeyClusterID is the unique UUID for this Cockroach cluster.
	// The value is a string UUID for the cluster.
	KeyClusterID = "cluster-id"

	// KeyConfigAccounting is the accounting configuration map.
	KeyConfigAccounting = "accounting"

	// KeyConfigPermission is the permission configuration map.
	KeyConfigPermission = "permissions"

	// KeyConfigZone is the zone configuration map.
	KeyConfigZone = "zones"

	// KeyMaxAvailCapacityPrefix is the key prefix for gossiping available
	// store capacity. The suffix is composed of:
	// <store attributes>-<hex node ID>-<hex store ID>. The value is a
	// storage.StoreDescriptor struct.
	KeyMaxAvailCapacityPrefix = "max-avail-capacity-"

	// KeyNodeCount is the count of gossip nodes in the network. The
	// value is an int64 containing the count of nodes in the cluster.
	// TODO(spencer): should remove this and instead just count the
	//   number of node ids being gossiped.
	KeyNodeCount = "node-count"

	// KeyNodeIDPrefix is the key prefix for gossiping node id
	// addresses. The actual key is suffixed with the hexadecimal
	// representation of the node id and the value is the host:port
	// string address of the node. E.g. node-1bfa: fwd56.sjcb1:24001
	KeyNodeIDPrefix = "node-"

	// KeySentinel is a key for gossip which must not expire or else the
	// node considers itself partitioned and will retry with bootstrap hosts.
	KeySentinel = KeyClusterID

	// KeyFirstRangeDescriptor is the descriptor for the "first"
	// range. The "first" range contains the meta1 key range, the first
	// level of the bi-level key addressing scheme. The value is a slice
	// of storage.Replica structs.
	KeyFirstRangeDescriptor = "first-range"
)

// MakeNodeIDGossipKey returns the gossip key for node ID info.
func MakeNodeIDGossipKey(nodeID int32) string {
	return KeyNodeIDPrefix + strconv.FormatInt(int64(nodeID), 16)
}
