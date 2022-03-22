// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// separator is used to separate the non-prefix components of a
// Gossip key, facilitating the automated generation of regular
// expressions for various prefixes.
// It must not be contained in any of the other keys defined here.
const separator = ":"

// Constants for gossip keys.
const (
	// KeyClusterID is the unique UUID for this Cockroach cluster.
	// The value is a string UUID for the cluster. The cluster ID is
	// gossiped by all nodes that contain a replica of the first range,
	// and it serves as a check for basic gossip connectivity. The
	// Gossip.Connected channel is closed when we see this key.
	KeyClusterID = "cluster-id"

	// KeyStorePrefix is the key prefix for gossiping stores in the network.
	// The suffix is a store ID and the value is a roachpb.StoreDescriptor.
	KeyStorePrefix = "store"

	// KeyNodeIDPrefix is the key prefix for gossiping node id addresses.
	// The actual key is suffixed with the decimal representation of the
	// node id (e.g. 'node:1') and the value is a roachpb.NodeDescriptor.
	KeyNodeIDPrefix = "node"

	// KeyHealthAlertPrefix is the key prefix for gossiping health alerts.
	// The value is a proto of type HealthCheckResult.
	KeyNodeHealthAlertPrefix = "health-alert"

	// KeyNodeLivenessPrefix is the key prefix for gossiping node liveness
	// info.
	KeyNodeLivenessPrefix = "liveness"

	// KeySentinel is a key for gossip which must not expire or
	// else the node considers itself partitioned and will retry with
	// bootstrap hosts.  The sentinel is gossiped by the node that holds
	// the range lease for the first range.
	KeySentinel = "sentinel"

	// KeyFirstRangeDescriptor is the descriptor for the "first" range. The
	// "first" range contains the meta1 key range, the first level of the
	// bi-level key addressing scheme. The value is a roachpb.RangeDescriptor.
	KeyFirstRangeDescriptor = "first-range"

	// KeyDeprecatedSystemConfig is the gossip key for the system DB span.
	// The value if a config.SystemConfig which holds all key/value
	// pairs in the system DB span.
	//
	// This key is used in the 21.2<->22.1 mixed version state. It is not used
	// in 22.1. However, it was written without a TTL, so there no guarantee
	// that it will actually be removed from the gossip network.
	//
	// TODO(ajwerner): Write a migration to remove the data, or release a
	// a version which drops the key entirely, and then, in a subsequent
	// release, delete this key.
	KeyDeprecatedSystemConfig = "system-db"

	// KeyDistSQLNodeVersionKeyPrefix is key prefix for each node's DistSQL
	// version.
	KeyDistSQLNodeVersionKeyPrefix = "distsql-version"

	// KeyDistSQLDrainingPrefix is the key prefix for each node's DistSQL
	// draining state.
	KeyDistSQLDrainingPrefix = "distsql-draining"

	// KeyGossipClientsPrefix is the prefix for keys that indicate which gossip
	// client connections a node has open. This is used by other nodes in the
	// cluster to build a map of the gossip network.
	KeyGossipClientsPrefix = "gossip-clients"

	// KeyGossipStatementDiagnosticsRequest is the gossip key for new statement
	// diagnostics requests. The values is the id of the request that generated
	// the notification, as a little-endian-encoded uint64.
	// stmtDiagnosticsRequestRegistry listens for notifications and responds by
	// polling for new requests.
	KeyGossipStatementDiagnosticsRequest = "stmt-diag-req"

	// KeyGossipStatementDiagnosticsRequestCancellation is the gossip key for
	// canceling an existing diagnostics request. The values is the id of the
	// request that needs to be canceled, as a little-endian-encoded uint64.
	// stmtDiagnosticsRequestRegistry listens for notifications and responds by
	// polling for new requests.
	KeyGossipStatementDiagnosticsRequestCancellation = "stmt-diag-cancel-req"
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
func MakeNodeIDKey(nodeID roachpb.NodeID) string {
	return MakeKey(KeyNodeIDPrefix, nodeID.String())
}

// IsNodeIDKey returns true iff the provided key is a valid node ID key.
func IsNodeIDKey(key string) bool {
	return strings.HasPrefix(key, KeyNodeIDPrefix+separator)
}

// NodeIDFromKey attempts to extract a NodeID from the provided key after
// stripping the provided prefix. Returns an error if the key is not of the
// correct type or is not parsable.
func NodeIDFromKey(key string, prefix string) (roachpb.NodeID, error) {
	trimmedKey, err := removePrefixFromKey(key, prefix)
	if err != nil {
		return 0, err
	}
	nodeID, err := strconv.ParseInt(trimmedKey, 10 /* base */, 64 /* bitSize */)
	if err != nil {
		return 0, errors.Wrapf(err, "failed parsing NodeID from key %q", key)
	}
	return roachpb.NodeID(nodeID), nil
}

// MakeGossipClientsKey returns the gossip client key for the given node.
func MakeGossipClientsKey(nodeID roachpb.NodeID) string {
	return MakeKey(KeyGossipClientsPrefix, nodeID.String())
}

// MakeNodeHealthAlertKey returns the gossip key under which the given node can
// gossip health alerts.
func MakeNodeHealthAlertKey(nodeID roachpb.NodeID) string {
	return MakeKey(KeyNodeHealthAlertPrefix, strconv.Itoa(int(nodeID)))
}

// MakeNodeLivenessKey returns the gossip key for node liveness info.
func MakeNodeLivenessKey(nodeID roachpb.NodeID) string {
	return MakeKey(KeyNodeLivenessPrefix, nodeID.String())
}

// MakeStoreKey returns the gossip key for the given store.
func MakeStoreKey(storeID roachpb.StoreID) string {
	return MakeKey(KeyStorePrefix, storeID.String())
}

// StoreIDFromKey attempts to extract a StoreID from the provided key after
// stripping the provided prefix. Returns an error if the key is not of the
// correct type or is not parsable.
func StoreIDFromKey(storeKey string) (roachpb.StoreID, error) {
	trimmedKey, err := removePrefixFromKey(storeKey, KeyStorePrefix)
	if err != nil {
		return 0, err
	}
	storeID, err := strconv.ParseInt(trimmedKey, 10 /* base */, 64 /* bitSize */)
	if err != nil {
		return 0, errors.Wrapf(err, "failed parsing StoreID from key %q", storeKey)
	}
	return roachpb.StoreID(storeID), nil
}

// MakeDistSQLNodeVersionKey returns the gossip key for the given store.
func MakeDistSQLNodeVersionKey(instanceID base.SQLInstanceID) string {
	return MakeKey(KeyDistSQLNodeVersionKeyPrefix, instanceID.String())
}

// MakeDistSQLDrainingKey returns the gossip key for the given node's distsql
// draining state.
func MakeDistSQLDrainingKey(instanceID base.SQLInstanceID) string {
	return MakeKey(KeyDistSQLDrainingPrefix, instanceID.String())
}

// removePrefixFromKey removes the key prefix and separator and returns what's
// left. Returns an error if the key doesn't have this prefix.
func removePrefixFromKey(key, prefix string) (string, error) {
	trimmedKey := strings.TrimPrefix(key, prefix+separator)
	if trimmedKey == key {
		return "", errors.Errorf("%q does not have expected prefix %q%s", key, prefix, separator)
	}
	return trimmedKey, nil
}
