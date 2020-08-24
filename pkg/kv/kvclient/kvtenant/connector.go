// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package kvtenant provides utilities required by SQL-only tenant processes in
// order to interact with the key-value layer.
package kvtenant

import (
	"context"
	"net"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// Connector mediates the communication of cluster-wide state to sandboxed
// SQL-only tenant processes through a restricted interface. A Connector is
// seeded with a set of one or more network addresses that reference existing
// KV nodes in the cluster (or a load-balancer which fans out to some/all KV
// nodes). On startup, it establishes contact with one of these nodes to learn
// about the topology of the cluster and bootstrap the rest of SQL <-> KV
// network communication.
type Connector interface {
	// Start starts the connector.
	Start(context.Context) error

	// Connector is capable of providing information on each of the KV nodes in
	// the cluster in the form of NodeDescriptors. This obviates the need for
	// SQL-only tenant processes to join the cluster-wide gossip network.
	kvcoord.NodeDescStore

	// Connector is capable of providing Range addressing information in the
	// form of RangeDescriptors through delegated RangeLookup requests. This is
	// necessary because SQL-only tenants are restricted from reading Range
	// Metadata keys directly. Instead, the RangeLookup requests are proxied
	// through existing KV nodes while being subject to additional validation
	// (e.g. is the Range being requested owned by the requesting tenant?).
	rangecache.RangeDescriptorDB

	// Connector is capable of providing a filtered view of the SystemConfig
	// containing only information applicable to secondary tenants. This
	// obviates the need for SQL-only tenant processes to join the cluster-wide
	// gossip network.
	config.SystemConfigProvider
}

// ConnectorConfig encompasses the configuration required to create a Connector.
type ConnectorConfig struct {
	AmbientCtx        log.AmbientContext
	RPCContext        *rpc.Context
	RPCRetryOptions   retry.Options
	DefaultZoneConfig *zonepb.ZoneConfig
}

// ConnectorFactory constructs a new tenant Connector from the provide network
// addresses pointing to KV nodes.
type ConnectorFactory interface {
	NewConnector(cfg ConnectorConfig, addrs []string) (Connector, error)
}

// Factory is a hook for binaries that include CCL code to inject a
// ConnectorFactory.
var Factory ConnectorFactory = requiresCCLBinaryFactory{}

type requiresCCLBinaryFactory struct{}

func (requiresCCLBinaryFactory) NewConnector(_ ConnectorConfig, _ []string) (Connector, error) {
	return nil, errors.Errorf(`tenant connector requires a CCL binary`)
}

// AddressResolver wraps a Connector in an adapter that allows it be used as a
// nodedialer.AddressResolver. Addresses are resolved to a node's KV
// address.
func AddressResolver(c Connector) nodedialer.AddressResolver {
	return func(nodeID roachpb.NodeID) (net.Addr, error) {
		nd, err := c.GetNodeDescriptor(nodeID)
		if err != nil {
			return nil, err
		}
		return &nd.Address, nil
	}
}

// GossipSubscriptionSystemConfigMask filters a system config down to just the
// keys that a tenant SQL process needs access to. All system tenant objects are
// filtered out (e.g. system tenant descriptors and users).
var GossipSubscriptionSystemConfigMask = config.MakeSystemConfigMask(
	// Tenant SQL processes need just enough of the zone hierarchy to understand
	// which zone configurations apply to their keyspace.
	config.MakeZoneKey(keys.RootNamespaceID),
	config.MakeZoneKey(keys.TenantsRangesID),
)
