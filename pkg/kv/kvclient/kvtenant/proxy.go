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

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// Proxy mediates the communication of cluster-wide state to sandboxed SQL-only
// tenant processes through a restricted interface. A Proxy is seeded with a set
// of one or more network addresses that reference existing KV nodes in the
// cluster (or a load-balancer which fans out to some/all KV nodes). On startup,
// it establishes contact with one of these nodes to learn about the topology of
// the cluster and bootstrap the rest of SQL <-> KV network communication.
type Proxy interface {
	// Start starts the proxy.
	Start(context.Context) error

	// Proxy is capable of providing information on each of the KV nodes in the
	// cluster in the form of NodeDescriptors. This obviates the need for SQL-only
	// tenant processes to join the cluster-wide gossip network.
	kvcoord.NodeDescStore

	// Proxy is capable of providing Range addressing information in the form of
	// RangeDescriptors through delegated RangeLookup requests. This is necessary
	// because SQL-only tenants are restricted from reading Range Metadata keys
	// directly. Instead, the RangeLookup requests are proxied through existing KV
	// nodes while being subject to additional validation (e.g. is the Range being
	// requested owned by the requesting tenant?).
	kvcoord.RangeDescriptorDB
}

// ProxyFactory constructs a new tenant proxy from the provide network addresses
// pointing to KV nodes.
type ProxyFactory interface {
	NewProxy(_ log.AmbientContext, _ *rpc.Context, _ retry.Options, addrs []string) (Proxy, error)
}

// Factory is a hook for binaries that include CCL code to inject a ProxyFactory.
var Factory ProxyFactory = requiresCCLBinaryFactory{}

type requiresCCLBinaryFactory struct{}

func (requiresCCLBinaryFactory) NewProxy(
	_ log.AmbientContext, _ *rpc.Context, _ retry.Options, _ []string,
) (Proxy, error) {
	return nil, errors.Errorf(`tenant proxy requires a CCL binary`)
}

// AddressResolver wraps a Proxy in an adapter that allows it be used as a
// nodedialer.AddressResolver. Addresses are resolved to a node's tenant KV
// address. See NodeDescriptor.CheckedTenantAddress.
func AddressResolver(p Proxy) nodedialer.AddressResolver {
	return func(nodeID roachpb.NodeID) (net.Addr, error) {
		nd, err := p.GetNodeDescriptor(nodeID)
		if err != nil {
			return nil, err
		}
		return nd.CheckedTenantAddress(), nil
	}
}
