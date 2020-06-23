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
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"google.golang.org/grpc"
)

// Proxy mediates the communication of cluster-wide state to sandboxed SQL-only
// tenant processes through a restricted interface. A Proxy is seeded with a set
// of one or more network addresses that reference existing KV nodes in the
// cluster (or a load-balancer which fans out to some/all KV nodes). On startup,
// it establishes contact with one of these nodes to learn about the topology of
// the cluster and bootstrap the rest of SQL <-> KV network communication.
//
// See below for the Proxy's roles.
type Proxy struct {
	stopper         *stop.Stopper
	rpcContext      *rpc.Context
	rpcRetryOptions retry.Options
	rpcDialTimeout  time.Duration // for testing
	addrs           []string
	startupC        chan struct{}

	mu        syncutil.RWMutex
	nodeDescs map[roachpb.NodeID]*roachpb.NodeDescriptor
}

// Proxy is capable of providing information on each of the KV nodes in the
// cluster in the form of NodeDescriptors. This obviates the need for SQL-only
// tenant processes to join the cluster-wide gossip network.
var _ kvcoord.NodeDescStore = (*Proxy)(nil)

// Proxy is capable of providing Range addressing information in the form of
// RangeDescriptors through delegated RangeLookup requests. This is necessary
// because SQL-only tenants are restricted from reading Range Metadata keys
// directly. Instead, the RangeLookup requests are proxied through existing KV
// nodes while being subject to additional validation (e.g. is the Range being
// requested owned by the requesting tenant?).
var _ kvcoord.RangeDescriptorDB = (*Proxy)(nil)

// NewProxy creates a new Proxy.
func NewProxy(
	stopper *stop.Stopper, rpcContext *rpc.Context, rpcRetryOptions retry.Options, addrs []string,
) *Proxy {
	return &Proxy{
		stopper:         stopper,
		rpcContext:      rpcContext,
		rpcRetryOptions: rpcRetryOptions,
		addrs:           addrs,
		startupC:        make(chan struct{}),
	}
}

// Start launches the proxy's worker thread and waits for it to receive an
// initial NodeInfo response.
func (p *Proxy) Start(ctx context.Context) error {
	startupC := p.startupC
	p.stopper.RunWorker(context.Background(), func(ctx context.Context) {
		ctx = logtags.AddTag(ctx, "tenant-proxy", nil)
		ctx, cancel := p.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		p.runNodeInfoStream(ctx)
	})
	// Synchronously block until the first NodeInfo response completes.
	select {
	case <-startupC:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Proxy) runNodeInfoStream(ctx context.Context) {
	for ctx.Err() == nil {
		client, err := p.dial(ctx)
		if err != nil {
			log.Warningf(ctx, "error dialing tenant KV address: %v", err)
			continue
		}
		stream, err := client.NodeInfo(ctx, &roachpb.NodeInfoRequest{})
		if err != nil {
			log.Warningf(ctx, "error issuing NodeInfo RPC: %v", err)
			continue
		}
		err = p.consumeNodeInfoStream(stream)
		log.Warningf(ctx, "error consuming NodeInfo RPC: %v", err)
	}
}

// consumeNodeInfoStream consumes NodeInfoResponse notifications from a single
// gRPC stream. Returns when that stream is broken, either due to a connection
// failure or due to context cancellation.
func (p *Proxy) consumeNodeInfoStream(stream roachpb.Internal_NodeInfoClient) error {
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		p.consumeNodeInfoResp(resp)
	}
}

func (p *Proxy) consumeNodeInfoResp(resp *roachpb.NodeInfoResponse) {
	nodeDescs := make(map[roachpb.NodeID]*roachpb.NodeDescriptor, len(resp.Descriptors))
	for _, desc := range resp.Descriptors {
		nodeDescs[desc.NodeID] = desc
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nodeDescs = nodeDescs
	if p.startupC != nil {
		close(p.startupC)
		p.startupC = nil
	}
}

// GetNodeDescriptor implements the kvcoord.NodeDescStore interface.
func (p *Proxy) GetNodeDescriptor(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	desc, ok := p.nodeDescs[nodeID]
	if !ok {
		return nil, errors.Errorf("unable to look up descriptor for n%d", nodeID)
	}
	return desc, nil
}

// RangeLookup implements the kvcoord.RangeDescriptorDB interface.
func (p *Proxy) RangeLookup(
	ctx context.Context, key roachpb.RKey, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	// Proxy range lookup requests through the Internal service.
	client, err := p.dial(ctx)
	if err != nil {
		return nil, nil, err
	}
	resp, err := client.RangeLookup(ctx, &roachpb.RangeLookupRequest{
		Key: key,
		// We perform the range lookup scan with a READ_UNCOMMITTED consistency
		// level because we want the scan to return intents as well as committed
		// values. The reason for this is because it's not clear whether the
		// intent or the previous value points to the correct location of the
		// Range. It gets even more complicated when there are split-related
		// intents or a txn record co-located with a replica involved in the
		// split. Since we cannot know the correct answer, we lookup both the
		// pre- and post- transaction values.
		ReadConsistency: roachpb.READ_UNCOMMITTED,
		// Until we add protection in the Internal service implementation to
		// prevent prefetching from traversing into RangeDescriptors owned by
		// other tenants, we must disable prefetching.
		PrefetchNum:     0,
		PrefetchReverse: useReverseScan,
	})
	if err != nil {
		return nil, nil, err
	}
	return resp.Descriptors, resp.PrefetchedDescriptors, nil
}

// FirstRange implements the kvcoord.RangeDescriptorDB interface.
func (p *Proxy) FirstRange() (*roachpb.RangeDescriptor, error) {
	return nil, errors.New("kvtenant.Proxy does not have access to FirstRange")
}

func (p *Proxy) dial(ctx context.Context) (roachpb.InternalClient, error) {
	// TODO DURING REVIEW: I had originally written this using a nodedialer
	// instead of an rpc.Context and GRPCUnvalidatedDial directly. The desire to
	// use a nodedialer was twofold:
	//
	// 1. it has a nicer API (e.g. accepts an AddressResolver) and already knows
	// about InternalClients
	// 2. it has transparent breakers installed (but keyed on nodeID)
	//
	// In the end, this didn't seem like the right approach. Still, we'll
	// probably want to add breakers on each addr here... Or not. In practice we
	// expect the proxy to be pointed at a load balancer which will perform
	// internal health checks and prioritize healthy destinations, so maybe
	// that's not worth it.
	for r := retry.StartWithCtx(ctx, p.rpcRetryOptions); r.Next(); {
		// Try each address on each retry iteration.
		randStart := rand.Intn(len(p.addrs))
		for i := range p.addrs {
			addr := p.addrs[(i+randStart)%len(p.addrs)]
			conn, err := p.dialAddr(ctx, addr)
			if err == nil {
				return roachpb.NewInternalClient(conn), nil
			}
			log.Warningf(ctx, "error dialing tenant KV address %s: %v", addr, err)
		}
	}
	return nil, ctx.Err()
}

func (p *Proxy) dialAddr(ctx context.Context, addr string) (conn *grpc.ClientConn, err error) {
	if p.rpcDialTimeout == 0 {
		return p.rpcContext.GRPCUnvalidatedDial(addr).Connect(ctx)
	}
	err = contextutil.RunWithTimeout(ctx, "dial addr", p.rpcDialTimeout, func(ctx context.Context) error {
		conn, err = p.rpcContext.GRPCUnvalidatedDial(addr).Connect(ctx)
		return err
	})
	return conn, err
}
