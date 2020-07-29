// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package kvtenantccl provides utilities required by SQL-only tenant processes
// in order to interact with the key-value layer.
package kvtenantccl

import (
	"context"
	"io"
	"math/rand"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	kvtenant.Factory = proxyFactory{}
}

// Proxy mediates the communication of cluster-wide state to sandboxed SQL-only
// tenant processes through a restricted interface. A Proxy is seeded with a set
// of one or more network addresses that reference existing KV nodes in the
// cluster (or a load-balancer which fans out to some/all KV nodes). On startup,
// it establishes contact with one of these nodes to learn about the topology of
// the cluster and bootstrap the rest of SQL <-> KV network communication.
//
// See below for the Proxy's roles.
type Proxy struct {
	log.AmbientContext

	rpcContext      *rpc.Context
	rpcRetryOptions retry.Options
	rpcDialTimeout  time.Duration // for testing
	rpcDial         singleflight.Group
	defaultZoneCfg  *zonepb.ZoneConfig
	addrs           []string
	startupC        chan struct{}

	mu struct {
		syncutil.RWMutex
		client               roachpb.InternalClient
		nodeDescs            map[roachpb.NodeID]*roachpb.NodeDescriptor
		systemConfig         *config.SystemConfig
		systemConfigChannels []chan<- struct{}
	}
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

// Proxy is capable of providing a filtered view of the SystemConfig containing
// only information applicable to secondary tenants. This obviates the need for
// SQL-only tenant processes to join the cluster-wide gossip network.
var _ config.SystemConfigProvider = (*Proxy)(nil)

// NewProxy creates a new Proxy.
func NewProxy(cfg kvtenant.ProxyConfig, addrs []string) *Proxy {
	cfg.AmbientCtx.AddLogTag("tenant-proxy", nil)
	return &Proxy{
		AmbientContext:  cfg.AmbientCtx,
		rpcContext:      cfg.RPCContext,
		rpcRetryOptions: cfg.RPCRetryOptions,
		defaultZoneCfg:  cfg.DefaultZoneConfig,
		addrs:           addrs,
		startupC:        make(chan struct{}),
	}
}

// proxyFactory implements kvtenant.ProxyFactory.
type proxyFactory struct{}

func (proxyFactory) NewProxy(cfg kvtenant.ProxyConfig, addrs []string) (kvtenant.Proxy, error) {
	return NewProxy(cfg, addrs), nil
}

// Start launches the proxy's worker thread and waits for it to receive an
// initial GossipSubscription event.
func (p *Proxy) Start(ctx context.Context) error {
	startupC := p.startupC
	p.rpcContext.Stopper.RunWorker(context.Background(), func(ctx context.Context) {
		ctx = p.AnnotateCtx(ctx)
		ctx, cancel := p.rpcContext.Stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		p.runGossipSubscription(ctx)
	})
	// Synchronously block until the first GossipSubscription event.
	select {
	case <-startupC:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Proxy) runGossipSubscription(ctx context.Context) {
	for ctx.Err() == nil {
		client, err := p.getClient(ctx)
		if err != nil {
			continue
		}
		stream, err := client.GossipSubscription(ctx, &roachpb.GossipSubscriptionRequest{
			Patterns: gossipSubsPatterns,
		})
		if err != nil {
			log.Warningf(ctx, "error issuing GossipSubscription RPC: %v", err)
			p.tryForgetClient(ctx, client)
			continue
		}
		for {
			e, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				// Soft RPC error. Drop client and retry.
				log.Warningf(ctx, "error consuming GossipSubscription RPC: %v", err)
				p.tryForgetClient(ctx, client)
				break
			}
			if e.Error != nil {
				// Hard logical error. We expect io.EOF next.
				log.Errorf(ctx, "error consuming GossipSubscription RPC: %v", e.Error)
				continue
			}
			handler, ok := gossipSubsHandlers[e.PatternMatched]
			if !ok {
				log.Errorf(ctx, "unknown GossipSubscription pattern: %q", e.PatternMatched)
				continue
			}
			handler(p, ctx, e.Key, e.Content)
			if p.startupC != nil {
				close(p.startupC)
				p.startupC = nil
			}
		}
	}
}

var gossipSubsHandlers = map[string]func(*Proxy, context.Context, string, roachpb.Value){
	// Subscribe to all *NodeDescriptor updates.
	gossip.MakePrefixPattern(gossip.KeyNodeIDPrefix): (*Proxy).updateNodeAddress,
	// Subscribe to a filtered view of *SystemConfig updates.
	gossip.KeySystemConfig: (*Proxy).updateSystemConfig,
}

var gossipSubsPatterns = func() []string {
	patterns := make([]string, 0, len(gossipSubsHandlers))
	for pattern := range gossipSubsHandlers {
		patterns = append(patterns, pattern)
	}
	sort.Strings(patterns)
	return patterns
}()

// updateNodeAddress handles updates to "node" gossip keys, performing the
// corresponding update to the Proxy's cached NodeDescriptor set.
func (p *Proxy) updateNodeAddress(ctx context.Context, key string, content roachpb.Value) {
	desc := new(roachpb.NodeDescriptor)
	if err := content.GetProto(desc); err != nil {
		log.Errorf(ctx, "could not unmarshal node descriptor: %v", err)
		return
	}

	// TODO(nvanbenschoten): this doesn't handle NodeDescriptor removal from the
	// gossip network. As it turns out, neither does Gossip.updateNodeAddress.
	// There is some logic in Gossip.updateNodeAddress that attempts to remove
	// replaced network addresses, but that logic has been dead since 5bce267.
	// Other than that, gossip callbacks are not invoked on info expiration, so
	// nothing ever removes them from Gossip.nodeDescs. Fix this.
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.nodeDescs == nil {
		p.mu.nodeDescs = make(map[roachpb.NodeID]*roachpb.NodeDescriptor)
	}
	p.mu.nodeDescs[desc.NodeID] = desc
}

// GetNodeDescriptor implements the kvcoord.NodeDescStore interface.
func (p *Proxy) GetNodeDescriptor(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	desc, ok := p.mu.nodeDescs[nodeID]
	if !ok {
		return nil, errors.Errorf("unable to look up descriptor for n%d", nodeID)
	}
	return desc, nil
}

// updateSystemConfig handles updates to a filtered view of the "system-db"
// gossip key, performing the corresponding update to the Proxy's cached
// SystemConfig.
func (p *Proxy) updateSystemConfig(ctx context.Context, key string, content roachpb.Value) {
	cfg := config.NewSystemConfig(p.defaultZoneCfg)
	if err := content.GetProto(&cfg.SystemConfigEntries); err != nil {
		log.Errorf(ctx, "could not unmarshal system config: %v", err)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.systemConfig = cfg
	for _, c := range p.mu.systemConfigChannels {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

// GetSystemConfig implements the config.SystemConfigProvider interface.
func (p *Proxy) GetSystemConfig() *config.SystemConfig {
	// TODO(nvanbenschoten): we need to wait in `(*Proxy).Start()` until the
	// system config is populated. As is, there's a small chance that we return
	// nil, which SQL does not handle.
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.mu.systemConfig
}

// RegisterSystemConfigChannel implements the config.SystemConfigProvider
// interface.
func (p *Proxy) RegisterSystemConfigChannel() <-chan struct{} {
	// Create channel that receives new system config notifications.
	// The channel has a size of 1 to prevent proxy from having to block on it.
	c := make(chan struct{}, 1)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.systemConfigChannels = append(p.mu.systemConfigChannels, c)

	// Notify the channel right away if we have a config.
	if p.mu.systemConfig != nil {
		c <- struct{}{}
	}
	return c
}

// RangeLookup implements the kvcoord.RangeDescriptorDB interface.
func (p *Proxy) RangeLookup(
	ctx context.Context, key roachpb.RKey, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	// Proxy range lookup requests through the Internal service.
	ctx = p.AnnotateCtx(ctx)
	for ctx.Err() == nil {
		client, err := p.getClient(ctx)
		if err != nil {
			continue
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
			log.Warningf(ctx, "error issuing RangeLookup RPC: %v", err)
			if status.Code(err) == codes.Unauthenticated {
				// Authentication error. Propagate.
				return nil, nil, err
			}
			// Soft RPC error. Drop client and retry.
			p.tryForgetClient(ctx, client)
			continue
		}
		if resp.Error != nil {
			// Hard logical error. Propagate.
			return nil, nil, resp.Error.GoError()
		}
		return resp.Descriptors, resp.PrefetchedDescriptors, nil
	}
	return nil, nil, ctx.Err()
}

// FirstRange implements the kvcoord.RangeDescriptorDB interface.
func (p *Proxy) FirstRange() (*roachpb.RangeDescriptor, error) {
	return nil, errors.New("kvtenant.Proxy does not have access to FirstRange")
}

// getClient returns the singleton InternalClient if one is currently active. If
// not, the method attempts to dial one of the configured addresses. The method
// blocks until either a connection is successfully established or the provided
// context is canceled.
func (p *Proxy) getClient(ctx context.Context) (roachpb.InternalClient, error) {
	p.mu.RLock()
	if c := p.mu.client; c != nil {
		p.mu.RUnlock()
		return c, nil
	}
	ch, _ := p.rpcDial.DoChan("dial", func() (interface{}, error) {
		dialCtx := p.AnnotateCtx(context.Background())
		dialCtx, cancel := p.rpcContext.Stopper.WithCancelOnQuiesce(dialCtx)
		defer cancel()
		err := p.rpcContext.Stopper.RunTaskWithErr(dialCtx, "kvtenant.Proxy: dial", p.dialAddrs)
		if err != nil {
			return nil, err
		}
		// NB: read lock not needed.
		return p.mu.client, nil
	})
	p.mu.RUnlock()

	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(roachpb.InternalClient), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// dialAddrs attempts to dial each of the configured addresses in a retry loop.
// The method will only return a non-nil error on context cancellation.
func (p *Proxy) dialAddrs(ctx context.Context) error {
	for r := retry.StartWithCtx(ctx, p.rpcRetryOptions); r.Next(); {
		// Try each address on each retry iteration.
		randStart := rand.Intn(len(p.addrs))
		for i := range p.addrs {
			addr := p.addrs[(i+randStart)%len(p.addrs)]
			conn, err := p.dialAddr(ctx, addr)
			if err != nil {
				log.Warningf(ctx, "error dialing tenant KV address %s: %v", addr, err)
				continue
			}
			client := roachpb.NewInternalClient(conn)
			p.mu.Lock()
			p.mu.client = client
			p.mu.Unlock()
			return nil
		}
	}
	return ctx.Err()
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

func (p *Proxy) tryForgetClient(ctx context.Context, c roachpb.InternalClient) {
	if ctx.Err() != nil {
		// Error (may be) due to context. Don't forget client.
		return
	}
	// Compare-and-swap to avoid thrashing.
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mu.client == c {
		p.mu.client = nil
	}
}
