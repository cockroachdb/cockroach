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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	kvtenant.Factory = connectorFactory{}
}

// Connector mediates the communication of cluster-wide state to sandboxed
// SQL-only tenant processes through a restricted interface.
//
// A Connector is instantiated inside a tenant's SQL process and is seeded with
// a set of one or more network addresses that reference existing KV nodes in
// the host cluster (or a load-balancer which fans out to some/all KV nodes). On
// startup, it establishes contact with one of these nodes to learn about the
// topology of the cluster and bootstrap the rest of SQL <-> KV network
// communication.
//
// The Connector communicates with the host cluster through the roachpb.Internal
// API.
//
// See below for the Connector's roles.
type Connector struct {
	log.AmbientContext

	tenantID        roachpb.TenantID
	rpcContext      *rpc.Context
	rpcRetryOptions retry.Options
	rpcDialTimeout  time.Duration // for testing
	rpcDial         singleflight.Group
	defaultZoneCfg  *zonepb.ZoneConfig
	addrs           []string

	mu struct {
		syncutil.RWMutex
		client               *client
		nodeDescs            map[roachpb.NodeID]*roachpb.NodeDescriptor
		systemConfig         *config.SystemConfig
		systemConfigChannels map[chan<- struct{}]struct{}
	}

	settingsMu struct {
		syncutil.Mutex

		allTenantOverrides map[string]settings.EncodedValue
		specificOverrides  map[string]settings.EncodedValue
		// notifyCh receives an event when there are changes to overrides.
		notifyCh chan struct{}
	}
}

// client represents an RPC client that proxies to a KV instance.
type client struct {
	roachpb.InternalClient
	serverpb.StatusClient
}

// Connector is capable of providing information on each of the KV nodes in the
// cluster in the form of NodeDescriptors. This obviates the need for SQL-only
// tenant processes to join the cluster-wide gossip network.
var _ kvcoord.NodeDescStore = (*Connector)(nil)

// Connector is capable of providing Range addressing information in the form of
// RangeDescriptors through delegated RangeLookup requests. This is necessary
// because SQL-only tenants are restricted from reading Range Metadata keys
// directly. Instead, the RangeLookup requests are proxied through existing KV
// nodes while being subject to additional validation (e.g. is the Range being
// requested owned by the requesting tenant?).
var _ rangecache.RangeDescriptorDB = (*Connector)(nil)

// Connector is capable of providing a filtered view of the SystemConfig
// containing only information applicable to secondary tenants. This obviates
// the need for SQL-only tenant processes to join the cluster-wide gossip
// network.
var _ config.SystemConfigProvider = (*Connector)(nil)

// Connector is capable of find the region of every node in the cluster.
// This is necessary for region validation for zone configurations and
// multi-region primitives.
var _ serverpb.RegionsServer = (*Connector)(nil)

// Connector is capable of finding debug information about the current
// tenant within the cluster. This is necessary for things such as
// debug zip and range reports.
var _ serverpb.TenantStatusServer = (*Connector)(nil)

// Connector is capable of accessing span configurations for secondary tenants.
var _ spanconfig.KVAccessor = (*Connector)(nil)

// NewConnector creates a new Connector.
// NOTE: Calling Start will set cfg.RPCContext.ClusterID.
func NewConnector(cfg kvtenant.ConnectorConfig, addrs []string) *Connector {
	cfg.AmbientCtx.AddLogTag("tenant-connector", nil)
	if cfg.TenantID.IsSystem() {
		panic("TenantID not set")
	}
	c := &Connector{
		tenantID:        cfg.TenantID,
		AmbientContext:  cfg.AmbientCtx,
		rpcContext:      cfg.RPCContext,
		rpcRetryOptions: cfg.RPCRetryOptions,
		defaultZoneCfg:  cfg.DefaultZoneConfig,
		addrs:           addrs,
	}

	c.mu.nodeDescs = make(map[roachpb.NodeID]*roachpb.NodeDescriptor)
	c.mu.systemConfigChannels = make(map[chan<- struct{}]struct{})
	c.settingsMu.allTenantOverrides = make(map[string]settings.EncodedValue)
	c.settingsMu.specificOverrides = make(map[string]settings.EncodedValue)
	return c
}

// connectorFactory implements kvtenant.ConnectorFactory.
type connectorFactory struct{}

func (connectorFactory) NewConnector(
	cfg kvtenant.ConnectorConfig, addrs []string,
) (kvtenant.Connector, error) {
	return NewConnector(cfg, addrs), nil
}

// Start launches the connector's worker thread and waits for it to successfully
// connect to a KV node. Start returns once the connector has determined the
// cluster's ID and set Connector.rpcContext.ClusterID.
func (c *Connector) Start(ctx context.Context) error {
	gossipStartupCh := make(chan struct{})
	settingsStartupCh := make(chan struct{})
	bgCtx := c.AnnotateCtx(context.Background())

	if err := c.rpcContext.Stopper.RunAsyncTask(bgCtx, "connector-gossip", func(ctx context.Context) {
		ctx = c.AnnotateCtx(ctx)
		ctx, cancel := c.rpcContext.Stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		c.runGossipSubscription(ctx, gossipStartupCh)
	}); err != nil {
		return err
	}

	if err := c.rpcContext.Stopper.RunAsyncTask(bgCtx, "connector-settings", func(ctx context.Context) {
		ctx = c.AnnotateCtx(ctx)
		ctx, cancel := c.rpcContext.Stopper.WithCancelOnQuiesce(ctx)
		defer cancel()
		c.runTenantSettingsSubscription(ctx, settingsStartupCh)
	}); err != nil {
		return err
	}

	// Block until we receive the first GossipSubscription event and the initial
	// setting overrides.
	for gossipStartupCh != nil || settingsStartupCh != nil {
		select {
		case <-gossipStartupCh:
			log.Infof(ctx, "kv connector gossip subscription started")
			gossipStartupCh = nil
		case <-settingsStartupCh:
			log.Infof(ctx, "kv connector tenant settings started")
			settingsStartupCh = nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// runGossipSubscription listens for gossip subscription events. It closes the
// given channel once the ClusterID gossip key has been handled.
// Exits when the context is done.
func (c *Connector) runGossipSubscription(ctx context.Context, startupCh chan struct{}) {
	for ctx.Err() == nil {
		client, err := c.getClient(ctx)
		if err != nil {
			continue
		}
		stream, err := client.GossipSubscription(ctx, &roachpb.GossipSubscriptionRequest{
			Patterns: gossipSubsPatterns,
		})
		if err != nil {
			log.Warningf(ctx, "error issuing GossipSubscription RPC: %v", err)
			c.tryForgetClient(ctx, client)
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
				c.tryForgetClient(ctx, client)
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
			handler(c, ctx, e.Key, e.Content)

			// Signal that startup is complete once the ClusterID gossip key has
			// been handled.
			if startupCh != nil && e.PatternMatched == gossip.KeyClusterID {
				close(startupCh)
				startupCh = nil
			}
		}
	}
}

var gossipSubsHandlers = map[string]func(*Connector, context.Context, string, roachpb.Value){
	// Subscribe to the ClusterID update.
	gossip.KeyClusterID: (*Connector).updateClusterID,
	// Subscribe to all *NodeDescriptor updates.
	gossip.MakePrefixPattern(gossip.KeyNodeIDPrefix): (*Connector).updateNodeAddress,
	// Subscribe to a filtered view of *SystemConfig updates.
	gossip.KeyDeprecatedSystemConfig: (*Connector).updateSystemConfig,
}

var gossipSubsPatterns = func() []string {
	patterns := make([]string, 0, len(gossipSubsHandlers))
	for pattern := range gossipSubsHandlers {
		patterns = append(patterns, pattern)
	}
	sort.Strings(patterns)
	return patterns
}()

// updateClusterID handles updates to the "ClusterID" gossip key, and sets the
// rpcContext so that it's available to other code running in the tenant.
func (c *Connector) updateClusterID(ctx context.Context, key string, content roachpb.Value) {
	bytes, err := content.GetBytes()
	if err != nil {
		log.Errorf(ctx, "invalid ClusterID value: %v", content.RawBytes)
		return
	}
	clusterID, err := uuid.FromBytes(bytes)
	if err != nil {
		log.Errorf(ctx, "invalid ClusterID value: %v", content.RawBytes)
		return
	}
	c.rpcContext.ClusterID.Set(ctx, clusterID)
}

// updateNodeAddress handles updates to "node" gossip keys, performing the
// corresponding update to the Connector's cached NodeDescriptor set.
func (c *Connector) updateNodeAddress(ctx context.Context, key string, content roachpb.Value) {
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
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.nodeDescs[desc.NodeID] = desc
}

// GetNodeDescriptor implements the kvcoord.NodeDescStore interface.
func (c *Connector) GetNodeDescriptor(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	desc, ok := c.mu.nodeDescs[nodeID]
	if !ok {
		return nil, errors.Errorf("unable to look up descriptor for n%d", nodeID)
	}
	return desc, nil
}

// updateSystemConfig handles updates to a filtered view of the "system-db"
// gossip key, performing the corresponding update to the Connector's cached
// SystemConfig.
func (c *Connector) updateSystemConfig(ctx context.Context, key string, content roachpb.Value) {
	cfg := config.NewSystemConfig(c.defaultZoneCfg)
	if err := content.GetProto(&cfg.SystemConfigEntries); err != nil {
		log.Errorf(ctx, "could not unmarshal system config: %v", err)
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.systemConfig = cfg
	for c := range c.mu.systemConfigChannels {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}

// GetSystemConfig implements the config.SystemConfigProvider interface.
func (c *Connector) GetSystemConfig() *config.SystemConfig {
	// TODO(nvanbenschoten): we need to wait in `(*Connector).Start()` until the
	// system config is populated. As is, there's a small chance that we return
	// nil, which SQL does not handle.
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.systemConfig
}

// RegisterSystemConfigChannel implements the config.SystemConfigProvider
// interface.
func (c *Connector) RegisterSystemConfigChannel() (_ <-chan struct{}, unregister func()) {
	// Create channel that receives new system config notifications. The channel
	// has a size of 1 to prevent connector from having to block on it.
	ch := make(chan struct{}, 1)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.systemConfigChannels[ch] = struct{}{}

	// Notify the channel right away if we have a config.
	if c.mu.systemConfig != nil {
		ch <- struct{}{}
	}
	return ch, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.mu.systemConfigChannels, ch)
	}
}

// RangeLookup implements the kvcoord.RangeDescriptorDB interface.
func (c *Connector) RangeLookup(
	ctx context.Context, key roachpb.RKey, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	// Proxy range lookup requests through the Internal service.
	ctx = c.AnnotateCtx(ctx)
	for ctx.Err() == nil {
		client, err := c.getClient(ctx)
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
			if grpcutil.IsAuthError(err) {
				// Authentication or authorization error. Propagate.
				return nil, nil, err
			}
			// Soft RPC error. Drop client and retry.
			c.tryForgetClient(ctx, client)
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

// Regions implements the serverpb.RegionsServer interface.
func (c *Connector) Regions(
	ctx context.Context, req *serverpb.RegionsRequest,
) (resp *serverpb.RegionsResponse, _ error) {
	if err := c.withClient(ctx, func(ctx context.Context, c *client) error {
		var err error
		resp, err = c.Regions(ctx, req)
		return err
	}); err != nil {
		return nil, err
	}

	return resp, nil
}

// TenantRanges implements the serverpb.TenantStatusServer interface
func (c *Connector) TenantRanges(
	ctx context.Context, req *serverpb.TenantRangesRequest,
) (resp *serverpb.TenantRangesResponse, _ error) {
	if err := c.withClient(ctx, func(ctx context.Context, c *client) error {
		var err error
		resp, err = c.TenantRanges(ctx, req)
		return err
	}); err != nil {
		return nil, err
	}

	return resp, nil
}

// FirstRange implements the kvcoord.RangeDescriptorDB interface.
func (c *Connector) FirstRange() (*roachpb.RangeDescriptor, error) {
	return nil, status.Error(codes.Unauthenticated, "kvtenant.Proxy does not have access to FirstRange")
}

// TokenBucket implements the kvtenant.TokenBucketProvider interface.
func (c *Connector) TokenBucket(
	ctx context.Context, in *roachpb.TokenBucketRequest,
) (*roachpb.TokenBucketResponse, error) {
	// Proxy token bucket requests through the Internal service.
	ctx = c.AnnotateCtx(ctx)
	for ctx.Err() == nil {
		client, err := c.getClient(ctx)
		if err != nil {
			continue
		}
		resp, err := client.TokenBucket(ctx, in)
		if err != nil {
			log.Warningf(ctx, "error issuing TokenBucket RPC: %v", err)
			if grpcutil.IsAuthError(err) {
				// Authentication or authorization error. Propagate.
				return nil, err
			}
			// Soft RPC error. Drop client and retry.
			c.tryForgetClient(ctx, client)
			continue
		}
		if resp.Error != (errorspb.EncodedError{}) {
			// Hard logical error. Propagate.
			return nil, errors.DecodeError(ctx, resp.Error)
		}
		return resp, nil
	}
	return nil, ctx.Err()
}

// GetSpanConfigRecords implements the spanconfig.KVAccessor interface.
func (c *Connector) GetSpanConfigRecords(
	ctx context.Context, targets []spanconfig.Target,
) (records []spanconfig.Record, _ error) {
	if err := c.withClient(ctx, func(ctx context.Context, c *client) error {
		resp, err := c.GetSpanConfigs(ctx, &roachpb.GetSpanConfigsRequest{
			Targets: spanconfig.TargetsToProtos(targets),
		})
		if err != nil {
			return err
		}

		records, err = spanconfig.EntriesToRecords(resp.SpanConfigEntries)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return records, nil
}

// UpdateSpanConfigRecords implements the spanconfig.KVAccessor
// interface.
func (c *Connector) UpdateSpanConfigRecords(
	ctx context.Context, toDelete []spanconfig.Target, toUpsert []spanconfig.Record,
) error {
	return c.withClient(ctx, func(ctx context.Context, c *client) error {
		_, err := c.UpdateSpanConfigs(ctx, &roachpb.UpdateSpanConfigsRequest{
			ToDelete: spanconfig.TargetsToProtos(toDelete),
			ToUpsert: spanconfig.RecordsToEntries(toUpsert),
		})
		return err
	})
}

// WithTxn implements the spanconfig.KVAccessor interface.
func (c *Connector) WithTxn(context.Context, *kv.Txn) spanconfig.KVAccessor {
	panic("not applicable")
}

// withClient is a convenience wrapper that executes the given closure while
// papering over InternalClient retrieval errors.
func (c *Connector) withClient(
	ctx context.Context, f func(ctx context.Context, c *client) error,
) error {
	ctx = c.AnnotateCtx(ctx)
	for ctx.Err() == nil {
		c, err := c.getClient(ctx)
		if err != nil {
			continue
		}
		return f(ctx, c)
	}
	return ctx.Err()
}

// getClient returns the singleton InternalClient if one is currently active. If
// not, the method attempts to dial one of the configured addresses. The method
// blocks until either a connection is successfully established or the provided
// context is canceled.
func (c *Connector) getClient(ctx context.Context) (*client, error) {
	c.mu.RLock()
	if client := c.mu.client; client != nil {
		c.mu.RUnlock()
		return client, nil
	}
	ch, _ := c.rpcDial.DoChan("dial", func() (interface{}, error) {
		dialCtx := c.AnnotateCtx(context.Background())
		dialCtx, cancel := c.rpcContext.Stopper.WithCancelOnQuiesce(dialCtx)
		defer cancel()
		var client *client
		err := c.rpcContext.Stopper.RunTaskWithErr(dialCtx, "kvtenant.Connector: dial",
			func(ctx context.Context) error {
				var err error
				client, err = c.dialAddrs(ctx)
				return err
			})
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.client = client
		return client, nil
	})
	c.mu.RUnlock()

	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*client), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// dialAddrs attempts to dial each of the configured addresses in a retry loop.
// The method will only return a non-nil error on context cancellation.
func (c *Connector) dialAddrs(ctx context.Context) (*client, error) {
	for r := retry.StartWithCtx(ctx, c.rpcRetryOptions); r.Next(); {
		// Try each address on each retry iteration (in random order).
		for _, i := range rand.Perm(len(c.addrs)) {
			addr := c.addrs[i]
			conn, err := c.dialAddr(ctx, addr)
			if err != nil {
				log.Warningf(ctx, "error dialing tenant KV address %s: %v", addr, err)
				continue
			}
			return &client{
				InternalClient: roachpb.NewInternalClient(conn),
				StatusClient:   serverpb.NewStatusClient(conn),
			}, nil
		}
	}
	return nil, ctx.Err()
}

func (c *Connector) dialAddr(ctx context.Context, addr string) (conn *grpc.ClientConn, err error) {
	if c.rpcDialTimeout == 0 {
		return c.rpcContext.GRPCUnvalidatedDial(addr).Connect(ctx)
	}
	err = contextutil.RunWithTimeout(ctx, "dial addr", c.rpcDialTimeout, func(ctx context.Context) error {
		conn, err = c.rpcContext.GRPCUnvalidatedDial(addr).Connect(ctx)
		return err
	})
	return conn, err
}

func (c *Connector) tryForgetClient(ctx context.Context, client roachpb.InternalClient) {
	if ctx.Err() != nil {
		// Error (may be) due to context. Don't forget client.
		return
	}
	// Compare-and-swap to avoid thrashing.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.client == client {
		c.mu.client = nil
	}
}
