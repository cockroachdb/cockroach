// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtenant

import (
	"context"
	"io"
	"math/rand"
	"net"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfo"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func init() {
	Factory = connectorFactory{}
}

// Connector mediates the communication of cluster-wide state to sandboxed
// SQL-only tenant servers through a restricted interface. A Connector is
// seeded with a set of one or more network addresses that reference existing
// KV nodes in the cluster (or a load-balancer which fans out to some/all KV
// nodes). On startup, it establishes contact with one of these nodes to learn
// about the topology of the cluster and bootstrap the rest of SQL <-> KV
// network communication.
type Connector interface {
	// Start starts the connector.
	Start(context.Context) error

	// TenantInfo retrieves current metadata about the tenant record and
	// an update channel to track changes
	TenantInfo() (tenantcapabilities.Entry, <-chan struct{})

	// ReadFromTenantInfoAccessor allows retrieving the other tenant, if any, from
	// which the calling tenant should configure itself to read, along with the
	// latest timestamp at which it should perform such reads at this time.
	mtinfo.ReadFromTenantInfoAccessor

	// NodeDescStore provides information on each of the KV nodes in the cluster
	// in the form of NodeDescriptors and StoreDescriptors. This obviates the
	// need for SQL-only tenant servers to join the cluster-wide gossip
	// network.
	kvclient.NodeDescStore

	// RangeDescriptorDB provides range addressing information in the form of
	// RangeDescriptors through delegated RangeLookup requests. This is
	// necessary because SQL-only tenants are restricted from reading Range
	// Metadata keys directly. Instead, the RangeLookup requests are proxied
	// through existing KV nodes while being subject to additional validation
	// (e.g. is the Range being requested owned by the requesting tenant?).
	rangecache.RangeDescriptorDB

	// IteratorFactory allows secondary tenants to access Range Metadata in the
	// form of iterators that return RangeDescriptors. Iterators are constructed
	// through delegated GetRangeDescriptors requests; the rationale behind
	// proxying requests is similar to the RangeDescriptorDB interface -- doing so
	// ensures SQL-only tenants are not able to access Range Metadata for Ranges
	// not owned by the requesting tenant.
	rangedesc.IteratorFactory

	// TenantStatusServer is the subset of the serverpb.StatusInterface that is
	// used by the SQL system to query for debug information, such as tenant-specific
	// range reports.
	serverpb.TenantStatusServer

	// TenantAdminServer is the subset of the serverpb.AdminInterface that is
	// used by the SQL system to query for debug information, such as cluster-wide
	// observability.
	serverpb.TenantAdminServer

	// TenantTimeSeriesServer is the subset of the tspb.TimeSeriesServer that is
	// used by the SQL system to query for timeseries data.
	tspb.TenantTimeSeriesServer

	// TokenBucketProvider provides access to the tenant cost control token
	// bucket.
	TokenBucketProvider

	// KVAccessor provides access to the subset of the cluster's span configs
	// applicable to secondary tenants.
	spanconfig.KVAccessor

	// Reporter provides access to conformance reports, i.e. whether ranges
	// backing queried keyspans conform the span configs that apply to them.
	spanconfig.Reporter

	// OverridesMonitor provides access to tenant cluster setting overrides.
	settingswatcher.OverridesMonitor

	// SystemConfigProvider provides access to basic host-tenant controlled
	// information regarding tenant zone configs. This is critical for the
	// mixed version 21.2->22.1 state where the tenant has not yet configured
	// its own zones.
	config.SystemConfigProvider

	// GetClusterInitGracePeriodTS will return the timestamp used to signal the
	// end of the grace period for clusters with a license. The timestamp is
	// represented as the number of seconds since the Unix epoch.
	GetClusterInitGracePeriodTS() int64
}

// TokenBucketProvider supplies an endpoint (to tenants) for the TokenBucket API
// (defined in roachpb.Internal), used to interact with the tenant cost control
// token bucket.
type TokenBucketProvider interface {
	TokenBucket(
		ctx context.Context, in *kvpb.TokenBucketRequest,
	) (*kvpb.TokenBucketResponse, error)
}

// connector mediates the communication of cluster-wide state to sandboxed
// SQL-only tenant servers through a restricted interface.
//
// A connector is instantiated inside a tenant's SQL server and is seeded with a
// set of one or more network addresses that reference existing KV nodes in the
// storage cluster (or a load-balancer which fans out to some/all KV nodes). On
// startup, it establishes contact with one of these nodes to learn about the
// topology of the cluster and bootstrap the rest of SQL <-> KV network
// communication.
//
// The connector communicates with the storage cluster through the
// roachpb.Internal API.
//
// See below for the connector's roles.
type connector struct {
	log.AmbientContext

	tenantID        roachpb.TenantID
	rpcContext      *rpc.Context
	rpcRetryOptions retry.Options
	rpcDialTimeout  time.Duration // for testing
	rpcDial         *singleflight.Group
	defaultZoneCfg  *zonepb.ZoneConfig
	addrs           []string

	earlyShutdownIfMissingTenantRecord bool

	startCh  chan struct{} // closed when connector has started up
	startErr error

	mu struct {
		syncutil.RWMutex
		client               *client
		nodeDescs            map[roachpb.NodeID]*roachpb.NodeDescriptor
		storeDescs           map[roachpb.StoreID]*roachpb.StoreDescriptor
		systemConfig         *config.SystemConfig
		systemConfigChannels map[chan<- struct{}]struct{}
	}

	settingsMu struct {
		syncutil.Mutex

		// receivedFirstAllTenantOverrides is set to true when the first batch of
		// all-tenant overrides has been received.
		receivedFirstAllTenantOverrides bool
		allTenantOverrides              map[settings.InternalKey]settings.EncodedValue

		// receivedFirstSpecificOverrides is set to true when the first batch of
		// tenant-specific overrides has been received.
		receivedFirstSpecificOverrides bool
		specificOverrides              map[settings.InternalKey]settings.EncodedValue

		// notifyCh is closed when there are changes to overrides.
		notifyCh chan struct{}
	}

	// testingEmulateOldVersionSettingsClient is set to true when the
	// connector should emulate the version where it processed all
	// events as settings events. Used only for testing.
	testingEmulateOldVersionSettingsClient bool

	metadataMu struct {
		syncutil.Mutex

		// receivedFirstMetadata is set to true when the first batch of
		// metadata bits has been received.
		receivedFirstMetadata bool

		tenantName               roachpb.TenantName
		dataState                mtinfopb.TenantDataState
		serviceMode              mtinfopb.TenantServiceMode
		capabilities             *tenantcapabilitiespb.TenantCapabilities
		clusterInitGracePeriodTS int64

		// notifyCh is closed when there are changes to the metadata.
		notifyCh chan struct{}
	}
}

// client represents an RPC client that proxies to a KV instance.
type client struct {
	kvpb.InternalClient
	serverpb.StatusClient
	serverpb.AdminClient
	tspb.TimeSeriesClient
}

// connector is capable of providing information on each of the KV nodes in the
// cluster in the form of NodeDescriptors. This obviates the need for SQL-only
// tenant servers to join the cluster-wide gossip network.
var _ kvclient.NodeDescStore = (*connector)(nil)

// connector is capable of providing Range addressing information in the form of
// RangeDescriptors through delegated RangeLookup requests. This is necessary
// because SQL-only tenants are restricted from reading Range Metadata keys
// directly. Instead, the RangeLookup requests are proxied through existing KV
// nodes while being subject to additional validation (e.g. is the Range being
// requested owned by the requesting tenant?).
var _ rangecache.RangeDescriptorDB = (*connector)(nil)

// connector is capable of providing a filtered view of the SystemConfig
// containing only information applicable to secondary tenants. This obviates
// the need for SQL-only tenant servers to join the cluster-wide gossip
// network.
var _ config.SystemConfigProvider = (*connector)(nil)

// connector is capable of finding debug information about the current
// tenant within the cluster. This is necessary for things such as
// debug zip and range reports.
var _ serverpb.TenantStatusServer = (*connector)(nil)

// connector is capable of finding debug information about the cluster
// the tenant belongs to. This is necessary for proper functioning of
// the DB Console in cases where the tenant has privileges allowing it
// to access system-level information.
var _ serverpb.TenantAdminServer = (*connector)(nil)
var _ tspb.TenantTimeSeriesServer = (*connector)(nil)

// connector is capable of accessing span configurations for secondary tenants.
var _ spanconfig.KVAccessor = (*connector)(nil)

// Reporter is capable of generating span configuration conformance reports for
// secondary tenants.
var _ spanconfig.Reporter = (*connector)(nil)

// NewConnector creates a new connector.
// NOTE: Calling Start will set cfg.RPCContext.ClusterID.
func NewConnector(cfg ConnectorConfig, addrs []string) Connector {
	cfg.AmbientCtx.AddLogTag("tenant-connector", nil)
	if cfg.TenantID.IsSystem() {
		panic("TenantID not set")
	}
	c := &connector{
		tenantID:        cfg.TenantID,
		AmbientContext:  cfg.AmbientCtx,
		rpcContext:      cfg.RPCContext,
		rpcDial:         singleflight.NewGroup("dial tenant connector", singleflight.NoTags),
		rpcRetryOptions: cfg.RPCRetryOptions,
		defaultZoneCfg:  cfg.DefaultZoneConfig,
		addrs:           addrs,

		earlyShutdownIfMissingTenantRecord: cfg.ShutdownTenantConnectorEarlyIfNoRecordPresent,
	}

	c.mu.nodeDescs = make(map[roachpb.NodeID]*roachpb.NodeDescriptor)
	c.mu.storeDescs = make(map[roachpb.StoreID]*roachpb.StoreDescriptor)
	c.mu.systemConfigChannels = make(map[chan<- struct{}]struct{})
	c.settingsMu.allTenantOverrides = make(map[settings.InternalKey]settings.EncodedValue)
	c.settingsMu.specificOverrides = make(map[settings.InternalKey]settings.EncodedValue)
	c.settingsMu.notifyCh = make(chan struct{})
	c.metadataMu.notifyCh = make(chan struct{})
	return c
}

// connectorFactory implements kvtenant.ConnectorFactory.
type connectorFactory struct{}

// NewConnector creates a new loopback only tenant connector with the given
// configuration. A loopback address is required, and an error is returned if
// one is not provided. If any remote addresses are provided, an error is
// returned.
func (connectorFactory) NewConnector(
	cfg ConnectorConfig, addressConfig KVAddressConfig,
) (Connector, error) {
	if addressConfig.LoopbackAddress == "" || len(addressConfig.RemoteAddresses) > 0 {
		return nil, pgerror.WithCandidateCode(
			errors.New(`tenant connector with remote KV addresses requires a CCL binary`),
			pgcode.CCLRequired)
	}
	return NewConnector(cfg, []string{addressConfig.LoopbackAddress}), nil
}

// WaitForStart waits until the connector has started.
func (c *connector) WaitForStart(ctx context.Context) error {
	// Fast path check.
	select {
	case <-c.startCh:
		return c.startErr
	default:
	}
	if c.startCh == nil {
		return errors.AssertionFailedf("Start() was not yet called")
	}
	select {
	case <-c.startCh:
		return c.startErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Start launches the connector's worker thread and waits for it to successfully
// connect to a KV node. Start returns once the connector has determined the
// cluster's ID and set connector.rpcContext.ClusterID.
func (c *connector) Start(ctx context.Context) error {
	c.startCh = make(chan struct{})
	c.startErr = c.internalStart(ctx)
	close(c.startCh)
	return c.startErr
}

func (c *connector) internalStart(ctx context.Context) error {
	gossipStartupCh := make(chan struct{})
	settingsStartupCh := make(chan error)
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
		case err := <-settingsStartupCh:
			settingsStartupCh = nil
			if err != nil {
				log.Infof(ctx, "kv connector initialization error: %v", err)
				return err
			}
			log.Infof(ctx, "kv connector tenant settings started")
		case <-ctx.Done():
			return ctx.Err()
		case <-c.rpcContext.Stopper.ShouldQuiesce():
			log.Infof(ctx, "kv connector asked to shut down before full start")
			return errors.New("request to shut down early")
		}
	}
	return nil
}

// runGossipSubscription listens for gossip subscription events. It closes the
// given channel once the ClusterID gossip key has been handled.
// Exits when the context is done.
func (c *connector) runGossipSubscription(ctx context.Context, startupCh chan struct{}) {
	for ctx.Err() == nil {
		client, err := c.getClient(ctx)
		if err != nil {
			continue
		}
		stream, err := client.GossipSubscription(ctx, &kvpb.GossipSubscriptionRequest{
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

var gossipSubsHandlers = map[string]func(*connector, context.Context, string, roachpb.Value){
	// Subscribe to the ClusterID update.
	gossip.KeyClusterID: (*connector).updateClusterID,
	// Subscribe to all *NodeDescriptor updates.
	gossip.MakePrefixPattern(gossip.KeyNodeDescPrefix): (*connector).updateNodeAddress,
	// Subscribe to all *StoreDescriptor updates.
	gossip.MakePrefixPattern(gossip.KeyStoreDescPrefix): (*connector).updateStoreMap,
	// Subscribe to a filtered view of *SystemConfig updates.
	gossip.KeyDeprecatedSystemConfig: (*connector).updateSystemConfig,
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
func (c *connector) updateClusterID(ctx context.Context, key string, content roachpb.Value) {
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
	c.rpcContext.StorageClusterID.Set(ctx, clusterID)
}

// updateNodeAddress handles updates to "node" gossip keys, performing the
// corresponding update to the connector's cached NodeDescriptor set.
func (c *connector) updateNodeAddress(ctx context.Context, key string, content roachpb.Value) {
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

// updateStoreMap handles updates to "store" gossip keys, performing the
// corresponding update to the connector's cached StoreDescriptor set.
func (c *connector) updateStoreMap(ctx context.Context, key string, content roachpb.Value) {
	desc := new(roachpb.StoreDescriptor)
	if err := content.GetProto(desc); err != nil {
		log.Errorf(ctx, "could not unmarshal store descriptor: %v", err)
		return
	}

	// TODO(nvanbenschoten): this doesn't handle StoreDescriptor removal from the
	// gossip network. See comment in updateNodeAddress.
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.storeDescs[desc.StoreID] = desc
}

// GetNodeDescriptor implements the kvclient.NodeDescStore interface.
func (c *connector) GetNodeDescriptor(nodeID roachpb.NodeID) (*roachpb.NodeDescriptor, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	desc, ok := c.mu.nodeDescs[nodeID]
	if !ok {
		return nil, kvpb.NewNodeDescNotFoundError(nodeID)
	}
	return desc, nil
}

// GetNodeDescriptorCount implements the kvclient.NodeDescStore interface.
func (c *connector) GetNodeDescriptorCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.mu.nodeDescs)
}

// GetStoreDescriptor implements the kvclient.NodeDescStore interface.
func (c *connector) GetStoreDescriptor(storeID roachpb.StoreID) (*roachpb.StoreDescriptor, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	desc, ok := c.mu.storeDescs[storeID]
	if !ok {
		return nil, kvpb.NewStoreDescNotFoundError(storeID)
	}
	return desc, nil
}

// updateSystemConfig handles updates to a filtered view of the "system-db"
// gossip key, performing the corresponding update to the connector's cached
// SystemConfig.
func (c *connector) updateSystemConfig(ctx context.Context, key string, content roachpb.Value) {
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
func (c *connector) GetSystemConfig() *config.SystemConfig {
	// TODO(nvanbenschoten): we need to wait in `(*connector).Start()` until the
	// system config is populated. As is, there's a small chance that we return
	// nil, which SQL does not handle.
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.systemConfig
}

// RegisterSystemConfigChannel implements the config.SystemConfigProvider
// interface.
func (c *connector) RegisterSystemConfigChannel() (_ <-chan struct{}, unregister func()) {
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
func (c *connector) RangeLookup(
	ctx context.Context, key roachpb.RKey, rc rangecache.RangeLookupConsistency, useReverseScan bool,
) ([]roachpb.RangeDescriptor, []roachpb.RangeDescriptor, error) {
	// Proxy range lookup requests through the Internal service.
	ctx = c.AnnotateCtx(ctx)
	for ctx.Err() == nil {
		client, err := c.getClient(ctx)
		if err != nil {
			continue
		}
		resp, err := client.RangeLookup(ctx, &kvpb.RangeLookupRequest{
			Key: key,
			// See the comment on (*kvcoord.DistSender).RangeLookup or kv.RangeLookup
			// for more discussion on the choice of ReadConsistency and its
			// implications.
			ReadConsistency: rc,
			PrefetchNum:     kvcoord.RangeLookupPrefetchCount,
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
	return nil, nil, errors.Wrap(ctx.Err(), "range lookup")
}

// Nodes implements the serverpb.TenantStatusServer interface
func (c *connector) Nodes(
	ctx context.Context, req *serverpb.NodesRequest,
) (resp *serverpb.NodesResponse, retErr error) {
	retErr = c.withClient(ctx, func(ctx context.Context, client *client) (err error) {
		resp, err = client.Nodes(ctx, req)
		return
	})
	return
}

// Regions implements the serverpb.TenantStatusServer interface
func (c *connector) Regions(
	ctx context.Context, req *serverpb.RegionsRequest,
) (resp *serverpb.RegionsResponse, retErr error) {
	retErr = c.withClient(ctx, func(ctx context.Context, client *client) (err error) {
		resp, err = client.Regions(ctx, req)
		return
	})
	return
}

// Ranges implements the serverpb.TenantStatusServer interface
func (c *connector) Ranges(
	ctx context.Context, req *serverpb.RangesRequest,
) (resp *serverpb.RangesResponse, retErr error) {
	retErr = c.withClient(ctx, func(ctx context.Context, client *client) (err error) {
		resp, err = client.Ranges(ctx, req)
		return
	})
	return
}

// TenantRanges implements the serverpb.TenantStatusServer interface
func (c *connector) TenantRanges(
	ctx context.Context, req *serverpb.TenantRangesRequest,
) (resp *serverpb.TenantRangesResponse, retErr error) {
	retErr = c.withClient(ctx, func(ctx context.Context, client *client) (err error) {
		resp, err = client.TenantRanges(ctx, req)
		return
	})
	return
}

// NetworkConnectivity implements the serverpb.TenantStatusServer interface
func (c *connector) NetworkConnectivity(
	ctx context.Context, req *serverpb.NetworkConnectivityRequest,
) (resp *serverpb.NetworkConnectivityResponse, retErr error) {
	retErr = c.withClient(ctx, func(ctx context.Context, client *client) (err error) {
		resp, err = client.NetworkConnectivity(ctx, req)
		return
	})
	return
}

// Gossip implements the serverpb.TenantStatusServer interface
func (c *connector) Gossip(
	ctx context.Context, req *serverpb.GossipRequest,
) (resp *gossip.InfoStatus, retErr error) {
	retErr = c.withClient(ctx, func(ctx context.Context, client *client) (err error) {
		resp, err = client.Gossip(ctx, req)
		return
	})
	return
}

// NewIterator implements the rangedesc.IteratorFactory interface.
func (c *connector) NewIterator(
	ctx context.Context, span roachpb.Span,
) (rangedesc.Iterator, error) {
	rangeDescriptors, err := c.getRangeDescs(ctx, span, 0)
	return rangedesc.NewSliceIterator(rangeDescriptors), err
}

func (c *connector) getRangeDescs(
	ctx context.Context, span roachpb.Span, pageSize int,
) ([]roachpb.RangeDescriptor, error) {
	var rangeDescriptors []roachpb.RangeDescriptor

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for ctx.Err() == nil {
		rangeDescriptors = rangeDescriptors[:0] // clear out.
		client, err := c.getClient(ctx)
		if err != nil {
			continue
		}
		stream, err := client.GetRangeDescriptors(streamCtx, &kvpb.GetRangeDescriptorsRequest{
			Span: span, BatchSize: int64(pageSize),
		})
		if err != nil {
			// TODO(arul): We probably don't want to treat all errors here as "soft".
			// for example, it doesn't make much sense to retry the request if it fails
			// the keybounds check.
			// Soft RPC error. Drop client and retry.
			log.Warningf(ctx, "error issuing GetRangeDescriptors RPC: %v", err)
			c.tryForgetClient(ctx, client)
			continue
		}

		for ctx.Err() == nil {
			e, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return rangeDescriptors, nil
				}
				log.Warningf(ctx, "error consuming GetRangeDescriptors RPC: %v", err)
				if grpcutil.IsAuthError(err) {
					// Authentication or authorization error. Propagate.
					return nil, err
				}
				// Soft RPC error. Drop client and retry.
				c.tryForgetClient(ctx, client)
				break
			}
			rangeDescriptors = append(rangeDescriptors, e.RangeDescriptors...)
			if pageSize != 0 && len(rangeDescriptors) >= pageSize {
				if err := stream.CloseSend(); err != nil {
					return nil, err
				}
				cancel()
				return rangeDescriptors, nil
			}
		}
	}
	return nil, errors.Wrap(ctx.Err(), "new iterator")
}

// NewLazyIterator implements the IteratorFactory interface.
func (i *connector) NewLazyIterator(
	ctx context.Context, span roachpb.Span, pageSize int,
) (rangedesc.LazyIterator, error) {
	return rangedesc.NewPaginatedIter(ctx, span, pageSize, i.getRangeDescs)
}

// TokenBucket implements the kvtenant.TokenBucketProvider interface.
func (c *connector) TokenBucket(
	ctx context.Context, in *kvpb.TokenBucketRequest,
) (*kvpb.TokenBucketResponse, error) {
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
	return nil, errors.Wrap(ctx.Err(), "token bucket")
}

// GetSpanConfigRecords implements the spanconfig.KVAccessor interface.
func (c *connector) GetSpanConfigRecords(
	ctx context.Context, targets []spanconfig.Target,
) (records []spanconfig.Record, _ error) {
	if err := c.withClient(ctx, func(ctx context.Context, c *client) error {
		resp, err := c.GetSpanConfigs(ctx, &roachpb.GetSpanConfigsRequest{
			Targets: spanconfig.TargetsToProtos(targets),
		})
		if err != nil {
			return errors.Wrap(err, "get span configs error")
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
func (c *connector) UpdateSpanConfigRecords(
	ctx context.Context,
	toDelete []spanconfig.Target,
	toUpsert []spanconfig.Record,
	minCommitTS, maxCommitTS hlc.Timestamp,
) error {
	return c.withClient(ctx, func(ctx context.Context, c *client) error {
		resp, err := c.UpdateSpanConfigs(ctx, &roachpb.UpdateSpanConfigsRequest{
			ToDelete:           spanconfig.TargetsToProtos(toDelete),
			ToUpsert:           spanconfig.RecordsToEntries(toUpsert),
			MinCommitTimestamp: minCommitTS,
			MaxCommitTimestamp: maxCommitTS,
		})
		if err != nil {
			return errors.Wrap(err, "update span configs error")
		}
		if resp.Error.IsSet() {
			// Logical error; propagate as such.
			return errors.DecodeError(ctx, resp.Error)
		}
		return nil
	})
}

// SpanConfigConformance implements the spanconfig.Reporter interface.
func (c *connector) SpanConfigConformance(
	ctx context.Context, spans []roachpb.Span,
) (roachpb.SpanConfigConformanceReport, error) {
	var report roachpb.SpanConfigConformanceReport
	if err := c.withClient(ctx, func(ctx context.Context, c *client) error {
		resp, err := c.SpanConfigConformance(ctx, &roachpb.SpanConfigConformanceRequest{
			Spans: spans,
		})
		if err != nil {
			return err
		}

		report = resp.Report
		return nil
	}); err != nil {
		return roachpb.SpanConfigConformanceReport{}, err
	}
	return report, nil
}

// SpanStats implements the serverpb.TenantStatusServer interface.
func (c *connector) SpanStats(
	ctx context.Context, req *roachpb.SpanStatsRequest,
) (*roachpb.SpanStatsResponse, error) {
	var response *roachpb.SpanStatsResponse
	err := c.withClient(ctx, func(ctx context.Context, c *client) error {
		stats, err := c.SpanStats(ctx, req)
		if err != nil {
			return err
		}
		response = stats
		return nil
	})
	return response, err
}

// GetAllSystemSpanConfigsThatApply implements the spanconfig.KVAccessor
// interface.
func (c *connector) GetAllSystemSpanConfigsThatApply(
	ctx context.Context, id roachpb.TenantID,
) ([]roachpb.SpanConfig, error) {
	var spanConfigs []roachpb.SpanConfig
	if err := c.withClient(ctx, func(ctx context.Context, c *client) error {
		resp, err := c.GetAllSystemSpanConfigsThatApply(
			ctx, &roachpb.GetAllSystemSpanConfigsThatApplyRequest{
				TenantID: id,
			})
		if err != nil {
			return errors.Wrap(err, "get all system span configs that apply error")
		}

		spanConfigs = resp.SpanConfigs
		return nil
	}); err != nil {
		return nil, err
	}
	return spanConfigs, nil
}

// HotRangesV2 implements the serverpb.HotRangesV2 interface
func (c *connector) HotRangesV2(
	ctx context.Context, req *serverpb.HotRangesRequest,
) (*serverpb.HotRangesResponseV2, error) {
	var resp *serverpb.HotRangesResponseV2
	r := *req
	// Force to assign tenant ID in request to be the same as requested tenant
	if len(req.TenantID) == 0 {
		r.TenantID = c.tenantID.String()
		log.Warningf(ctx, "tenant ID is set to %s", c.tenantID)
	} else if c.tenantID.String() != req.TenantID {
		return nil, status.Error(codes.PermissionDenied, "cannot request hot ranges for another tenant")
	}
	if err := c.withClient(ctx, func(ctx context.Context, c *client) error {
		var err error
		resp, err = c.HotRangesV2(ctx, &r)
		return err
	}); err != nil {
		return nil, err
	}
	return resp, nil
}

// DownloadSpan implements the serverpb.TenantStatusServer interface
func (c *connector) DownloadSpan(
	ctx context.Context, req *serverpb.DownloadSpanRequest,
) (*serverpb.DownloadSpanResponse, error) {
	if !c.tenantID.IsSystem() {
		tSpan := keys.MakeTenantSpan(c.tenantID)
		for i := range req.Spans {
			if !tSpan.Contains(req.Spans[i]) {
				return nil, status.Errorf(codes.PermissionDenied, "only the system tenant can issue download span requests for another tenant")
			}
		}
	}
	var resp *serverpb.DownloadSpanResponse
	if err := c.withClient(ctx, func(ctx context.Context, c *client) error {
		var err error
		resp, err = c.DownloadSpan(ctx, req)
		return err
	}); err != nil {
		return nil, err
	}
	return resp, nil
}

// WithTxn implements the spanconfig.KVAccessor interface.
func (c *connector) WithTxn(context.Context, *kv.Txn) spanconfig.KVAccessor {
	panic("not applicable")
}

// WithISQLTxn is part of the spanconfig.KVAccessor interface.
func (c *connector) WithISQLTxn(context.Context, isql.Txn) spanconfig.KVAccessor {
	panic("not applicable")
}

// withClient is a convenience wrapper that executes the given closure while
// papering over InternalClient retrieval errors.
func (c *connector) withClient(
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
	return errors.Wrap(ctx.Err(), "with client")
}

// getClient returns the singleton InternalClient if one is currently active. If
// not, the method attempts to dial one of the configured addresses. The method
// blocks until either a connection is successfully established or the provided
// context is canceled.
func (c *connector) getClient(ctx context.Context) (*client, error) {
	ctx = c.AnnotateCtx(ctx)
	c.mu.RLock()
	if client := c.mu.client; client != nil {
		c.mu.RUnlock()
		return client, nil
	}
	future, _ := c.rpcDial.DoChan(ctx,
		"dial",
		singleflight.DoOpts{
			Stop:               c.rpcContext.Stopper,
			InheritCancelation: false,
		},
		func(ctx context.Context) (interface{}, error) {
			var client *client
			err := c.rpcContext.Stopper.RunTaskWithErr(ctx, "kvtenant.connector: dial",
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

	res := future.WaitForResult(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	return res.Val.(*client), nil
}

// dialAddrs attempts to dial each of the configured addresses in a retry loop.
// The method will only return a non-nil error on context cancellation.
func (c *connector) dialAddrs(ctx context.Context) (*client, error) {
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
				InternalClient:   kvpb.NewInternalClient(conn),
				StatusClient:     serverpb.NewStatusClient(conn),
				AdminClient:      serverpb.NewAdminClient(conn),
				TimeSeriesClient: tspb.NewTimeSeriesClient(conn),
			}, nil
		}
	}
	return nil, errors.Wrap(ctx.Err(), "dial addrs")
}

func (c *connector) dialAddr(ctx context.Context, addr string) (conn *grpc.ClientConn, err error) {
	if c.rpcDialTimeout == 0 {
		return c.rpcContext.GRPCUnvalidatedDial(addr, roachpb.Locality{}).Connect(ctx)
	}
	err = timeutil.RunWithTimeout(ctx, "dial addr", c.rpcDialTimeout, func(ctx context.Context) error {
		conn, err = c.rpcContext.GRPCUnvalidatedDial(addr, roachpb.Locality{}).Connect(ctx)
		return err
	})
	return conn, err
}

func (c *connector) tryForgetClient(ctx context.Context, client kvpb.InternalClient) {
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

// Liveness implements the serverpb.TenantAdminServer interface
func (c *connector) Liveness(
	ctx context.Context, req *serverpb.LivenessRequest,
) (resp *serverpb.LivenessResponse, retErr error) {
	retErr = c.withClient(ctx, func(ctx context.Context, client *client) (err error) {
		resp, err = client.Liveness(ctx, req)
		return
	})
	return
}

// Query implements the serverpb.TenantTimeSeriesServer interface
func (c *connector) Query(
	ctx context.Context, req *tspb.TimeSeriesQueryRequest,
) (resp *tspb.TimeSeriesQueryResponse, retErr error) {
	retErr = c.withClient(ctx, func(ctx context.Context, client *client) (err error) {
		resp, err = client.Query(ctx, req)
		return
	})
	return
}

// AddressResolver wraps a NodeDescStore interface in an adapter that allows it
// be used as a nodedialer.AddressResolver. Addresses are resolved to a node's
// address.
func AddressResolver(s kvclient.NodeDescStore) nodedialer.AddressResolver {
	return func(nodeID roachpb.NodeID) (net.Addr, roachpb.Locality, error) {
		nd, err := s.GetNodeDescriptor(nodeID)
		if err != nil {
			return nil, roachpb.Locality{}, err
		}
		return &nd.Address, nd.Locality, nil
	}
}

// GossipSubscriptionSystemConfigMask filters a system config down to just the
// keys that a tenant SQL servers needs access to. All system tenant objects are
// filtered out (e.g. system tenant descriptors and users).
var GossipSubscriptionSystemConfigMask = config.MakeSystemConfigMask(
	// Tenant SQL servers need just enough of the zone hierarchy to understand
	// which zone configurations apply to their keyspace.
	config.MakeZoneKey(keys.SystemSQLCodec, keys.RootNamespaceID),
	config.MakeZoneKey(keys.SystemSQLCodec, keys.TenantsRangesID),
)

// CombineKVAddresses combines the remote and loopback addresses into a single
// slice, while aldo de-duplicating the loopback address if it's present in the
// remote addresses.
func CombineKVAddresses(addressConfig KVAddressConfig) []string {
	addrs := make([]string, 0, len(addressConfig.RemoteAddresses)+1)
	for _, addr := range addressConfig.RemoteAddresses {
		if addr == addressConfig.LoopbackAddress {
			continue
		}
		addrs = append(addrs, addr)
	}
	if addressConfig.LoopbackAddress != "" {
		addrs = append(addrs, addressConfig.LoopbackAddress)
	}
	return addrs
}
