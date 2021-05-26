// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	// gossipStatusInterval is the interval for logging gossip status.
	gossipStatusInterval = 1 * time.Minute

	// FirstNodeID is the node ID of the first node in a new cluster.
	FirstNodeID         = 1
	graphiteIntervalKey = "external.graphite.interval"
	maxGraphiteInterval = 15 * time.Minute
)

// Metric names.
var (
	metaExecLatency = metric.Metadata{
		Name:        "exec.latency",
		Help:        "Latency of batch KV requests executed on this node",
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaExecSuccess = metric.Metadata{
		Name:        "exec.success",
		Help:        "Number of batch KV requests executed successfully on this node",
		Measurement: "Batch KV Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaExecError = metric.Metadata{
		Name:        "exec.error",
		Help:        "Number of batch KV requests that failed to execute on this node",
		Measurement: "Batch KV Requests",
		Unit:        metric.Unit_COUNT,
	}

	metaDiskStalls = metric.Metadata{
		Name:        "engine.stalls",
		Help:        "Number of disk stalls detected on this node",
		Measurement: "Disk stalls detected",
		Unit:        metric.Unit_COUNT,
	}
)

// Cluster settings.
var (
	// graphiteEndpoint is host:port, if any, of Graphite metrics server.
	graphiteEndpoint = settings.RegisterStringSetting(
		"external.graphite.endpoint",
		"if nonempty, push server metrics to the Graphite or Carbon server at the specified host:port",
		"",
	).WithPublic()
	// graphiteInterval is how often metrics are pushed to Graphite, if enabled.
	graphiteInterval = settings.RegisterDurationSetting(
		graphiteIntervalKey,
		"the interval at which metrics are pushed to Graphite (if enabled)",
		10*time.Second,
		settings.NonNegativeDurationWithMaximum(maxGraphiteInterval),
	).WithPublic()
)

type nodeMetrics struct {
	Latency    *metric.Histogram
	Success    *metric.Counter
	Err        *metric.Counter
	DiskStalls *metric.Counter
}

func makeNodeMetrics(reg *metric.Registry, histogramWindow time.Duration) nodeMetrics {
	nm := nodeMetrics{
		Latency:    metric.NewLatency(metaExecLatency, histogramWindow),
		Success:    metric.NewCounter(metaExecSuccess),
		Err:        metric.NewCounter(metaExecError),
		DiskStalls: metric.NewCounter(metaDiskStalls),
	}
	reg.AddMetricStruct(nm)
	return nm
}

// callComplete records very high-level metrics about the number of completed
// calls and their latency. Currently, this only records statistics at the batch
// level; stats on specific lower-level kv operations are not recorded.
func (nm nodeMetrics) callComplete(d time.Duration, pErr *roachpb.Error) {
	if pErr != nil && pErr.TransactionRestart() == roachpb.TransactionRestart_NONE {
		nm.Err.Inc(1)
	} else {
		nm.Success.Inc(1)
	}
	nm.Latency.RecordValue(d.Nanoseconds())
}

// A Node manages a map of stores (by store ID) for which it serves
// traffic. A node is the top-level data structure. There is one node
// instance per process. A node accepts incoming RPCs and services
// them by directing the commands contained within RPCs to local
// stores, which in turn direct the commands to specific ranges. Each
// node has access to the global, monolithic Key-Value abstraction via
// its client.DB reference. Nodes use this to allocate node and store
// IDs for bootstrapping the node itself or initializing new stores as
// they're added on subsequent instantiations.
type Node struct {
	stopper      *stop.Stopper
	clusterID    *base.ClusterIDContainer // UUID for Cockroach cluster
	Descriptor   roachpb.NodeDescriptor   // Node ID, network/physical topology
	storeCfg     kvserver.StoreConfig     // Config to use and pass to stores
	sqlExec      *sql.InternalExecutor    // For event logging
	stores       *kvserver.Stores         // Access to node-local stores
	metrics      nodeMetrics
	recorder     *status.MetricsRecorder
	startedAt    int64
	lastUp       int64
	initialStart bool // True if this is the first time this node has started.
	txnMetrics   kvcoord.TxnMetrics

	// Used to signal when additional stores, if any, have been initialized.
	additionalStoreInitCh chan struct{}

	perReplicaServer kvserver.Server

	admissionQ *admission.WorkQueue
}

var _ roachpb.InternalServer = &Node{}

// allocateNodeID increments the node id generator key to allocate
// a new, unique node id.
func allocateNodeID(ctx context.Context, db *kv.DB) (roachpb.NodeID, error) {
	val, err := kv.IncrementValRetryable(ctx, db, keys.NodeIDGenerator, 1)
	if err != nil {
		return 0, errors.Wrap(err, "unable to allocate node ID")
	}
	return roachpb.NodeID(val), nil
}

// allocateStoreIDs increments the store id generator key for the
// specified node to allocate count new, unique store ids. The
// first ID in a contiguous range is returned on success.
func allocateStoreIDs(
	ctx context.Context, nodeID roachpb.NodeID, count int64, db *kv.DB,
) (roachpb.StoreID, error) {
	val, err := kv.IncrementValRetryable(ctx, db, keys.StoreIDGenerator, count)
	if err != nil {
		return 0, errors.Wrapf(err, "unable to allocate %d store IDs for node %d", count, nodeID)
	}
	return roachpb.StoreID(val - count + 1), nil
}

// GetBootstrapSchema returns the schema which will be used to bootstrap a new
// server.
func GetBootstrapSchema(
	defaultZoneConfig *zonepb.ZoneConfig, defaultSystemZoneConfig *zonepb.ZoneConfig,
) bootstrap.MetadataSchema {
	return bootstrap.MakeMetadataSchema(keys.SystemSQLCodec, defaultZoneConfig, defaultSystemZoneConfig)
}

// bootstrapCluster initializes the passed-in engines for a new cluster.
// Returns the cluster ID.
//
// The first engine will contain ranges for various static split points (i.e.
// various system ranges and system tables). Note however that many of these
// ranges cannot be accessed by KV in regular means until the node liveness is
// written, since epoch-based leases cannot be granted until then. All other
// engines are initialized with their StoreIdent.
func bootstrapCluster(
	ctx context.Context, engines []storage.Engine, initCfg initServerCfg,
) (*initState, error) {
	clusterID := uuid.MakeV4()
	// TODO(andrei): It'd be cool if this method wouldn't do anything to engines
	// other than the first one, and let regular node startup code deal with them.
	var bootstrapVersion clusterversion.ClusterVersion
	const firstStoreID = 1
	for i, eng := range engines {
		cv, err := kvserver.ReadClusterVersion(ctx, eng)
		if err != nil {
			return nil, errors.Wrapf(err, "reading cluster version of %s", eng)
		} else if cv.Major == 0 {
			return nil, errors.Errorf("missing bootstrap version")
		}

		// bootstrapCluster requires matching cluster versions on all engines.
		if i == 0 {
			bootstrapVersion = cv
		} else if bootstrapVersion != cv {
			return nil, errors.Errorf("found cluster versions %s and %s", bootstrapVersion, cv)
		}

		sIdent := roachpb.StoreIdent{
			ClusterID: clusterID,
			NodeID:    FirstNodeID,
			StoreID:   roachpb.StoreID(i + firstStoreID),
		}

		// Initialize the engine backing the store with the store ident and cluster
		// version.
		if err := kvserver.InitEngine(ctx, eng, sIdent); err != nil {
			return nil, err
		}

		// Create first range, writing directly to engine. Note this does
		// not create the range, just its data. Only do this if this is the
		// first store.
		if i == 0 {
			schema := GetBootstrapSchema(&initCfg.defaultZoneConfig, &initCfg.defaultSystemZoneConfig)
			initialValues, tableSplits := schema.GetInitialValues()
			splits := append(config.StaticSplits(), tableSplits...)
			sort.Slice(splits, func(i, j int) bool {
				return splits[i].Less(splits[j])
			})

			var storeKnobs kvserver.StoreTestingKnobs
			if kn, ok := initCfg.testingKnobs.Store.(*kvserver.StoreTestingKnobs); ok {
				storeKnobs = *kn
			}
			if err := kvserver.WriteInitialClusterData(
				ctx, eng, initialValues,
				bootstrapVersion.Version, len(engines), splits,
				hlc.UnixNano(), storeKnobs,
			); err != nil {
				return nil, err
			}
		}
	}

	return inspectEngines(ctx, engines, initCfg.binaryVersion, initCfg.binaryMinSupportedVersion)
}

// NewNode returns a new instance of Node.
//
// execCfg can be nil to help bootstrapping of a Server (the Node is created
// before the ExecutorConfig is initialized). In that case, InitLogger() needs
// to be called before the Node is used.
func NewNode(
	cfg kvserver.StoreConfig,
	recorder *status.MetricsRecorder,
	reg *metric.Registry,
	stopper *stop.Stopper,
	txnMetrics kvcoord.TxnMetrics,
	stores *kvserver.Stores,
	execCfg *sql.ExecutorConfig,
	clusterID *base.ClusterIDContainer,
) *Node {
	var sqlExec *sql.InternalExecutor
	if execCfg != nil {
		sqlExec = execCfg.InternalExecutor
	}
	n := &Node{
		storeCfg:   cfg,
		stopper:    stopper,
		recorder:   recorder,
		metrics:    makeNodeMetrics(reg, cfg.HistogramWindowInterval),
		stores:     stores,
		txnMetrics: txnMetrics,
		sqlExec:    sqlExec,
		clusterID:  clusterID,
	}
	n.perReplicaServer = kvserver.MakeServer(&n.Descriptor, n.stores)
	return n
}

// InitLogger needs to be called if a nil execCfg was passed to NewNode().
func (n *Node) InitLogger(execCfg *sql.ExecutorConfig) {
	n.sqlExec = execCfg.InternalExecutor
}

// String implements fmt.Stringer.
func (n *Node) String() string {
	return fmt.Sprintf("node=%d", n.Descriptor.NodeID)
}

// AnnotateCtx is a convenience wrapper; see AmbientContext.
func (n *Node) AnnotateCtx(ctx context.Context) context.Context {
	return n.storeCfg.AmbientCtx.AnnotateCtx(ctx)
}

// AnnotateCtxWithSpan is a convenience wrapper; see AmbientContext.
func (n *Node) AnnotateCtxWithSpan(
	ctx context.Context, opName string,
) (context.Context, *tracing.Span) {
	return n.storeCfg.AmbientCtx.AnnotateCtxWithSpan(ctx, opName)
}

// start starts the node by registering the storage instance for the RPC
// service "Node" and initializing stores for each specified engine.
// Launches periodic store gossiping in a goroutine. A callback can
// be optionally provided that will be invoked once this node's
// NodeDescriptor is available, to help bootstrapping.
func (n *Node) start(
	ctx context.Context,
	addr, sqlAddr net.Addr,
	state initState,
	initialStart bool,
	clusterName string,
	attrs roachpb.Attributes,
	locality roachpb.Locality,
	localityAddress []roachpb.LocalityAddress,
	nodeDescriptorCallback func(descriptor roachpb.NodeDescriptor),
) error {
	n.initialStart = initialStart
	n.startedAt = n.storeCfg.Clock.Now().WallTime
	n.Descriptor = roachpb.NodeDescriptor{
		NodeID:          state.nodeID,
		Address:         util.MakeUnresolvedAddr(addr.Network(), addr.String()),
		SQLAddress:      util.MakeUnresolvedAddr(sqlAddr.Network(), sqlAddr.String()),
		Attrs:           attrs,
		Locality:        locality,
		LocalityAddress: localityAddress,
		ClusterName:     clusterName,
		ServerVersion:   n.storeCfg.Settings.Version.BinaryVersion(),
		BuildTag:        build.GetInfo().Tag,
		StartedAt:       n.startedAt,
	}
	// Invoke any passed in nodeDescriptorCallback as soon as it's available, to
	// ensure that other components (currently the DistSQLPlanner) are initialized
	// before store startup continues.
	if nodeDescriptorCallback != nil {
		nodeDescriptorCallback(n.Descriptor)
	}

	// Gossip the node descriptor to make this node addressable by node ID.
	n.storeCfg.Gossip.NodeID.Set(ctx, n.Descriptor.NodeID)
	if err := n.storeCfg.Gossip.SetNodeDescriptor(&n.Descriptor); err != nil {
		return errors.Errorf("couldn't gossip descriptor for node %d: %s", n.Descriptor.NodeID, err)
	}

	// Start the closed timestamp subsystem.
	n.storeCfg.ClosedTimestamp.Start(n.Descriptor.NodeID)

	// Create stores from the engines that were already initialized.
	for _, e := range state.initializedEngines {
		s := kvserver.NewStore(ctx, n.storeCfg, e, &n.Descriptor)
		if err := s.Start(ctx, n.stopper); err != nil {
			return errors.Errorf("failed to start store: %s", err)
		}

		n.addStore(ctx, s)
		log.Infof(ctx, "initialized store s%s", s.StoreID())
	}

	// Verify all initialized stores agree on cluster and node IDs.
	if err := n.validateStores(ctx); err != nil {
		return err
	}
	log.VEventf(ctx, 2, "validated stores")

	// Compute the time this node was last up; this is done by reading the
	// "last up time" from every store and choosing the most recent timestamp.
	var mostRecentTimestamp hlc.Timestamp
	if err := n.stores.VisitStores(func(s *kvserver.Store) error {
		timestamp, err := s.ReadLastUpTimestamp(ctx)
		if err != nil {
			return err
		}
		if mostRecentTimestamp.Less(timestamp) {
			mostRecentTimestamp = timestamp
		}
		return nil
	}); err != nil {
		return errors.Wrapf(err, "failed to read last up timestamp from stores")
	}
	n.lastUp = mostRecentTimestamp.WallTime

	// Set the stores map as the gossip persistent storage, so that
	// gossip can bootstrap using the most recently persisted set of
	// node addresses.
	if err := n.storeCfg.Gossip.SetStorage(n.stores); err != nil {
		return fmt.Errorf("failed to initialize the gossip interface: %s", err)
	}

	// Initialize remaining stores/engines, if any.
	if len(state.uninitializedEngines) > 0 {
		// We need to initialize any remaining stores asynchronously.
		// Consider the range that houses the store ID allocator. When we
		// restart the set of nodes that holds a quorum of these replicas,
		// specifically when we restart them with auxiliary stores, these stores
		// will require store IDs during initialization[1]. But if we're gating
		// node start up (specifically the opening up of RPC floodgates) on
		// having all stores in the node fully initialized, we'll simply hang
		// when trying to allocate store IDs. See
		// TestAddNewStoresToExistingNodes and #39415 for more details.
		//
		// So instead we opt to initialize additional stores asynchronously, and
		// rely on the blocking function n.waitForAdditionalStoreInit() to
		// signal to the caller that all stores have been fully initialized.
		//
		// [1]: It's important to note that store IDs are allocated via a
		// sequence ID generator stored in a system key.
		n.additionalStoreInitCh = make(chan struct{})
		if err := n.stopper.RunAsyncTask(ctx, "initialize-additional-stores", func(ctx context.Context) {
			if err := n.initializeAdditionalStores(ctx, state.uninitializedEngines, n.stopper); err != nil {
				log.Fatalf(ctx, "while initializing additional stores: %v", err)
			}
			close(n.additionalStoreInitCh)
		}); err != nil {
			close(n.additionalStoreInitCh)
			return err
		}
	}

	n.startComputePeriodicMetrics(n.stopper, base.DefaultMetricsSampleInterval)

	// Be careful about moving this line above where we start stores; store
	// migrations rely on the fact that the cluster version has not been updated
	// via Gossip (we have migrations that want to run only if the server starts
	// with a given cluster version, but not if the server starts with a lower
	// one and gets bumped immediately, which would be possible if gossip got
	// started earlier).
	n.startGossiping(ctx, n.stopper)

	allEngines := append([]storage.Engine(nil), state.initializedEngines...)
	allEngines = append(allEngines, state.uninitializedEngines...)
	for _, e := range allEngines {
		t := e.Type()
		log.Infof(ctx, "started with engine type %v", t)
	}
	log.Infof(ctx, "started with attributes %v", attrs.Attrs)
	return nil
}

// waitForAdditionalStoreInit blocks until all additional empty stores,
// if any, have been initialized.
func (n *Node) waitForAdditionalStoreInit() {
	if n.additionalStoreInitCh != nil {
		<-n.additionalStoreInitCh
	}
}

// IsDraining returns true if at least one Store housed on this Node is not
// currently allowing range leases to be procured or extended.
func (n *Node) IsDraining() bool {
	var isDraining bool
	if err := n.stores.VisitStores(func(s *kvserver.Store) error {
		isDraining = isDraining || s.IsDraining()
		return nil
	}); err != nil {
		panic(err)
	}
	return isDraining
}

// SetDraining sets the draining mode on all of the node's underlying stores.
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (n *Node) SetDraining(drain bool, reporter func(int, redact.SafeString)) error {
	return n.stores.VisitStores(func(s *kvserver.Store) error {
		s.SetDraining(drain, reporter)
		return nil
	})
}

// SetHLCUpperBound sets the upper bound of the HLC wall time on all of the
// node's underlying stores.
func (n *Node) SetHLCUpperBound(ctx context.Context, hlcUpperBound int64) error {
	return n.stores.VisitStores(func(s *kvserver.Store) error {
		return s.WriteHLCUpperBound(ctx, hlcUpperBound)
	})
}

func (n *Node) addStore(ctx context.Context, store *kvserver.Store) {
	cv, err := kvserver.ReadClusterVersion(context.TODO(), store.Engine())
	if err != nil {
		log.Fatalf(ctx, "%v", err)
	}
	if cv == (clusterversion.ClusterVersion{}) {
		// The store should have had a version written to it during the store
		// initialization process.
		log.Fatal(ctx, "attempting to add a store without a version")
	}
	n.stores.AddStore(store)
	n.recorder.AddStore(store)
}

// validateStores iterates over all stores, verifying they agree on node ID.
// The node's ident is initialized based on the agreed-upon node ID. Note that
// cluster ID consistency is checked elsewhere in inspectEngines.
//
// TODO(tbg): remove this, we already validate everything in inspectEngines now.
func (n *Node) validateStores(ctx context.Context) error {
	return n.stores.VisitStores(func(s *kvserver.Store) error {
		if n.Descriptor.NodeID != s.Ident.NodeID {
			return errors.Errorf("store %s node ID doesn't match node ID: %d", s, n.Descriptor.NodeID)
		}
		return nil
	})
}

// initializeAdditionalStores initializes the given set of engines once the
// cluster and node ID have been established for this node. Store IDs are
// allocated via a sequence id generator stored at a system key per node. The
// new stores are added to n.stores.
func (n *Node) initializeAdditionalStores(
	ctx context.Context, engines []storage.Engine, stopper *stop.Stopper,
) error {
	if n.clusterID.Get() == uuid.Nil {
		return errors.New("missing cluster ID during initialization of additional store")
	}

	{
		// Initialize all waiting stores by allocating a new store id for each
		// and invoking kvserver.InitEngine() to persist it. We'll then
		// construct a new store out of the initialized engine and attach it to
		// ourselves.
		storeIDAlloc := int64(len(engines))
		startID, err := allocateStoreIDs(ctx, n.Descriptor.NodeID, storeIDAlloc, n.storeCfg.DB)
		if err != nil {
			return errors.Errorf("error allocating store ids: %s", err)
		}

		sIdent := roachpb.StoreIdent{
			ClusterID: n.clusterID.Get(),
			NodeID:    n.Descriptor.NodeID,
			StoreID:   startID,
		}
		for _, eng := range engines {
			if err := kvserver.InitEngine(ctx, eng, sIdent); err != nil {
				return err
			}

			s := kvserver.NewStore(ctx, n.storeCfg, eng, &n.Descriptor)
			if err := s.Start(ctx, stopper); err != nil {
				return err
			}

			n.addStore(ctx, s)
			log.Infof(ctx, "initialized store s%s", s.StoreID())

			// Done regularly in Node.startGossiping, but this cuts down the time
			// until this store is used for range allocations.
			if err := s.GossipStore(ctx, false /* useCached */); err != nil {
				log.Warningf(ctx, "error doing initial gossiping: %s", err)
			}

			sIdent.StoreID++
		}
	}

	// Write a new status summary after all stores have been initialized; this
	// helps the UI remain responsive when new nodes are added.
	if err := n.writeNodeStatus(ctx, 0 /* alertTTL */, false /* mustExist */); err != nil {
		log.Warningf(ctx, "error writing node summary after store bootstrap: %s", err)
	}

	return nil
}

// startGossiping loops on a periodic ticker to gossip node-related
// information. Starts a goroutine to loop until the node is closed.
func (n *Node) startGossiping(ctx context.Context, stopper *stop.Stopper) {
	ctx = n.AnnotateCtx(ctx)
	_ = stopper.RunAsyncTask(ctx, "start-gossip", func(ctx context.Context) {
		// Verify we've already gossiped our node descriptor.
		//
		// TODO(tbg): see if we really needed to do this earlier already. We
		// probably needed to (this call has to come late for ... reasons I
		// still need to look into) and nobody can talk to this node until
		// the descriptor is in Gossip.
		if _, err := n.storeCfg.Gossip.GetNodeDescriptor(n.Descriptor.NodeID); err != nil {
			panic(err)
		}

		// NB: Gossip may not be connected at this point. That's fine though,
		// we can still gossip something; Gossip sends it out reactively once
		// it can.

		statusTicker := time.NewTicker(gossipStatusInterval)
		storesTicker := time.NewTicker(gossip.StoresInterval)
		nodeTicker := time.NewTicker(gossip.NodeDescriptorInterval)
		defer storesTicker.Stop()
		defer nodeTicker.Stop()
		n.gossipStores(ctx) // one-off run before going to sleep
		for {
			select {
			case <-statusTicker.C:
				n.storeCfg.Gossip.LogStatus()
			case <-storesTicker.C:
				n.gossipStores(ctx)
			case <-nodeTicker.C:
				if err := n.storeCfg.Gossip.SetNodeDescriptor(&n.Descriptor); err != nil {
					log.Warningf(ctx, "couldn't gossip descriptor for node %d: %s", n.Descriptor.NodeID, err)
				}
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// gossipStores broadcasts each store and dead replica to the gossip network.
func (n *Node) gossipStores(ctx context.Context) {
	if err := n.stores.VisitStores(func(s *kvserver.Store) error {
		return s.GossipStore(ctx, false /* useCached */)
	}); err != nil {
		log.Warningf(ctx, "%v", err)
	}
}

// startComputePeriodicMetrics starts a loop which periodically instructs each
// store to compute the value of metrics which cannot be incrementally
// maintained.
func (n *Node) startComputePeriodicMetrics(stopper *stop.Stopper, interval time.Duration) {
	ctx := n.AnnotateCtx(context.Background())
	_ = stopper.RunAsyncTask(ctx, "compute-metrics", func(ctx context.Context) {
		// Compute periodic stats at the same frequency as metrics are sampled.
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for tick := 0; ; tick++ {
			select {
			case <-ticker.C:
				if err := n.computePeriodicMetrics(ctx, tick); err != nil {
					log.Errorf(ctx, "failed computing periodic metrics: %s", err)
				}
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// computePeriodicMetrics instructs each store to compute the value of
// complicated metrics.
func (n *Node) computePeriodicMetrics(ctx context.Context, tick int) error {
	return n.stores.VisitStores(func(store *kvserver.Store) error {
		if err := store.ComputeMetrics(ctx, tick); err != nil {
			log.Warningf(ctx, "%s: unable to compute metrics: %s", store, err)
		}
		return nil
	})
}

// GetPebbleMetrics implements admission.PebbleMetricsProvider.
func (n *Node) GetPebbleMetrics() []*pebble.Metrics {
	var metrics []*pebble.Metrics
	_ = n.stores.VisitStores(func(store *kvserver.Store) error {
		m := store.Engine().GetMetrics()
		metrics = append(metrics, m.Metrics)
		return nil
	})
	return metrics
}

func (n *Node) startGraphiteStatsExporter(st *cluster.Settings) {
	ctx := logtags.AddTag(n.AnnotateCtx(context.Background()), "graphite stats exporter", nil)
	pm := metric.MakePrometheusExporter()

	_ = n.stopper.RunAsyncTask(ctx, "graphite-exporter", func(ctx context.Context) {
		var timer timeutil.Timer
		defer timer.Stop()
		for {
			timer.Reset(graphiteInterval.Get(&st.SV))
			select {
			case <-n.stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
				endpoint := graphiteEndpoint.Get(&st.SV)
				if endpoint != "" {
					if err := n.recorder.ExportToGraphite(ctx, endpoint, &pm); err != nil {
						log.Infof(ctx, "error pushing metrics to graphite: %s\n", err)
					}
				}
			}
		}
	})
}

// startWriteNodeStatus begins periodically persisting status summaries for the
// node and its stores.
func (n *Node) startWriteNodeStatus(frequency time.Duration) error {
	ctx := logtags.AddTag(n.AnnotateCtx(context.Background()), "summaries", nil)
	// Immediately record summaries once on server startup. The update loop below
	// will only update the key if it exists, to avoid race conditions during
	// node decommissioning, so we have to error out if we can't create it.
	if err := n.writeNodeStatus(ctx, 0 /* alertTTL */, false /* mustExist */); err != nil {
		return errors.Wrap(err, "error recording initial status summaries")
	}
	return n.stopper.RunAsyncTask(ctx, "write-node-status", func(ctx context.Context) {
		// Write a status summary immediately; this helps the UI remain
		// responsive when new nodes are added.
		ticker := time.NewTicker(frequency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Use an alertTTL of twice the ticker frequency. This makes sure that
				// alerts don't disappear and reappear spuriously while at the same
				// time ensuring that an alert doesn't linger for too long after having
				// resolved.
				//
				// The status key must already exist, to avoid race conditions
				// during decommissioning of this node. Decommissioning may be
				// carried out by a different node, so this avoids resurrecting
				// the status entry after the decommissioner has removed it.
				// See Server.Decommission().
				if err := n.writeNodeStatus(ctx, 2*frequency, true /* mustExist */); err != nil {
					log.Warningf(ctx, "error recording status summaries: %s", err)
				}
			case <-n.stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// writeNodeStatus retrieves status summaries from the supplied
// NodeStatusRecorder and persists them to the cockroach data store.
// If mustExist is true the status key must already exist and must
// not change during writing -- if false, the status is always written.
func (n *Node) writeNodeStatus(ctx context.Context, alertTTL time.Duration, mustExist bool) error {
	var err error
	if runErr := n.stopper.RunTask(ctx, "node.Node: writing summary", func(ctx context.Context) {
		nodeStatus := n.recorder.GenerateNodeStatus(ctx)
		if nodeStatus == nil {
			return
		}

		if result := n.recorder.CheckHealth(ctx, *nodeStatus); len(result.Alerts) != 0 {
			var numNodes int
			if err := n.storeCfg.Gossip.IterateInfos(gossip.KeyNodeIDPrefix, func(k string, info gossip.Info) error {
				numNodes++
				return nil
			}); err != nil {
				log.Warningf(ctx, "%v", err)
			}
			if numNodes > 1 {
				// Avoid this warning on single-node clusters, which require special UX.
				log.Warningf(ctx, "health alerts detected: %+v", result)
			}
			if err := n.storeCfg.Gossip.AddInfoProto(
				gossip.MakeNodeHealthAlertKey(n.Descriptor.NodeID), &result, alertTTL,
			); err != nil {
				log.Warningf(ctx, "unable to gossip health alerts: %+v", result)
			}

			// TODO(tschottdorf): add a metric that we increment every time there are
			// alerts. This can help understand how long the cluster has been in that
			// state (since it'll be incremented every ~10s).
		}

		err = n.recorder.WriteNodeStatus(ctx, n.storeCfg.DB, *nodeStatus, mustExist)
	}); runErr != nil {
		err = runErr
	}
	return err
}

// recordJoinEvent begins an asynchronous task which attempts to log a "node
// join" or "node restart" event. This query will retry until it succeeds or the
// server stops.
func (n *Node) recordJoinEvent(ctx context.Context) {
	var event eventpb.EventPayload
	var nodeDetails *eventpb.CommonNodeEventDetails
	if !n.initialStart {
		ev := &eventpb.NodeRestart{}
		event = ev
		nodeDetails = &ev.CommonNodeEventDetails
		nodeDetails.LastUp = n.lastUp
	} else {
		ev := &eventpb.NodeJoin{}
		event = ev
		nodeDetails = &ev.CommonNodeEventDetails
		nodeDetails.LastUp = n.startedAt
	}
	event.CommonDetails().Timestamp = timeutil.Now().UnixNano()
	nodeDetails.StartedAt = n.startedAt
	nodeDetails.NodeID = int32(n.Descriptor.NodeID)

	// Ensure that the event goes to log files even if LogRangeEvents is
	// disabled (which means skip the system.eventlog _table_).
	log.StructuredEvent(ctx, event)

	if !n.storeCfg.LogRangeEvents {
		return
	}

	_ = n.stopper.RunAsyncTask(ctx, "record-join", func(bgCtx context.Context) {
		ctx, span := n.AnnotateCtxWithSpan(bgCtx, "record-join-event")
		defer span.Finish()
		retryOpts := base.DefaultRetryOptions()
		retryOpts.Closer = n.stopper.ShouldQuiesce()
		for r := retry.Start(retryOpts); r.Next(); {
			if err := n.storeCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				return sql.InsertEventRecord(ctx, n.sqlExec,
					txn,
					int32(n.Descriptor.NodeID), /* reporting ID: the node where the event is logged */
					sql.LogToSystemTable|sql.LogToDevChannelIfVerbose, /* LogEventDestination: we already call log.StructuredEvent above */
					int32(n.Descriptor.NodeID),                        /* target ID: the node that is joining (ourselves) */
					event,
				)
			}); err != nil {
				log.Warningf(ctx, "%s: unable to log event %v: %v", n, event, err)
			} else {
				return
			}
		}
	})
}

// If we receive a (proto-marshaled) roachpb.BatchRequest whose Requests contain
// a message type unknown to this node, we will end up with a zero entry in the
// slice. If we don't error out early, this breaks all sorts of assumptions and
// usually ends in a panic.
func checkNoUnknownRequest(reqs []roachpb.RequestUnion) *roachpb.UnsupportedRequestError {
	for _, req := range reqs {
		if req.GetValue() == nil {
			return &roachpb.UnsupportedRequestError{}
		}
	}
	return nil
}

func (n *Node) batchInternal(
	ctx context.Context, args *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if detail := checkNoUnknownRequest(args.Requests); detail != nil {
		var br roachpb.BatchResponse
		br.Error = roachpb.NewError(detail)
		return &br, nil
	}

	var br *roachpb.BatchResponse
	if err := n.stopper.RunTaskWithErr(ctx, "node.Node: batch", func(ctx context.Context) error {
		var finishSpan func(*roachpb.BatchResponse)
		// Shadow ctx from the outer function. Written like this to pass the linter.
		ctx, finishSpan = n.setupSpanForIncomingRPC(ctx, grpcutil.IsLocalRequestContext(ctx))
		// NB: wrapped to delay br evaluation to its value when returning.
		defer func() { finishSpan(br) }()
		if log.HasSpanOrEvent(ctx) {
			log.Eventf(ctx, "node received request: %s", args.Summary())
		}

		tStart := timeutil.Now()
		var pErr *roachpb.Error
		br, pErr = n.stores.Send(ctx, *args)
		if pErr != nil {
			br = &roachpb.BatchResponse{}
			log.VErrEventf(ctx, 3, "error from stores.Send: %s", pErr)
		}
		if br.Error != nil {
			panic(roachpb.ErrorUnexpectedlySet(n.stores, br))
		}
		n.metrics.callComplete(timeutil.Since(tStart), pErr)
		br.Error = pErr
		return nil
	}); err != nil {
		return nil, err
	}
	return br, nil
}

// Batch implements the roachpb.InternalServer interface.
func (n *Node) Batch(
	ctx context.Context, args *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	// NB: Node.Batch is called directly for "local" calls. We don't want to
	// carry the associated log tags forward as doing so makes adding additional
	// log tags more expensive and makes local calls differ from remote calls.
	ctx = n.storeCfg.AmbientCtx.ResetAndAnnotateCtx(ctx)

	var callAdmittedWorkDone bool
	var tenantID roachpb.TenantID
	if n.admissionQ != nil {
		bypassAdmission := args.IsAdmin()
		// TODO(sumeer): properly initialize tenant ID. If non-SystemTenantID sends
		// a request with source other than AdmissionHeader_FROM_SQL, change it to
		// FROM_SQL.
		tenantID = roachpb.SystemTenantID
		if args.AdmissionHeader.Source == roachpb.AdmissionHeader_OTHER {
			bypassAdmission = true
		}
		createTime := args.AdmissionHeader.CreateTime
		if !bypassAdmission && createTime == 0 {
			// TODO(sumeer): revisit this for multi-tenant. Specifically, the SQL use
			// of zero CreateTime needs to be revisited. It should use high priority.
			createTime = timeutil.Now().UnixNano()
		}
		admissionInfo := admission.WorkInfo{
			TenantID:        tenantID,
			Priority:        admission.WorkPriority(args.AdmissionHeader.Priority),
			CreateTime:      createTime,
			BypassAdmission: bypassAdmission,
		}
		var err error
		callAdmittedWorkDone, err = n.admissionQ.Admit(ctx, admissionInfo)
		if err != nil {
			return nil, err
		}
	}
	br, err := n.batchInternal(ctx, args)
	if callAdmittedWorkDone {
		n.admissionQ.AdmittedWorkDone(tenantID)
	}

	// We always return errors via BatchResponse.Error so structure is
	// preserved; plain errors are presumed to be from the RPC
	// framework and not from cockroach.
	if err != nil {
		if br == nil {
			br = &roachpb.BatchResponse{}
		}
		if br.Error != nil {
			log.Fatalf(
				ctx, "attempting to return both a plain error (%s) and roachpb.Error (%s)", err, br.Error,
			)
		}
		br.Error = roachpb.NewError(err)
	}
	return br, nil
}

// setupSpanForIncomingRPC takes a context and returns a derived context with a
// new span in it. Depending on the input context, that span might be a root
// span or a child span. If it is a child span, it might be a child span of a
// local or a remote span. Note that supporting both the "child of local span"
// and "child of remote span" cases are important, as this RPC can be called
// either through the network or directly if the caller is local.
//
// It returns the derived context and a cleanup function to be
// called when servicing the RPC is done. The cleanup function will
// close the span and serialize any data recorded to that span into
// the BatchResponse. The cleanup function takes the BatchResponse
// in which the response is to serialized. The BatchResponse can
// be nil in case no response is to be returned to the rpc caller.
func (n *Node) setupSpanForIncomingRPC(
	ctx context.Context, isLocalRequest bool,
) (context.Context, func(*roachpb.BatchResponse)) {
	// The operation name matches the one created by the interceptor in the
	// remoteTrace case below.
	const opName = "/cockroach.roachpb.Internal/Batch"
	tr := n.storeCfg.AmbientCtx.Tracer
	var newSpan, grpcSpan *tracing.Span
	if isLocalRequest {
		// This is a local request which circumvented gRPC. Start a span now.
		ctx, newSpan = tracing.EnsureChildSpan(ctx, tr, opName)
	} else {
		grpcSpan = tracing.SpanFromContext(ctx)
		if grpcSpan == nil {
			// If tracing information was passed via gRPC metadata, the gRPC interceptor
			// should have opened a span for us. If not, open a span now (if tracing is
			// disabled, this will be a noop span).
			ctx, newSpan = tr.StartSpanCtx(ctx, opName)
		} else {
			grpcSpan.SetTag("node", n.Descriptor.NodeID)
		}
	}

	finishSpan := func(br *roachpb.BatchResponse) {
		if newSpan != nil {
			newSpan.Finish()
		}
		if br == nil {
			return
		}
		if grpcSpan != nil {
			// If our local span descends from a parent on the other
			// end of the RPC (i.e. the !isLocalRequest) case,
			// attach the span recording to the batch response.
			if rec := grpcSpan.GetRecording(); rec != nil {
				br.CollectedSpans = append(br.CollectedSpans, rec...)
			}
		}
	}
	return ctx, finishSpan
}

// RangeLookup implements the roachpb.InternalServer interface.
func (n *Node) RangeLookup(
	ctx context.Context, req *roachpb.RangeLookupRequest,
) (*roachpb.RangeLookupResponse, error) {
	ctx = n.storeCfg.AmbientCtx.AnnotateCtx(ctx)

	// Proxy the RangeLookup through the local DB. Note that this does not use
	// the local RangeDescriptorCache itself (for the direct range descriptor).
	// To be able to do so, we'd have to let tenant's evict descriptors from our
	// cache in order to avoid serving the same stale descriptor over and over
	// again. Because of that, using our own cache doesn't seem worth it, at
	// least for now.
	sender := n.storeCfg.DB.NonTransactionalSender()
	rs, preRs, err := kv.RangeLookup(
		ctx,
		sender,
		req.Key.AsRawKey(),
		req.ReadConsistency,
		req.PrefetchNum,
		req.PrefetchReverse,
	)
	resp := new(roachpb.RangeLookupResponse)
	if err != nil {
		resp.Error = roachpb.NewError(err)
	} else {
		resp.Descriptors = rs
		resp.PrefetchedDescriptors = preRs
	}
	return resp, nil
}

// RangeFeed implements the roachpb.InternalServer interface.
func (n *Node) RangeFeed(
	args *roachpb.RangeFeedRequest, stream roachpb.Internal_RangeFeedServer,
) error {
	pErr := n.stores.RangeFeed(args, stream)
	if pErr != nil {
		var event roachpb.RangeFeedEvent
		event.SetValue(&roachpb.RangeFeedError{
			Error: *pErr,
		})
		return stream.Send(&event)
	}
	return nil
}

// ResetQuorum implements the roachpb.InternalServer interface.
func (n *Node) ResetQuorum(
	ctx context.Context, req *roachpb.ResetQuorumRequest,
) (_ *roachpb.ResetQuorumResponse, rErr error) {
	// Get range descriptor and save original value of the descriptor for the input range id.
	var desc roachpb.RangeDescriptor
	var expValue roachpb.Value
	txnTries := 0
	if err := n.storeCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txnTries++
		if txnTries > 1 {
			log.Infof(ctx, "failed to retrieve range descriptor for r%d, retrying...", req.RangeID)
		}
		kvs, err := kvclient.ScanMetaKVs(ctx, txn, roachpb.Span{
			Key:    roachpb.KeyMin,
			EndKey: roachpb.KeyMax,
		})
		if err != nil {
			return err
		}

		for i := range kvs {
			if err := kvs[i].Value.GetProto(&desc); err != nil {
				return err
			}
			if desc.RangeID == roachpb.RangeID(req.RangeID) {
				expValue = *kvs[i].Value
				return nil
			}
		}
		return errors.Errorf("r%d not found", req.RangeID)
	}); err != nil {
		log.Errorf(ctx, "range descriptor for r%d could not be read: %v", req.RangeID, err)
		return nil, err
	}
	log.Infof(ctx, "retrieved original range descriptor %s", desc)

	// Check that we've actually lost quorum.
	livenessMap := n.storeCfg.NodeLiveness.GetIsLiveMap()
	available := desc.Replicas().CanMakeProgress(func(rDesc roachpb.ReplicaDescriptor) bool {
		return livenessMap[rDesc.NodeID].IsLive
	})
	if available {
		return nil, errors.Errorf("targeted range to recover has not lost quorum.")
	}
	// Check that we're not a metaX range.
	if bytes.HasPrefix(desc.StartKey, keys.Meta1Prefix) || bytes.HasPrefix(desc.StartKey, keys.Meta2Prefix) {
		return nil, errors.Errorf("targeted range to recover is a meta1 or meta2 range.")
	}

	// Update the range descriptor and update meta ranges for the descriptor, removing all replicas.
	deadReplicas := append([]roachpb.ReplicaDescriptor(nil), desc.Replicas().Descriptors()...)
	for _, rd := range deadReplicas {
		desc.RemoveReplica(rd.NodeID, rd.StoreID)
	}
	// Pick any store on the current node to send the snapshot to.
	var storeID roachpb.StoreID
	if err := n.stores.VisitStores(func(s *kvserver.Store) error {
		if storeID == 0 {
			storeID = s.StoreID()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if storeID == 0 {
		return nil, errors.New("no store found")
	}
	// Add current node as new replica.
	toReplicaDescriptor := desc.AddReplica(n.Descriptor.NodeID, storeID, roachpb.VOTER_FULL)
	// Increment the generation so that the various caches will recognize this descriptor as newer.
	desc.IncrementGeneration()

	log.Infof(ctx, "initiating recovery process using %s", desc)

	// Update the meta2 entry. Note that we're intentionally
	// eschewing updateRangeAddressing since the copy of the
	// descriptor that resides on the range itself has lost quorum.
	metaKey := keys.RangeMetaKey(desc.EndKey).AsRawKey()
	if err := n.storeCfg.DB.CPut(ctx, metaKey, &desc, expValue.TagAndDataBytes()); err != nil {
		return nil, err
	}
	log.Infof(ctx, "updated meta2 entry for r%d", desc.RangeID)

	// Set up connection to self. Use rpc.SystemClass to avoid throttling.
	conn, err := n.storeCfg.NodeDialer.Dial(ctx, n.Descriptor.NodeID, rpc.SystemClass)
	if err != nil {
		return nil, err
	}

	// Initialize and send an empty snapshot to self in order to use crdb
	// internal upreplication and rebalancing mechanisms to create further
	// replicas from this fresh snapshot.
	if err := kvserver.SendEmptySnapshot(
		ctx,
		n.storeCfg.Settings,
		conn,
		n.storeCfg.Clock.Now(),
		desc,
		toReplicaDescriptor,
	); err != nil {
		return nil, err
	}
	log.Infof(ctx, "sent empty snapshot to %s", toReplicaDescriptor)

	return &roachpb.ResetQuorumResponse{}, nil
}

// GossipSubscription implements the roachpb.InternalServer interface.
func (n *Node) GossipSubscription(
	args *roachpb.GossipSubscriptionRequest, stream roachpb.Internal_GossipSubscriptionServer,
) error {
	ctx := n.storeCfg.AmbientCtx.AnnotateCtx(stream.Context())
	ctxDone := ctx.Done()

	stripContent := func(_ string, content roachpb.Value) (roachpb.Value, error) {
		// Don't strip anything.
		return content, nil
	}
	if _, ok := roachpb.TenantFromContext(ctx); ok {
		// If this is a tenant connection, strip portions of the gossip infos.
		stripContent = func(key string, content roachpb.Value) (roachpb.Value, error) {
			switch key {
			case gossip.KeySystemConfig:
				// Strip the system config down to just those keys in the
				// GossipSubscriptionSystemConfigMask, preventing cluster-wide
				// or system tenant-specific information to leak.
				var ents config.SystemConfigEntries
				if err := content.GetProto(&ents); err != nil {
					return roachpb.Value{}, errors.Errorf("could not unmarshal system config: %v", err)
				}

				var newContent roachpb.Value
				newEnts := kvtenant.GossipSubscriptionSystemConfigMask.Apply(ents)
				if err := newContent.SetProto(&newEnts); err != nil {
					return roachpb.Value{}, errors.Errorf("could not marshal system config: %v", err)
				}
				return newContent, nil
			default:
				return content, nil
			}
		}
	}

	// Register a callback for each of the requested patterns. We don't want to
	// block the gossip callback goroutine on a slow consumer, so we instead
	// handle all communication asynchronously. We could pick a channel size and
	// say that if the channel ever blocks, terminate the subscription. Doing so
	// feels fragile, though, especially during the initial information dump.
	// Instead, we say that if the channel ever blocks for more than some
	// duration, terminate the subscription.
	entC := make(chan *roachpb.GossipSubscriptionEvent, 256)
	entCClosed := false
	var callbackMu syncutil.Mutex
	const maxBlockDur = 1 * time.Millisecond
	for _, pattern := range args.Patterns {
		pattern := pattern
		callback := func(key string, content roachpb.Value) {
			callbackMu.Lock()
			defer callbackMu.Unlock()
			if entCClosed {
				return
			}
			content, err := stripContent(key, content)
			var event roachpb.GossipSubscriptionEvent
			if err != nil {
				event.Error = roachpb.NewError(err)
			} else {
				event.Key = key
				event.Content = content
				event.PatternMatched = pattern
			}
			select {
			case entC <- &event:
			default:
				select {
				case entC <- &event:
				case <-time.After(maxBlockDur):
					// entC blocking for too long. The consumer must not be
					// keeping up. Terminate the subscription.
					close(entC)
					entCClosed = true
				}
			}
		}
		unregister := n.storeCfg.Gossip.RegisterCallback(pattern, callback)
		defer unregister()
	}

	for {
		select {
		case e, ok := <-entC:
			if !ok {
				// The consumer was not keeping up with gossip updates, so its
				// subscription was terminated to avoid blocking gossip.
				err := roachpb.NewErrorf("subscription terminated due to slow consumption")
				log.Warningf(ctx, "%v", err)
				e = &roachpb.GossipSubscriptionEvent{Error: err}
			}
			if err := stream.Send(e); err != nil {
				return err
			}
		case <-ctxDone:
			return ctx.Err()
		}
	}
}

// Join implements the roachpb.InternalServer service. This is the
// "connectivity" API; individual CRDB servers are passed in a --join list and
// the join targets are addressed through this API.
func (n *Node) Join(
	ctx context.Context, req *roachpb.JoinNodeRequest,
) (*roachpb.JoinNodeResponse, error) {
	ctx, span := n.AnnotateCtxWithSpan(ctx, "alloc-{node,store}-id")
	defer span.Finish()

	activeVersion := n.storeCfg.Settings.Version.ActiveVersion(ctx)
	if req.BinaryVersion.Less(activeVersion.Version) {
		return nil, grpcstatus.Error(codes.PermissionDenied, ErrIncompatibleBinaryVersion.Error())
	}

	nodeID, err := allocateNodeID(ctx, n.storeCfg.DB)
	if err != nil {
		return nil, err
	}

	storeID, err := allocateStoreIDs(ctx, nodeID, 1, n.storeCfg.DB)
	if err != nil {
		return nil, err
	}

	// We create a liveness record here for the joining node while here. We do
	// so to maintain the invariant that there's always a liveness record for a
	// given node. See `WriteInitialClusterData` for the other codepath where we
	// manually create a liveness record to maintain this same invariant.
	//
	// NB: This invariant will be required for when we introduce long running
	// migrations. See https://github.com/cockroachdb/cockroach/pull/48843 for
	// details.
	if err := n.storeCfg.NodeLiveness.CreateLivenessRecord(ctx, nodeID); err != nil {
		return nil, err
	}

	log.Infof(ctx, "allocated IDs: n%d, s%d", nodeID, storeID)

	return &roachpb.JoinNodeResponse{
		ClusterID:     n.clusterID.Get().GetBytes(),
		NodeID:        int32(nodeID),
		StoreID:       int32(storeID),
		ActiveVersion: &activeVersion.Version,
	}, nil
}
