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
	"strings"
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
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/tenantsettingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	// gossipStatusInterval is the interval for logging gossip status.
	gossipStatusInterval = 1 * time.Minute

	graphiteIntervalKey = "external.graphite.interval"
	maxGraphiteInterval = 15 * time.Minute
)

// Metric names.
var (
	metaExecLatency = metric.Metadata{
		Name: "exec.latency",
		Help: `Latency of batch KV requests (including errors) executed on this node.

This measures requests already addressed to a single replica, from the moment
at which they arrive at the internal gRPC endpoint to the moment at which the
response (or an error) is returned.

This latency includes in particular commit waits, conflict resolution and replication,
and end-users can easily produce high measurements via long-running transactions that
conflict with foreground traffic. This metric thus does not provide a good signal for
understanding the health of the KV layer.
`,
		Measurement: "Latency",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaExecSuccess = metric.Metadata{
		Name: "exec.success",
		Help: `Number of batch KV requests executed successfully on this node.

A request is considered to have executed 'successfully' if it either returns a result
or a transaction restart/abort error.
`,
		Measurement: "Batch KV Requests",
		Unit:        metric.Unit_COUNT,
	}
	metaExecError = metric.Metadata{
		Name: "exec.error",
		Help: `Number of batch KV requests that failed to execute on this node.

This count excludes transaction restart/abort errors. However, it will include
other errors expected during normal operation, such as ConditionFailedError.
This metric is thus not an indicator of KV health.`,
		Measurement: "Batch KV Requests",
		Unit:        metric.Unit_COUNT,
	}

	metaDiskStalls = metric.Metadata{
		Name:        "engine.stalls",
		Help:        "Number of disk stalls detected on this node",
		Measurement: "Disk stalls detected",
		Unit:        metric.Unit_COUNT,
	}

	metaInternalBatchRPCMethodCount = metric.Metadata{
		Name:        "rpc.method.%s.recv",
		Help:        "Number of %s requests processed",
		Measurement: "RPCs",
		Unit:        metric.Unit_COUNT,
	}

	metaInternalBatchRPCCount = metric.Metadata{
		Name:        "rpc.batches.recv",
		Help:        "Number of batches processed",
		Measurement: "Batches",
		Unit:        metric.Unit_COUNT,
	}
)

// Cluster settings.
var (
	// graphiteEndpoint is host:port, if any, of Graphite metrics server.
	graphiteEndpoint = settings.RegisterStringSetting(
		settings.TenantWritable,
		"external.graphite.endpoint",
		"if nonempty, push server metrics to the Graphite or Carbon server at the specified host:port",
		"",
	).WithPublic()
	// graphiteInterval is how often metrics are pushed to Graphite, if enabled.
	graphiteInterval = settings.RegisterDurationSetting(
		settings.TenantWritable,
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

	BatchCount   *metric.Counter
	MethodCounts [roachpb.NumMethods]*metric.Counter
}

func makeNodeMetrics(reg *metric.Registry, histogramWindow time.Duration) nodeMetrics {
	nm := nodeMetrics{
		Latency:    metric.NewLatency(metaExecLatency, histogramWindow),
		Success:    metric.NewCounter(metaExecSuccess),
		Err:        metric.NewCounter(metaExecError),
		DiskStalls: metric.NewCounter(metaDiskStalls),
		BatchCount: metric.NewCounter(metaInternalBatchRPCCount),
	}

	for i := range nm.MethodCounts {
		method := roachpb.Method(i).String()
		meta := metaInternalBatchRPCMethodCount
		meta.Name = fmt.Sprintf(meta.Name, strings.ToLower(method))
		meta.Help = fmt.Sprintf(meta.Help, method)
		nm.MethodCounts[i] = metric.NewCounter(meta)
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
	initialStart bool // true if this is the first time this node has started
	txnMetrics   kvcoord.TxnMetrics

	// Used to signal when additional stores, if any, have been initialized.
	additionalStoreInitCh chan struct{}

	perReplicaServer kvserver.Server

	admissionController kvserver.KVAdmissionController

	tenantUsage multitenant.TenantUsageServer

	tenantSettingsWatcher *tenantsettingswatcher.Watcher

	spanConfigAccessor spanconfig.KVAccessor // powers the span configuration RPCs

	// Turns `Node.writeNodeStatus` into a no-op. This is a hack to enable the
	// COCKROACH_DEBUG_TS_IMPORT_FILE env var.
	suppressNodeStatus syncutil.AtomicBool

	testingErrorEvent func(context.Context, *roachpb.BatchRequest, error)
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
			NodeID:    kvserver.FirstNodeID,
			StoreID:   kvserver.FirstStoreID + roachpb.StoreID(i),
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
	kvAdmissionQ *admission.WorkQueue,
	storeGrantCoords *admission.StoreGrantCoordinators,
	tenantUsage multitenant.TenantUsageServer,
	tenantSettingsWatcher *tenantsettingswatcher.Watcher,
	spanConfigAccessor spanconfig.KVAccessor,
) *Node {
	var sqlExec *sql.InternalExecutor
	if execCfg != nil {
		sqlExec = execCfg.InternalExecutor
	}
	n := &Node{
		storeCfg:              cfg,
		stopper:               stopper,
		recorder:              recorder,
		metrics:               makeNodeMetrics(reg, cfg.HistogramWindowInterval),
		stores:                stores,
		txnMetrics:            txnMetrics,
		sqlExec:               sqlExec,
		clusterID:             clusterID,
		admissionController:   kvserver.MakeKVAdmissionController(kvAdmissionQ, storeGrantCoords),
		tenantUsage:           tenantUsage,
		tenantSettingsWatcher: tenantSettingsWatcher,
		spanConfigAccessor:    spanConfigAccessor,
		testingErrorEvent:     cfg.TestingKnobs.TestingResponseErrorEvent,
	}
	n.storeCfg.KVAdmissionController = n.admissionController
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
//
// addr, sqlAddr, and httpAddr are used to populate the Address,
// SQLAddress, and HTTPAddress fields respectively of the
// NodeDescriptor. If sqlAddr is not provided or empty, it is assumed
// that SQL connections are accepted at addr. Neither is ever assumed
// to carry HTTP, only if httpAddr is non-null will this node accept
// proxied traffic from other nodes.
func (n *Node) start(
	ctx context.Context,
	addr, sqlAddr, httpAddr net.Addr,
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
		HTTPAddress:     util.MakeUnresolvedAddr(httpAddr.Network(), httpAddr.String()),
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
		return errors.Wrapf(err, "couldn't gossip descriptor for node %d", n.Descriptor.NodeID)
	}

	// Create stores from the engines that were already initialized.
	for _, e := range state.initializedEngines {
		s := kvserver.NewStore(ctx, n.storeCfg, e, &n.Descriptor)
		if err := s.Start(ctx, n.stopper); err != nil {
			return errors.Wrap(err, "failed to start store")
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
		return errors.Wrap(err, "failed to initialize the gossip interface")
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
func (n *Node) SetDraining(drain bool, reporter func(int, redact.SafeString), verbose bool) error {
	return n.stores.VisitStores(func(s *kvserver.Store) error {
		s.SetDraining(drain, reporter, verbose)
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
			return errors.Wrap(err, "error allocating store ids")
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
		defer func() {
			nodeTicker.Stop()
			storesTicker.Stop()
			statusTicker.Stop()
		}()

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
func (n *Node) GetPebbleMetrics() []admission.StoreMetrics {
	var metrics []admission.StoreMetrics
	_ = n.stores.VisitStores(func(store *kvserver.Store) error {
		m := store.Engine().GetMetrics()
		metrics = append(
			metrics, admission.StoreMetrics{StoreID: int32(store.StoreID()), Metrics: m.Metrics})
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
	if n.suppressNodeStatus.Get() {
		return nil
	}
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
	ctx context.Context, tenID roachpb.TenantID, args *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if detail := checkNoUnknownRequest(args.Requests); detail != nil {
		var br roachpb.BatchResponse
		br.Error = roachpb.NewError(detail)
		return &br, nil
	}

	var br *roachpb.BatchResponse
	if err := n.stopper.RunTaskWithErr(ctx, "node.Node: batch", func(ctx context.Context) error {
		var reqSp spanForRequest
		// Shadow ctx from the outer function. Written like this to pass the linter.
		ctx, reqSp = n.setupSpanForIncomingRPC(ctx, tenID, args)
		// NB: wrapped to delay br evaluation to its value when returning.
		defer func() { reqSp.finish(ctx, br) }()
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

// incrementBatchCounters increments counters to track the batch and composite
// request methods.
func (n *Node) incrementBatchCounters(ba *roachpb.BatchRequest) {
	n.metrics.BatchCount.Inc(1)
	for _, ru := range ba.Requests {
		m := ru.GetInner().Method()
		n.metrics.MethodCounts[m].Inc(1)
	}
}

// Batch implements the roachpb.InternalServer interface.
func (n *Node) Batch(
	ctx context.Context, args *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {

	n.incrementBatchCounters(args)

	// NB: Node.Batch is called directly for "local" calls. We don't want to
	// carry the associated log tags forward as doing so makes adding additional
	// log tags more expensive and makes local calls differ from remote calls.
	ctx = n.storeCfg.AmbientCtx.ResetAndAnnotateCtx(ctx)

	tenantID, ok := roachpb.TenantFromContext(ctx)
	if !ok {
		tenantID = roachpb.SystemTenantID
	}

	// Requests from tenants don't have gateway node id set but are required for
	// the QPS based rebalancing to work. The GatewayNodeID is used as a proxy
	// for the locality of the origin of the request. The replica stats aggregate
	// all incoming BatchRequests and which localities they come from in order to
	// compute per second stats used for the rebalancing decisions.
	if args.GatewayNodeID == 0 && tenantID != roachpb.SystemTenantID {
		args.GatewayNodeID = n.Descriptor.NodeID
	}

	handle, err := n.admissionController.AdmitKVWork(ctx, tenantID, args)
	var br *roachpb.BatchResponse
	if err == nil {
		br, err = n.batchInternal(ctx, tenantID, args)
		n.admissionController.AdmittedKVWorkDone(handle)
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
	if buildutil.CrdbTestBuild && br.Error != nil && n.testingErrorEvent != nil {
		n.testingErrorEvent(ctx, args, errors.DecodeError(ctx, br.Error.EncodedError))
	}
	return br, nil
}

// spanForRequest is the retval of setupSpanForIncomingRPC. It groups together a
// few variables needed when finishing an RPC's span.
//
// finish() must be called when the span is done.
type spanForRequest struct {
	sp            *tracing.Span
	needRecording bool
	tenID         roachpb.TenantID
}

// finish finishes the span. If the span was recording and br is not nil, the
// recording is written to br.CollectedSpans.
func (sp *spanForRequest) finish(ctx context.Context, br *roachpb.BatchResponse) {
	var rec tracing.Recording
	// If we don't have a response, there's nothing to attach a trace to.
	// Nothing more for us to do.
	sp.needRecording = sp.needRecording && br != nil

	if !sp.needRecording {
		sp.sp.Finish()
		return
	}

	rec = sp.sp.FinishAndGetConfiguredRecording()
	if rec != nil {
		// Decide if the trace for this RPC, if any, will need to be redacted. It
		// needs to be redacted if the response goes to a tenant. In case the request
		// is local, then the trace might eventually go to a tenant (and tenID might
		// be set), but it will go to the tenant only indirectly, through the response
		// of a parent RPC. In that case, that parent RPC is responsible for the
		// redaction.
		//
		// Tenants get a redacted recording, i.e. with anything
		// sensitive stripped out of the verbose messages. However,
		// structured payloads stay untouched.
		needRedaction := sp.tenID != roachpb.SystemTenantID
		if needRedaction {
			if err := redactRecordingForTenant(sp.tenID, rec); err != nil {
				log.Errorf(ctx, "error redacting trace recording: %s", err)
				rec = nil
			}
		}
		br.CollectedSpans = append(br.CollectedSpans, rec...)
	}
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
	ctx context.Context, tenID roachpb.TenantID, ba *roachpb.BatchRequest,
) (context.Context, spanForRequest) {
	return setupSpanForIncomingRPC(ctx, tenID, ba, n.storeCfg.AmbientCtx.Tracer)
}

func setupSpanForIncomingRPC(
	ctx context.Context, tenID roachpb.TenantID, ba *roachpb.BatchRequest, tr *tracing.Tracer,
) (context.Context, spanForRequest) {
	var newSpan *tracing.Span
	parentSpan := tracing.SpanFromContext(ctx)
	localRequest := grpcutil.IsLocalRequestContext(ctx)
	// For non-local requests, we'll need to attach the recording to the outgoing
	// BatchResponse if the request is traced. We ignore whether the request is
	// traced or not here; if it isn't, the recording will be empty.
	needRecordingCollection := !localRequest
	if localRequest {
		// This is a local request which circumvented gRPC. Start a span now.
		ctx, newSpan = tracing.EnsureChildSpan(ctx, tr, tracing.BatchMethodName, tracing.WithServerSpanKind)
	} else if parentSpan == nil {
		// Non-local call. Tracing information comes from the request proto.
		var remoteParent tracing.SpanMeta
		if !ba.TraceInfo.Empty() {
			ctx, newSpan = tr.StartSpanCtx(ctx, tracing.BatchMethodName,
				tracing.WithRemoteParentFromTraceInfo(&ba.TraceInfo),
				tracing.WithServerSpanKind)
		} else {
			// For backwards compatibility with 21.2, if tracing info was passed as
			// gRPC metadata, we use it.
			var err error
			remoteParent, err = tracing.ExtractSpanMetaFromGRPCCtx(ctx, tr)
			if err != nil {
				log.Warningf(ctx, "error extracting tracing info from gRPC: %s", err)
			}
			ctx, newSpan = tr.StartSpanCtx(ctx, tracing.BatchMethodName,
				tracing.WithRemoteParentFromSpanMeta(remoteParent),
				tracing.WithServerSpanKind)
		}
	} else {
		// It's unexpected to find a span in the context for a non-local request.
		// Let's create a span for the RPC anyway.
		ctx, newSpan = tr.StartSpanCtx(ctx, tracing.BatchMethodName,
			tracing.WithParent(parentSpan),
			tracing.WithServerSpanKind)
	}

	return ctx, spanForRequest{
		needRecording: needRecordingCollection,
		tenID:         tenID,
		sp:            newSpan,
	}
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

	_, isSecondaryTenant := roachpb.TenantFromContext(ctx)

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
	var systemConfigUpdateCh <-chan struct{}
	for i := range args.Patterns {
		pattern := args.Patterns[i] // copy for closure
		switch pattern {
		// Note that we need to support clients subscribing to the system config
		// over this RPC even if the system config is no longer stored in gossip
		// in the host cluster. To achieve this, we special-case the system config
		// key and hook it up to the node's SystemConfigProvider. We need to
		// support this because tenant clusters are upgraded *after* the system
		// tenant of the host cluster. Tenant sql servers will still be expecting
		// this information to drive GC TTLs for their GC jobs. It's worth noting
		// that those zone configurations won't really map to reality, but that's
		// okay, we just need to tell the pods something.
		//
		// TODO(ajwerner): Remove support for the system config key in the
		// in 22.2, or leave it and make it a no-op.
		case gossip.KeyDeprecatedSystemConfig:
			var unregister func()
			systemConfigUpdateCh, unregister = n.storeCfg.SystemConfigProvider.RegisterSystemConfigChannel()
			defer unregister()
		default:
			callback := func(key string, content roachpb.Value) {
				callbackMu.Lock()
				defer callbackMu.Unlock()
				if entCClosed {
					return
				}
				var event roachpb.GossipSubscriptionEvent
				event.Key = key
				event.Content = content
				event.PatternMatched = pattern
				const maxBlockDur = 1 * time.Millisecond
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
	}
	handleSystemConfigUpdate := func() error {
		cfg := n.storeCfg.SystemConfigProvider.GetSystemConfig()
		ents := cfg.SystemConfigEntries
		if isSecondaryTenant {
			ents = kvtenant.GossipSubscriptionSystemConfigMask.Apply(ents)
		}
		var event roachpb.GossipSubscriptionEvent
		var content roachpb.Value
		if err := content.SetProto(&ents); err != nil {
			event.Error = roachpb.NewError(errors.Wrap(err, "could not marshal system config"))
		} else {
			event.Key = gossip.KeyDeprecatedSystemConfig
			event.Content = content
			event.PatternMatched = gossip.KeyDeprecatedSystemConfig
		}
		return stream.Send(&event)
	}
	for {
		select {
		case <-systemConfigUpdateCh:
			if err := handleSystemConfigUpdate(); err != nil {
				return errors.Wrap(err, "handling system config update")
			}
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
		case <-n.stopper.ShouldQuiesce():
			return stop.ErrUnavailable
		}
	}
}

// TenantSettings implements the roachpb.InternalServer interface.
func (n *Node) TenantSettings(
	args *roachpb.TenantSettingsRequest, stream roachpb.Internal_TenantSettingsServer,
) error {
	ctx := n.storeCfg.AmbientCtx.AnnotateCtx(stream.Context())
	ctxDone := ctx.Done()

	w := n.tenantSettingsWatcher
	if err := w.WaitForStart(ctx); err != nil {
		return stream.Send(&roachpb.TenantSettingsEvent{
			Error: errors.EncodeError(ctx, err),
		})
	}

	send := func(precedence roachpb.TenantSettingsPrecedence, overrides []roachpb.TenantSetting) error {
		log.VInfof(ctx, 1, "sending precedence %d: %v", precedence, overrides)
		return stream.Send(&roachpb.TenantSettingsEvent{
			Precedence:  precedence,
			Incremental: false,
			Overrides:   overrides,
		})
	}

	allOverrides, allCh := w.GetAllTenantOverrides()
	if err := send(roachpb.AllTenantsOverrides, allOverrides); err != nil {
		return err
	}

	tenantOverrides, tenantCh := w.GetTenantOverrides(args.TenantID)
	if err := send(roachpb.SpecificTenantOverrides, tenantOverrides); err != nil {
		return err
	}

	for {
		select {
		case <-allCh:
			// All-tenant overrides have changed, send them again.
			allOverrides, allCh = w.GetAllTenantOverrides()
			if err := send(roachpb.AllTenantsOverrides, allOverrides); err != nil {
				return err
			}

		case <-tenantCh:
			// Tenant-specific overrides have changed, send them again.
			tenantOverrides, tenantCh = w.GetTenantOverrides(args.TenantID)
			if err := send(roachpb.SpecificTenantOverrides, tenantOverrides); err != nil {
				return err
			}

		case <-ctxDone:
			return ctx.Err()

		case <-n.stopper.ShouldQuiesce():
			return stop.ErrUnavailable
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

// TokenBucket is part of the roachpb.InternalServer service.
func (n *Node) TokenBucket(
	ctx context.Context, in *roachpb.TokenBucketRequest,
) (*roachpb.TokenBucketResponse, error) {
	// Check tenant ID. Note that in production configuration, the tenant ID has
	// already been checked in the RPC layer (see rpc.tenantAuthorizer).
	if in.TenantID == 0 || in.TenantID == roachpb.SystemTenantID.ToUint64() {
		return &roachpb.TokenBucketResponse{
			Error: errors.EncodeError(ctx, errors.Errorf(
				"token bucket request with invalid tenant ID %d", in.TenantID,
			)),
		}, nil
	}
	tenantID := roachpb.MakeTenantID(in.TenantID)
	return n.tenantUsage.TokenBucketRequest(ctx, tenantID, in), nil
}

// NewTenantUsageServer is a hook for CCL code which implements the tenant usage
// server.
var NewTenantUsageServer = func(
	settings *cluster.Settings,
	db *kv.DB,
	executor *sql.InternalExecutor,
) multitenant.TenantUsageServer {
	return dummyTenantUsageServer{}
}

// dummyTenantUsageServer is a stub implementation of TenantUsageServer that
// errors out on all APIs.
type dummyTenantUsageServer struct{}

// TokenBucketRequest is defined in the TenantUsageServer interface.
func (dummyTenantUsageServer) TokenBucketRequest(
	ctx context.Context, tenantID roachpb.TenantID, in *roachpb.TokenBucketRequest,
) *roachpb.TokenBucketResponse {
	return &roachpb.TokenBucketResponse{
		Error: errors.EncodeError(ctx, errors.New("tenant usage requires a CCL binary")),
	}
}

// ReconfigureTokenBucket is defined in the TenantUsageServer interface.
func (dummyTenantUsageServer) ReconfigureTokenBucket(
	ctx context.Context,
	txn *kv.Txn,
	tenantID roachpb.TenantID,
	availableRU float64,
	refillRate float64,
	maxBurstRU float64,
	asOf time.Time,
	asOfConsumedRequestUnits float64,
) error {
	return errors.Errorf("tenant resource limits require a CCL binary")
}

// Metrics is defined in the TenantUsageServer interface.
func (dummyTenantUsageServer) Metrics() metric.Struct {
	return emptyMetricStruct{}
}

type emptyMetricStruct struct{}

var _ metric.Struct = emptyMetricStruct{}

func (emptyMetricStruct) MetricStruct() {}

// GetSpanConfigs implements the roachpb.InternalServer interface.
func (n *Node) GetSpanConfigs(
	ctx context.Context, req *roachpb.GetSpanConfigsRequest,
) (*roachpb.GetSpanConfigsResponse, error) {
	targets, err := spanconfig.TargetsFromProtos(req.Targets)
	if err != nil {
		return nil, err
	}
	records, err := n.spanConfigAccessor.GetSpanConfigRecords(ctx, targets)
	if err != nil {
		return nil, err
	}

	return &roachpb.GetSpanConfigsResponse{
		SpanConfigEntries: spanconfig.RecordsToEntries(records),
	}, nil
}

// UpdateSpanConfigs implements the roachpb.InternalServer interface.
func (n *Node) UpdateSpanConfigs(
	ctx context.Context, req *roachpb.UpdateSpanConfigsRequest,
) (*roachpb.UpdateSpanConfigsResponse, error) {
	// TODO(irfansharif): We want to protect ourselves from tenants creating
	// outlandishly large string buffers here and OOM-ing the host cluster. Is
	// the maximum protobuf message size enough of a safeguard?
	toUpsert, err := spanconfig.EntriesToRecords(req.ToUpsert)
	if err != nil {
		return nil, err
	}
	toDelete, err := spanconfig.TargetsFromProtos(req.ToDelete)
	if err != nil {
		return nil, err
	}
	if err := n.spanConfigAccessor.UpdateSpanConfigRecords(ctx, toDelete, toUpsert); err != nil {
		return nil, err
	}
	return &roachpb.UpdateSpanConfigsResponse{}, nil
}
