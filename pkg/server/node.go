// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvissettings"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/spanstatscollector"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvtenant"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitieswatcher"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/tenantsettingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/disk"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/pprofutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/grpcinterceptor"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
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

	metaBatchRequestsBytes = metric.Metadata{
		Name:        "batch_requests.bytes",
		Help:        `Total byte count of batch requests processed`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBatchResponsesBytes = metric.Metadata{
		Name:        "batch_responses.bytes",
		Help:        `Total byte count of batch responses received`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaCrossRegionBatchRequest = metric.Metadata{
		Name: "batch_requests.cross_region.bytes",
		Help: `Total byte count of batch requests processed cross region when region
		tiers are configured`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaCrossRegionBatchResponse = metric.Metadata{
		Name: "batch_responses.cross_region.bytes",
		Help: `Total byte count of batch responses received cross region when region
		tiers are configured`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaCrossZoneBatchRequest = metric.Metadata{
		Name: "batch_requests.cross_zone.bytes",
		Help: `Total byte count of batch requests processed cross zone within
		the same region when region and zone tiers are configured. However, if the
		region tiers are not configured, this count may also include batch data sent
		between different regions. Ensuring consistent configuration of region and
		zone tiers across nodes helps to accurately monitor the data transmitted.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaCrossZoneBatchResponse = metric.Metadata{
		Name: "batch_responses.cross_zone.bytes",
		Help: `Total byte count of batch responses received cross zone within the
		same region when region and zone tiers are configured. However, if the
		region tiers are not configured, this count may also include batch data
		received between different regions. Ensuring consistent configuration of
		region and zone tiers across nodes helps to accurately monitor the data
		transmitted.`,
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaActiveMuxRangeFeed = metric.Metadata{
		Name:        "rpc.streams.mux_rangefeed.active",
		Help:        `Number of currently running MuxRangeFeed streams`,
		Measurement: "Streams",
		Unit:        metric.Unit_COUNT,
	}
	metaTotalMuxRangeFeed = metric.Metadata{
		Name:        "rpc.streams.mux_rangefeed.recv",
		Help:        `Total number of MuxRangeFeed streams`,
		Measurement: "Streams",
		Unit:        metric.Unit_COUNT,
	}
)

// Cluster settings.
var (
	// graphiteEndpoint is host:port, if any, of Graphite metrics server.
	graphiteEndpoint = settings.RegisterStringSetting(
		settings.ApplicationLevel,
		"external.graphite.endpoint",
		"if nonempty, push server metrics to the Graphite or Carbon server at the specified host:port",
		"",
		settings.WithPublic)

	// graphiteInterval is how often metrics are pushed to Graphite, if enabled.
	graphiteInterval = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		graphiteIntervalKey,
		"the interval at which metrics are pushed to Graphite (if enabled)",
		10*time.Second,
		settings.NonNegativeDurationWithMaximum(maxGraphiteInterval),
		settings.WithPublic)

	RedactServerTracesForSecondaryTenants = settings.RegisterBoolSetting(
		settings.SystemOnly,
		"server.secondary_tenants.redact_trace.enabled",
		"if enabled, storage/KV trace results are redacted when returned to a virtual cluster",
		false,
		settings.WithName("trace.redact_at_virtual_cluster_boundary.enabled"),
	)

	slowRequestHistoricalStackThreshold = settings.RegisterDurationSetting(
		settings.SystemOnly,
		"kv.trace.slow_request_stacks.threshold",
		`duration spent in processing above any available stack history is appended to its trace, if automatic trace snapshots are enabled`,
		time.Second*30,
	)

	livenessRangeCompactInterval = settings.RegisterDurationSetting(
		settings.SystemOnly,
		"kv.liveness_range_compact.interval",
		`interval at which the liveness range is compacted. A value of 0 disables the periodic compaction`,
		0,
	)
)

// By default, stores will be started concurrently.
// To start stores sequentially set the environment variable
// COCKROACH_CONCURRENT_STORE_START=false
var startStoresAsync = envutil.EnvOrDefaultBool("COCKROACH_CONCURRENT_STORE_START", true)

type nodeMetrics struct {
	Latency metric.IHistogram
	Success *metric.Counter
	Err     *metric.Counter

	BatchCount                    *metric.Counter
	MethodCounts                  [kvpb.NumMethods]*metric.Counter
	BatchRequestsBytes            *metric.Counter
	BatchResponsesBytes           *metric.Counter
	CrossRegionBatchRequestBytes  *metric.Counter
	CrossRegionBatchResponseBytes *metric.Counter
	CrossZoneBatchRequestBytes    *metric.Counter
	CrossZoneBatchResponseBytes   *metric.Counter
	NumMuxRangeFeed               *metric.Counter
	ActiveMuxRangeFeed            *metric.Gauge
}

var _ rangefeed.RangefeedMetricsRecorder = &nodeMetrics{}

func makeNodeMetrics(reg *metric.Registry, histogramWindow time.Duration) *nodeMetrics {
	nm := &nodeMetrics{
		Latency: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaExecLatency,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		Success:                       metric.NewCounter(metaExecSuccess),
		Err:                           metric.NewCounter(metaExecError),
		BatchCount:                    metric.NewCounter(metaInternalBatchRPCCount),
		BatchRequestsBytes:            metric.NewCounter(metaBatchRequestsBytes),
		BatchResponsesBytes:           metric.NewCounter(metaBatchResponsesBytes),
		CrossRegionBatchRequestBytes:  metric.NewCounter(metaCrossRegionBatchRequest),
		CrossRegionBatchResponseBytes: metric.NewCounter(metaCrossRegionBatchResponse),
		CrossZoneBatchRequestBytes:    metric.NewCounter(metaCrossZoneBatchRequest),
		CrossZoneBatchResponseBytes:   metric.NewCounter(metaCrossZoneBatchResponse),
		ActiveMuxRangeFeed:            metric.NewGauge(metaActiveMuxRangeFeed),
		NumMuxRangeFeed:               metric.NewCounter(metaTotalMuxRangeFeed),
	}

	for i := range nm.MethodCounts {
		method := kvpb.Method(i).String()
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
func (nm *nodeMetrics) callComplete(d time.Duration, pErr *kvpb.Error) {
	if pErr != nil && pErr.TransactionRestart() == kvpb.TransactionRestart_NONE {
		nm.Err.Inc(1)
	} else {
		nm.Success.Inc(1)
	}
	nm.Latency.RecordValue(d.Nanoseconds())
}

// updateCrossLocalityMetricsOnBatchRequest updates nodeMetrics for batch
// requests processed on the node. The metrics being updated include 1. total
// byte count of batch requests processed 2. cross-region metrics, which monitor
// activities across different regions, and 3. cross-zone metrics, which monitor
// activities across different zones within the same region or in cases where
// region tiers are not configured. These metrics may include batches that were
// not successfully sent but were terminated at an early stage.
func (nm *nodeMetrics) updateCrossLocalityMetricsOnBatchRequest(
	comparisonResult roachpb.LocalityComparisonType, inc int64,
) {
	nm.BatchRequestsBytes.Inc(inc)
	switch comparisonResult {
	case roachpb.LocalityComparisonType_CROSS_REGION:
		nm.CrossRegionBatchRequestBytes.Inc(inc)
	case roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE:
		nm.CrossZoneBatchRequestBytes.Inc(inc)
	}
}

// updateCrossLocalityMetricsOnBatchResponse updates nodeMetrics for batch
// responses that are received back. It updates based on the comparisonResult
// parameter determined during the initial batch requests check. The underlying
// assumption is that the response should match the cross-region or cross-zone
// nature of the requests.
func (nm *nodeMetrics) updateCrossLocalityMetricsOnBatchResponse(
	comparisonResult roachpb.LocalityComparisonType, inc int64,
) {
	nm.BatchResponsesBytes.Inc(inc)
	switch comparisonResult {
	case roachpb.LocalityComparisonType_CROSS_REGION:
		nm.CrossRegionBatchResponseBytes.Inc(inc)
	case roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE:
		nm.CrossZoneBatchResponseBytes.Inc(inc)
	}
}

// UpdateOnRangefeedConnect increments rangefeed metrics when a new server
// rangefeed is added.
func (nm *nodeMetrics) UpdateMetricsOnRangefeedConnect() {
	nm.NumMuxRangeFeed.Inc(1)
	nm.ActiveMuxRangeFeed.Inc(1)
}

// UpdateMetricsOnRangefeedDisconnect decrements rangefeed metrics when one
// server rangefeed is disconnected.
func (nm *nodeMetrics) UpdateMetricsOnRangefeedDisconnect() {
	nm.UpdateMetricsOnRangefeedDisconnectBy(1)
}

// UpdateMetricsOnRangefeedDisconnectBy decrements rangefeed metrics by the
// given num argument when there are multiple rangefeed disconnects.
func (nm *nodeMetrics) UpdateMetricsOnRangefeedDisconnectBy(num int64) {
	nm.ActiveMuxRangeFeed.Dec(num)
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
	execCfg      *sql.ExecutorConfig      // For event logging
	stores       *kvserver.Stores         // Access to node-local stores
	metrics      *nodeMetrics
	recorder     *status.MetricsRecorder
	startedAt    int64
	lastUp       int64
	initialStart bool // true if this is the first time this node has started
	txnMetrics   kvcoord.TxnMetrics

	// Used to signal when additional stores, if any, have been initialized.
	additionalStoreInitCh chan struct{}

	perReplicaServer kvserver.Server

	tenantUsage multitenant.TenantUsageServer

	tenantSettingsWatcher *tenantsettingswatcher.Watcher
	tenantInfoWatcher     *tenantcapabilitieswatcher.Watcher

	spanConfigAccessor spanconfig.KVAccessor // powers the span configuration RPCs

	spanConfigReporter spanconfig.Reporter // powers the span configuration RPCs

	// Turns `Node.writeNodeStatus` into a no-op. This is a hack to enable the
	// COCKROACH_DEBUG_TS_IMPORT_FILE env var.
	suppressNodeStatus atomic.Bool

	testingErrorEvent func(context.Context, *kvpb.BatchRequest, error)

	// Used to collect samples for the key visualizer.
	spanStatsCollector *spanstatscollector.SpanStatsCollector

	// versionUpdateMu is used by the TenantSettings endpoint
	// to inform tenant servers of storage version changes.
	versionUpdateMu struct {
		syncutil.Mutex
		encodedVersion string
		updateCh       chan struct{}
	}
	proxySender kv.Sender

	diskSlowCoalescerMu struct {
		syncutil.Mutex
		lastDiskSlow map[roachpb.StoreID]time.Time
	}

	perConsumerCatchupLimiterMu struct {
		syncutil.Mutex
		limiters map[int64]*perConsumerLimiter
	}

	// Event handler called in logStructuredEvent. Used in tests only.
	// TODO(radu): this should be a testing knob.
	onStructuredEvent func(ctx context.Context, event logpb.EventPayload)

	// licenseEnforcer is used to enforce license policies on the cluster
	licenseEnforcer *license.Enforcer
}

var _ kvpb.InternalServer = &Node{}

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
	// We expect all the stores to be empty at this point, except for
	// the store cluster version key. Assert so.
	//
	// TODO(jackson): Eventually we should be able to avoid opening the
	// engines altogether until here.
	if err := assertEnginesEmpty(engines); err != nil {
		return nil, err
	}

	// We use our binary version to bootstrap the cluster.
	bootstrapVersion := clusterversion.ClusterVersion{Version: initCfg.latestVersion}
	if err := kvstorage.WriteClusterVersionToEngines(ctx, engines, bootstrapVersion); err != nil {
		return nil, err
	}

	clusterID := uuid.MakeV4()
	// TODO(andrei): It'd be cool if this method wouldn't do anything to engines
	// other than the first one, and let regular node startup code deal with them.
	for i, eng := range engines {
		sIdent := roachpb.StoreIdent{
			ClusterID: clusterID,
			NodeID:    kvstorage.FirstNodeID,
			StoreID:   kvstorage.FirstStoreID + roachpb.StoreID(i),
		}

		// Initialize the engine backing the store with the store ident and cluster
		// version.
		if err := kvstorage.InitEngine(ctx, eng, sIdent); err != nil {
			return nil, err
		}

		// Create first range, writing directly to engine. Note this does
		// not create the range, just its data. Only do this if this is the
		// first store.
		if i == 0 {
			initialValuesOpts := bootstrap.InitialValuesOpts{
				DefaultZoneConfig:       &initCfg.defaultZoneConfig,
				DefaultSystemZoneConfig: &initCfg.defaultSystemZoneConfig,
				Codec:                   keys.SystemSQLCodec,
			}
			for _, v := range bootstrap.VersionsWithInitialValues() {
				if initCfg.latestVersion == v.Version() {
					initialValuesOpts.OverrideKey = v
					break
				}
			}
			if initialValuesOpts.OverrideKey == 0 {
				if initCfg.latestVersion.Less(clusterversion.MinSupported.Version()) {
					// As an exception, we tolerate tests creating older versions; we just
					// use the minimum supported version.
					// TODO(radu): should we make sure there are no upgrades for versions
					// earlier than this still registered?
					initialValuesOpts.OverrideKey = clusterversion.MinSupported
				} else {
					return nil, errors.AssertionFailedf("cannot bootstrap at version %s", initCfg.latestVersion)
				}
			}

			initialValues, tableSplits, err := initialValuesOpts.GenerateInitialValues()
			if err != nil {
				return nil, err
			}

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
				timeutil.Now().UnixNano(), storeKnobs,
			); err != nil {
				return nil, err
			}
		}
	}

	// Note that we wrote initcfg.binaryVersion, that will always be the version
	// that inspectEngines determines.
	return inspectEngines(ctx, engines, initCfg.latestVersion, initCfg.minSupportedVersion)
}

// NewNode returns a new instance of Node.
//
// InitLogger() needs to be called before the Node is used.
func NewNode(
	cfg kvserver.StoreConfig,
	recorder *status.MetricsRecorder,
	reg *metric.Registry,
	stopper *stop.Stopper,
	txnMetrics kvcoord.TxnMetrics,
	stores *kvserver.Stores,
	clusterID *base.ClusterIDContainer,
	kvAdmissionQ *admission.WorkQueue,
	elasticCPUGrantCoord *admission.ElasticCPUGrantCoordinator,
	storeGrantCoords *admission.StoreGrantCoordinators,
	tenantUsage multitenant.TenantUsageServer,
	tenantSettingsWatcher *tenantsettingswatcher.Watcher,
	tenantInfoWatcher *tenantcapabilitieswatcher.Watcher,
	spanConfigAccessor spanconfig.KVAccessor,
	spanConfigReporter spanconfig.Reporter,
	proxySender kv.Sender,
	licenseEnforcer *license.Enforcer,
) *Node {
	n := &Node{
		storeCfg:              cfg,
		stopper:               stopper,
		recorder:              recorder,
		metrics:               makeNodeMetrics(reg, cfg.HistogramWindowInterval),
		stores:                stores,
		txnMetrics:            txnMetrics,
		execCfg:               nil, // filled in later by InitLogger()
		clusterID:             clusterID,
		tenantUsage:           tenantUsage,
		tenantSettingsWatcher: tenantSettingsWatcher,
		tenantInfoWatcher:     tenantInfoWatcher,
		spanConfigAccessor:    spanConfigAccessor,
		spanConfigReporter:    spanConfigReporter,
		testingErrorEvent:     cfg.TestingKnobs.TestingResponseErrorEvent,
		spanStatsCollector:    spanstatscollector.New(cfg.Settings),
		proxySender:           proxySender,
		licenseEnforcer:       licenseEnforcer,
	}
	n.perConsumerCatchupLimiterMu.limiters = make(map[int64]*perConsumerLimiter)
	n.diskSlowCoalescerMu.lastDiskSlow = make(map[roachpb.StoreID]time.Time)
	n.versionUpdateMu.updateCh = make(chan struct{})
	n.perReplicaServer = kvserver.MakeServer(&n.Descriptor, n.stores)
	return n
}

// InitLogger connects the Node to the Executor to be used for event
// logging.
func (n *Node) InitLogger(execCfg *sql.ExecutorConfig) {
	n.execCfg = execCfg
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
// Launches periodic store gossiping in a goroutine.
//
// addr, sqlAddr, and httpAddr are used to populate the Address,
// SQLAddress, and HTTPAddress fields respectively of the
// NodeDescriptor. If sqlAddr is not provided or empty, it is assumed
// that SQL connections are accepted at addr. Neither is ever assumed
// to carry HTTP, only if httpAddr is non-null will this node accept
// proxied traffic from other nodes.
func (n *Node) start(
	ctx, workersCtx context.Context,
	addr, sqlAddr, httpAddr net.Addr,
	state initState,
	initialStart bool,
	clusterName string,
	attrs roachpb.Attributes,
	locality roachpb.Locality,
	localityAddress []roachpb.LocalityAddress,
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
		ServerVersion:   n.storeCfg.Settings.Version.LatestVersion(),
		BuildTag:        build.GetInfo().Tag,
		StartedAt:       n.startedAt,
		HTTPAddress:     util.MakeUnresolvedAddr(httpAddr.Network(), httpAddr.String()),
	}

	// Track changes to the version setting to inform the tenant connector.
	n.storeCfg.Settings.Version.SetOnChange(n.notifyClusterVersionChange)
	// Also update the encoded copy immediately.
	n.notifyClusterVersionChange(ctx, n.storeCfg.Settings.Version.ActiveVersion(ctx))

	// Gossip the node descriptor to make this node addressable by node ID.
	n.storeCfg.Gossip.NodeID.Set(ctx, n.Descriptor.NodeID)
	if err := n.storeCfg.Gossip.SetNodeDescriptor(&n.Descriptor); err != nil {
		return errors.Wrapf(err, "couldn't gossip descriptor for node %d", n.Descriptor.NodeID)
	}

	// Create stores from engines that are already initialized. This uses a
	// channel to collect errors when starting stores in separate goroutines.
	// The channel is a buffered channel with the same size as the number of
	// stores so worker go routines will never wait on the channel.
	var sem *quotapool.IntPool
	if !startStoresAsync {
		sem = quotapool.NewIntPool("store start concurrency", 1)
	}
	engineErrC := make(chan error, len(state.initializedEngines))
	for i := range state.initializedEngines {
		engine := state.initializedEngines[i]
		err := n.stopper.RunAsyncTaskEx(ctx,
			stop.TaskOpts{TaskName: "initialize-stores", SpanOpt: stop.FollowsFromSpan, Sem: sem, WaitForSem: true},
			func(ctx context.Context) {
				start := timeutil.Now()
				s := kvserver.NewStore(ctx, n.storeCfg, engine, &n.Descriptor)
				if err := s.Start(workersCtx, n.stopper); err != nil {
					engineErrC <- errors.Wrap(err, "failed to start store")
					return
				}
				n.addStore(ctx, s)
				log.Infof(ctx, "initialized store s%s in %s (%d replicas)",
					s.StoreID(), timeutil.Since(start).Truncate(time.Millisecond), s.ReplicaCount())
				engineErrC <- nil
			})
		if err != nil {
			return err
		}
	}

	// Collect errors from the go routines and return the first error received.
	// This also waits for all stores to finish starting.
	for range state.initializedEngines {
		select {
		case <-n.stopper.ShouldQuiesce():
			return errors.New("shutting down")
		case <-ctx.Done():
			return ctx.Err()
		case err := <-engineErrC:
			if err != nil {
				return err
			}
		}
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
		if err := n.stopper.RunAsyncTask(workersCtx, "initialize-additional-stores", func(ctx context.Context) {
			if err := n.initializeAdditionalStores(ctx, state.uninitializedEngines); err != nil {
				log.Fatalf(ctx, "while initializing additional stores: %v", err)
			}
			close(n.additionalStoreInitCh)
		}); err != nil {
			close(n.additionalStoreInitCh)
			return err
		}
	}

	n.startComputePeriodicMetrics(n.stopper, base.DefaultMetricsSampleInterval)
	// Stores have been created, so can start providing tenant weights.
	n.storeCfg.KVAdmissionController.SetTenantWeightProvider(n, n.stopper)

	// Be careful about moving this line above where we start stores; store
	// upgrades rely on the fact that the cluster version has not been updated
	// via Gossip (we have upgrades that want to run only if the server starts
	// with a given cluster version, but not if the server starts with a lower
	// one and gets bumped immediately, which would be possible if gossip got
	// started earlier).
	n.startGossiping(workersCtx, n.stopper)

	var terminateCollector func() = nil

	if keyvissettings.Enabled.Get(&n.storeCfg.Settings.SV) {
		terminateCollector = n.enableSpanStatsCollector(ctx)
	}

	keyvissettings.Enabled.SetOnChange(&n.storeCfg.Settings.SV, func(ctx context.Context) {
		enabled := keyvissettings.Enabled.Get(&n.storeCfg.Settings.SV)
		if enabled {
			terminateCollector = n.enableSpanStatsCollector(ctx)
		} else if terminateCollector != nil {
			terminateCollector()
		}
	})

	log.Infof(ctx, "started with attributes %v", attrs.Attrs)

	n.startPeriodicLivenessCompaction(n.stopper, livenessRangeCompactInterval)
	return nil
}

func (n *Node) enableSpanStatsCollector(ctx context.Context) func() {
	collectorCtx, terminate := context.WithCancel(ctx)
	n.spanStatsCollector.Start(collectorCtx, n.stopper)
	return terminate
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
	cv := store.TODOEngine().MinVersion()
	if cv == (roachpb.Version{}) {
		// The store should have had a version written to it during the store
		// initialization process.
		log.Fatal(ctx, "attempting to add a store without a version")
	}
	store.TODOEngine().RegisterDiskSlowCallback(func(info pebble.DiskSlowInfo) {
		n.onStoreDiskSlow(n.AnnotateCtx(context.Background()), store.StoreID(), info)
	})
	store.TODOEngine().RegisterLowDiskSpaceCallback(func(info pebble.LowDiskSpaceInfo) {
		n.onLowDiskSpace(n.AnnotateCtx(context.Background()), store.StoreID(), info)
	})
	n.stores.AddStore(store)
	n.recorder.AddStore(store)
}

func (n *Node) onStoreDiskSlow(
	ctx context.Context, storeID roachpb.StoreID, info pebble.DiskSlowInfo,
) {
	// Emit a disk slow event for each slow store that reports a disk slow event.
	// After diskSlowClearInterval since the last disk slow event, we emit a disk
	// slowness cleared event. Any other disk slow events for the same store in
	// between are ignored as duplicates.
	//
	// We reuse the maxSyncInterval, as it's unlikely for slow events to last longer
	// than this without significant corrective action (eg. crashes).
	//
	// TODO(bilal): Store stats on events observed and coalesced events.
	diskSlowClearInterval := fs.MaxSyncDuration.Get(&n.storeCfg.Settings.SV)
	if storeID == 0 {
		// Uninitialized store; cannot coalesce events for an unknown store.
		return
	}
	n.diskSlowCoalescerMu.Lock()
	last, ok := n.diskSlowCoalescerMu.lastDiskSlow[storeID]
	n.diskSlowCoalescerMu.lastDiskSlow[storeID] = timeutil.Now()
	n.diskSlowCoalescerMu.Unlock()
	// NB: A zero / nonexistent value for last means that we are not currently running
	// the below async task, and this is a new disk slowness event that needs a
	// start/clear pair emitted for it.
	if ok && last != (time.Time{}) {
		// This is a duplicate event for this store.
		return
	}

	_ = n.stopper.RunAsyncTask(ctx, "clear-disk-slow", func(ctx context.Context) {
		ev := &eventpb.DiskSlownessDetected{}
		ev.StoreID = int32(storeID)
		ev.NodeID = int32(n.Descriptor.NodeID)
		ev.CommonDetails().Timestamp = timeutil.Now().UnixNano()

		// logStructuredEvent does an async write to system.eventlog, however it could
		// do a synchronous log write to the node logger, so we call it within this async
		// task as opposed to outside it.
		n.logStructuredEvent(ctx, logpb.EventPayload(ev))

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				clearing := false
				n.diskSlowCoalescerMu.Lock()
				last := n.diskSlowCoalescerMu.lastDiskSlow[storeID]
				if timeutil.Since(last) > diskSlowClearInterval {
					clearing = true
					delete(n.diskSlowCoalescerMu.lastDiskSlow, storeID)
				}
				n.diskSlowCoalescerMu.Unlock()

				if !clearing {
					continue
				}

				// Clear the event and break out of the loop.
				ev := &eventpb.DiskSlownessCleared{}
				ev.StoreID = int32(storeID)
				ev.NodeID = int32(n.Descriptor.NodeID)
				ev.CommonDetails().Timestamp = timeutil.Now().UnixNano()

				n.logStructuredEvent(ctx, logpb.EventPayload(ev))

				return
			case <-n.stopper.ShouldQuiesce():
				return
			}
		}
	})

}

func (n *Node) onLowDiskSpace(
	ctx context.Context, storeID roachpb.StoreID, info pebble.LowDiskSpaceInfo,
) {
	ev := &eventpb.LowDiskSpace{
		StoreID:          int32(storeID),
		NodeID:           int32(n.Descriptor.NodeID),
		PercentThreshold: int32(info.PercentThreshold),
		AvailableBytes:   info.AvailBytes,
		TotalBytes:       info.TotalBytes,
	}
	ev.CommonDetails().Timestamp = timeutil.Now().UnixNano()
	n.logStructuredEvent(ctx, logpb.EventPayload(ev))
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
func (n *Node) initializeAdditionalStores(ctx context.Context, engines []storage.Engine) error {
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
			if err := kvstorage.InitEngine(ctx, eng, sIdent); err != nil {
				return err
			}

			s := kvserver.NewStore(ctx, n.storeCfg, eng, &n.Descriptor)
			if err := s.Start(ctx, n.stopper); err != nil {
				return err
			}

			n.addStore(ctx, s)
			log.Infof(ctx, "initialized new store s%s", s.StoreID())

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
	n.stopper.AddCloser(stop.CloserFn(func() { n.stores.CloseDiskMonitors() }))
	_ = stopper.RunAsyncTask(ctx, "compute-metrics", func(ctx context.Context) {
		// Compute periodic stats at the same frequency as metrics are sampled.
		ticker := time.NewTicker(interval)
		previousMetrics := make(map[*kvserver.Store]*storage.MetricsForInterval)
		defer ticker.Stop()
		for tick := 0; ; tick++ {
			select {
			case <-ticker.C:
				if err := n.computeMetricsPeriodically(ctx, previousMetrics, tick); err != nil {
					log.Errorf(ctx, "failed computing periodic metrics: %s", err)
				}
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// startPeriodicLivenessCompaction starts a loop where it periodically compacts
// the liveness range.
func (n *Node) startPeriodicLivenessCompaction(
	stopper *stop.Stopper, livenessRangeCompactInterval *settings.DurationSetting,
) {
	ctx := n.AnnotateCtx(context.Background())

	// getCompactionInterval() returns the interval at which the liveness range is
	// set to be compacted. If the interval is set to 0, the period is set to the
	// max possible duration because a value of 0 cause the ticker to panic.
	getCompactionInterval := func() time.Duration {
		interval := livenessRangeCompactInterval.Get(&n.storeCfg.Settings.SV)
		if interval == 0 {
			interval = math.MaxInt64
		}
		return interval
	}

	if err := stopper.RunAsyncTask(ctx, "liveness-compaction", func(ctx context.Context) {
		interval := getCompactionInterval()
		ticker := time.NewTicker(interval)

		intervalChangeChan := make(chan time.Duration)

		// Update the compaction interval when the setting changes.
		livenessRangeCompactInterval.SetOnChange(&n.storeCfg.Settings.SV, func(ctx context.Context) {
			// intervalChangeChan is used to signal the compaction loop that the
			// interval has changed. Avoid blocking the main goroutine that is
			// responsible for handling all settings updates.
			select {
			case intervalChangeChan <- getCompactionInterval():
			default:
			}
		})

		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Find the liveness replica in order to compact it.
				_ = n.stores.VisitStores(func(store *kvserver.Store) error {
					store.VisitReplicas(func(repl *kvserver.Replica) bool {
						span := repl.Desc().KeySpan().AsRawSpanWithNoLocals()
						if keys.NodeLivenessSpan.Overlaps(span) {

							// The CompactRange() method expects the start and end keys to be
							// encoded.
							startEngineKey :=
								storage.EngineKey{
									Key: span.Key,
								}.Encode()

							endEngineKey :=
								storage.EngineKey{
									Key: span.EndKey,
								}.Encode()

							timeBeforeCompaction := timeutil.Now()
							if err := store.StateEngine().CompactRange(startEngineKey, endEngineKey); err != nil {
								log.Errorf(ctx, "failed compacting liveness replica: %+v with error: %s", repl, err)
							}

							log.Infof(ctx, "finished compacting liveness replica: %+v and it took: %+v",
								repl, timeutil.Since(timeBeforeCompaction))
						}
						return true
					})
					return nil
				})
			case newInterval := <-intervalChangeChan:
				ticker.Reset(newInterval)
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	}); err != nil {
		log.Errorf(ctx, "failed to start the async liveness compaction task")
	}

}

// updateNodeRangeCount updates the internal counter of the total ranges across
// all stores. This value is used to make a decision on whether the node should
// use expiration leases (see Replica.shouldUseExpirationLeaseRLocked).
func (n *Node) updateNodeRangeCount() {
	var count int64
	_ = n.stores.VisitStores(func(store *kvserver.Store) error {
		count += store.Metrics().RangeCount.Value()
		return nil
	})
	n.storeCfg.RangeCount.Store(count)
}

// computeMetricsPeriodically instructs each store to compute the value of
// complicated metrics.
func (n *Node) computeMetricsPeriodically(
	ctx context.Context, storeToMetrics map[*kvserver.Store]*storage.MetricsForInterval, tick int,
) error {
	err := n.stores.VisitStores(func(store *kvserver.Store) error {
		if newMetrics, err := store.ComputeMetricsPeriodically(ctx, storeToMetrics[store], tick); err != nil {
			log.Warningf(ctx, "%s: unable to compute metrics: %s", store, err)
		} else {
			if storeToMetrics[store] == nil {
				storeToMetrics[store] = &storage.MetricsForInterval{
					FlushWriteThroughput: newMetrics.Flush.WriteThroughput,
				}
			} else {
				storeToMetrics[store].FlushWriteThroughput = newMetrics.Flush.WriteThroughput
			}
			if err := newMetrics.LogWriter.FsyncLatency.Write(&storeToMetrics[store].WALFsyncLatency); err != nil {
				return err
			}
			if newMetrics.WAL.Failover.FailoverWriteAndSyncLatency != nil {
				if err := newMetrics.WAL.Failover.FailoverWriteAndSyncLatency.Write(
					&storeToMetrics[store].WALFailoverWriteAndSyncLatency); err != nil {
					return err
				}
			}
		}
		return nil
	})
	n.updateNodeRangeCount()
	n.storeCfg.KVFlowStreamTokenProvider.UpdateMetricGauges()
	return err
}

// UpdateIOThreshold relays the supplied IOThreshold to the same method on the
// designated Store.
func (n *Node) UpdateIOThreshold(id roachpb.StoreID, threshold *admissionpb.IOThreshold) {
	s, err := n.stores.GetStore(id)
	if err != nil {
		log.Errorf(n.AnnotateCtx(context.Background()), "%v", err)
	}
	s.UpdateIOThreshold(threshold)
}

// diskStatsMap encapsulates all the logic for populating DiskStats for
// admission.StoreMetrics.
type diskStatsMap struct {
	provisionedRate map[roachpb.StoreID]base.ProvisionedRateSpec
	diskMonitors    map[roachpb.StoreID]kvserver.DiskStatsMonitor
}

func (dsm *diskStatsMap) tryPopulateAdmissionDiskStats(
	clusterProvisionedBandwidth int64,
) (stats map[roachpb.StoreID]admission.DiskStats, err error) {
	if dsm.empty() {
		return stats, nil
	}
	stats = make(map[roachpb.StoreID]admission.DiskStats, len(dsm.diskMonitors))
	for id, monitor := range dsm.diskMonitors {
		cumulativeStats, err := monitor.CumulativeStats()
		if err != nil {
			return map[roachpb.StoreID]admission.DiskStats{}, err
		}

		provisionedBandwidth := clusterProvisionedBandwidth
		spec, ok := dsm.provisionedRate[id]
		if ok && spec.ProvisionedBandwidth > 0 {
			provisionedBandwidth = spec.ProvisionedBandwidth
		}
		stats[id] = admission.DiskStats{
			ProvisionedBandwidth: provisionedBandwidth,
			BytesRead:            uint64(cumulativeStats.BytesRead()),
			BytesWritten:         uint64(cumulativeStats.BytesWritten()),
		}
	}
	return stats, nil
}

func (dsm *diskStatsMap) empty() bool {
	return len(dsm.provisionedRate) == 0
}

// monitorManagerInterface abstracts disk.MonitorManager for testing purposes.
type monitorManagerInterface interface {
	Monitor(string) (kvserver.DiskStatsMonitor, error)
}

func (dsm *diskStatsMap) initDiskStatsMap(
	specs []base.StoreSpec, engines []storage.Engine, diskManager monitorManagerInterface,
) error {
	*dsm = diskStatsMap{
		provisionedRate: make(map[roachpb.StoreID]base.ProvisionedRateSpec),
		diskMonitors:    make(map[roachpb.StoreID]kvserver.DiskStatsMonitor),
	}
	for i := range engines {
		if specs[i].Path == "" || specs[i].InMemory {
			continue
		}
		id, err := kvstorage.ReadStoreIdent(context.Background(), engines[i])
		if err != nil {
			return err
		}
		monitor, err := diskManager.Monitor(specs[i].Path)
		if err != nil {
			return err
		}
		dsm.provisionedRate[id.StoreID] = specs[i].ProvisionedRateSpec
		dsm.diskMonitors[id.StoreID] = monitor
	}
	return nil
}

func (dsm *diskStatsMap) closeDiskMonitors() {
	for _, monitor := range dsm.diskMonitors {
		monitor.Close()
	}
}

type diskMonitorManager disk.MonitorManager

// Monitor wraps disk.MonitorManager to satisfy monitorManagerInterface.
func (mm *diskMonitorManager) Monitor(path string) (kvserver.DiskStatsMonitor, error) {
	return (*disk.MonitorManager)(mm).Monitor(path)
}

func (n *Node) registerEnginesForDiskStatsMap(
	specs []base.StoreSpec, engines []storage.Engine, diskManager *diskMonitorManager,
) (admission.PebbleMetricsProvider, error) {
	pmp := &nodePebbleMetricsProvider{n: n}
	if err := pmp.diskStatsMap.initDiskStatsMap(specs, engines, diskManager); err != nil {
		return nil, err
	}
	if err := n.stores.RegisterDiskMonitors(pmp.diskStatsMap.diskMonitors); err != nil {
		return nil, err
	}
	return pmp, nil
}

func (n *Node) makeStoreRegistryProvider() admission.MetricsRegistryProvider {
	return &storeMetricsRegistryProvider{n: n}
}

type nodePebbleMetricsProvider struct {
	n            *Node
	diskStatsMap diskStatsMap
}

// GetPebbleMetrics implements admission.PebbleMetricsProvider.
func (pmp *nodePebbleMetricsProvider) GetPebbleMetrics() []admission.StoreMetrics {
	clusterProvisionedBandwidth := kvadmission.ProvisionedBandwidth.Get(
		&pmp.n.storeCfg.Settings.SV)
	storeIDToDiskStats, err := pmp.diskStatsMap.tryPopulateAdmissionDiskStats(clusterProvisionedBandwidth)
	if err != nil {
		log.Warningf(context.Background(), "%v",
			errors.Wrapf(err, "unable to populate disk stats"))
	}
	var metrics []admission.StoreMetrics
	_ = pmp.n.stores.VisitStores(func(store *kvserver.Store) error {
		eng := store.TODOEngine()
		m := eng.GetMetrics()
		opts := eng.GetPebbleOptions()
		memTableSizeForStopWrites := opts.MemTableSize * uint64(opts.MemTableStopWritesThreshold)
		diskStats := admission.DiskStats{ProvisionedBandwidth: clusterProvisionedBandwidth}
		if s, ok := storeIDToDiskStats[store.StoreID()]; ok {
			diskStats = s
		}
		metrics = append(metrics, admission.StoreMetrics{
			StoreID:                   store.StoreID(),
			Metrics:                   m.Metrics,
			WriteStallCount:           m.WriteStallCount,
			DiskStats:                 diskStats,
			MemTableSizeForStopWrites: memTableSizeForStopWrites,
		})
		return nil
	})
	return metrics
}

// Close implements admission.PebbleMetricsProvider.
func (pmp *nodePebbleMetricsProvider) Close() {
	pmp.diskStatsMap.closeDiskMonitors()
}

type storeMetricsRegistryProvider struct {
	n *Node
}

// GetMetricsRegistry implements admission.MetricsRegistryProvider.
func (mrp *storeMetricsRegistryProvider) GetMetricsRegistry(
	storeID roachpb.StoreID,
) *metric.Registry {
	return mrp.n.stores.GetStoreMetricRegistry(storeID)
}

// GetTenantWeights implements kvserver.TenantWeightProvider.
func (n *Node) GetTenantWeights() kvadmission.TenantWeights {
	weights := kvadmission.TenantWeights{
		Node: make(map[uint64]uint32),
	}
	_ = n.stores.VisitStores(func(store *kvserver.Store) error {
		sw := make(map[uint64]uint32)
		weights.Stores = append(weights.Stores, kvadmission.TenantWeightsForStore{
			StoreID: store.StoreID(),
			Weights: sw,
		})
		store.VisitReplicas(func(r *kvserver.Replica) bool {
			tid, valid := r.TenantID()
			if valid {
				weights.Node[tid.ToUint64()]++
				sw[tid.ToUint64()]++
			}
			return true
		})
		return nil
	})
	return weights
}

func startGraphiteStatsExporter(
	ctx context.Context,
	stopper *stop.Stopper,
	recorder *status.MetricsRecorder,
	st *cluster.Settings,
) {
	ctx = logtags.AddTag(ctx, "graphite stats exporter", nil)
	pm := metric.MakePrometheusExporter()

	_ = stopper.RunAsyncTask(ctx, "graphite-exporter", func(ctx context.Context) {
		var timer timeutil.Timer
		defer timer.Stop()
		for {
			timer.Reset(graphiteInterval.Get(&st.SV))
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
				endpoint := graphiteEndpoint.Get(&st.SV)
				if endpoint != "" {
					if err := recorder.ExportToGraphite(ctx, endpoint, &pm); err != nil {
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
	if err := startup.RunIdempotentWithRetry(ctx,
		n.stopper.ShouldQuiesce(),
		"kv write node status", func(ctx context.Context) error {
			return n.writeNodeStatus(ctx, 0 /* alertTTL */, false /* mustExist */)
		}); err != nil {
		return errors.Wrap(err, "error recording initial status summaries")
	}
	return n.stopper.RunAsyncTask(ctx, "write-node-status",
		func(ctx context.Context) {
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
	if n.suppressNodeStatus.Load() {
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
			if err := n.storeCfg.Gossip.IterateInfos(gossip.KeyNodeDescPrefix, func(k string, info gossip.Info) error {
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
	var event logpb.EventPayload
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

	n.logStructuredEvent(ctx, event)
}

func (n *Node) logStructuredEvent(ctx context.Context, event logpb.EventPayload) {
	// Ensure that the event goes to log files even if LogRangeAndNodeEvents is
	// disabled (which means skip the system.eventlog _table_).
	log.StructuredEvent(ctx, severity.INFO, event)

	if !n.storeCfg.LogRangeAndNodeEvents {
		return
	}

	// InsertEventRecord processes the event asynchronously.
	sql.InsertEventRecords(ctx, n.execCfg,
		sql.LogToSystemTable|sql.LogToDevChannelIfVerbose, /* not LogExternally: we already call log.StructuredEvent above */
		event,
	)

	if n.onStructuredEvent != nil {
		n.onStructuredEvent(ctx, event)
	}
}

// If we receive a (proto-marshaled) kvpb.BatchRequest whose Requests contain
// a message type unknown to this node, we will end up with a zero entry in the
// slice. If we don't error out early, this breaks all sorts of assumptions and
// usually ends in a panic.
func checkNoUnknownRequest(reqs []kvpb.RequestUnion) *kvpb.UnsupportedRequestError {
	for _, req := range reqs {
		if req.GetValue() == nil {
			return &kvpb.UnsupportedRequestError{}
		}
	}
	return nil
}

func (n *Node) batchInternal(
	ctx context.Context, tenID roachpb.TenantID, args *kvpb.BatchRequest,
) (br *kvpb.BatchResponse, _ error) {
	if detail := checkNoUnknownRequest(args.Requests); detail != nil {
		br = &kvpb.BatchResponse{}
		br.Error = kvpb.NewError(detail)
		return br, nil
	}

	var reqSp spanForRequest
	ctx, reqSp = n.setupSpanForIncomingRPC(ctx, tenID, args)
	// NB: wrapped to delay br evaluation to its value when returning.
	defer func() {
		var redact redactOpt
		if RedactServerTracesForSecondaryTenants.Get(&n.storeCfg.Settings.SV) {
			redact = redactIfTenantRequest
		} else {
			redact = dontRedactEvenIfTenantRequest
		}
		reqSp.finish(br, redact)
	}()
	if log.HasSpan(ctx) {
		log.Eventf(ctx, "node received request: %s", args.Summary())
		defer log.Event(ctx, "node sending response")
	}

	tStart := timeutil.Now()
	handle, err := n.storeCfg.KVAdmissionController.AdmitKVWork(ctx, tenID, args)
	if err != nil {
		return nil, err
	}
	ctx = handle.AnnotateCtx(ctx)

	var writeBytes *kvadmission.StoreWriteBytes
	defer func() {
		n.storeCfg.KVAdmissionController.AdmittedKVWorkDone(handle, writeBytes)
		writeBytes.Release()
	}()

	// If a proxy attempt is requested, we copy the request to prevent evaluation
	// from modifying the request. There are places on the server that can modify
	// the request, and we can't keep these modifications if we later proxy it.
	// Note we only ShallowCopy, so care must be taken with internal changes.
	// For reference some of the places are:
	//  SetActiveTimestamp - sets the Header.Timestamp
	//  maybeStripInFlightWrites - can modify internal EndTxn requests
	//  tryBumpBatchTimestamp - can modify the txn.ReadTimestamp
	// TODO(baptist): Other code copies the BatchRequest, in some cases
	// unnecessarily, to prevent modifying the passed in request. We should clean
	// up the contract of the Send method to allow modifying the request or more
	// strictly enforce that the callee is not allowed to change it.
	var originalRequest *kvpb.BatchRequest
	if args.ProxyRangeInfo != nil {
		originalRequest = args.ShallowCopy()
	}
	var pErr *kvpb.Error
	br, writeBytes, pErr = n.stores.SendWithWriteBytes(ctx, args)
	if pErr != nil {
		if originalRequest != nil {
			if proxyResponse := n.maybeProxyRequest(ctx, originalRequest, pErr); proxyResponse != nil {
				// If the proxy request succeeded then return its result instead of
				// our error. If not, use our original error.
				return proxyResponse, nil
			}
		}

		br = &kvpb.BatchResponse{}
		if pErr.Index != nil && keyvissettings.Enabled.Get(&n.storeCfg.Settings.SV) {
			// Tell the SpanStatsCollector about the requests in this BatchRequest,
			// but stop when we reach the requests that were not attempted
			// due to an error.
			for i, union := range args.Requests {
				if int32(i) == pErr.Index.Index {
					break
				}
				arg := union.GetInner()
				n.spanStatsCollector.Increment(arg.Header().Span())
			}
		}
		log.VErrEventf(ctx, 3, "error from stores.Send: %s", pErr)
	} else {
		if keyvissettings.Enabled.Get(&n.storeCfg.Settings.SV) {
			// Tell the SpanStatsCollector about the requests in this BatchRequest.
			for _, union := range args.Requests {
				arg := union.GetInner()
				n.spanStatsCollector.Increment(arg.Header().Span())
			}
		}
	}
	if br.Error != nil {
		panic(kvpb.ErrorUnexpectedlySet(n.stores, br))
	}
	if timeutil.Since(tStart) > slowRequestHistoricalStackThreshold.Get(&n.storeCfg.Settings.SV) {
		tracing.SpanFromContext(ctx).MaybeRecordStackHistory(tStart)
	}

	// If the sender cancelled the context they may not wait around for the
	// replica to notice the cancellation and return a response. For this reason,
	// we log the server-side trace of the cancelled request to help debug what
	// the request was doing at the time it noticed the cancellation.
	//
	// To avoid log spam for now we only log the trace if the request was an
	// ExportRequest.
	if pErr != nil && ctx.Err() != nil && args.IsSingleExportRequest() {
		if sp := tracing.SpanFromContext(ctx); sp != nil {
			recording := sp.GetConfiguredRecording()
			if recording.Len() != 0 {
				log.Infof(ctx, "batch request %s failed with error: %v\ntrace:\n%s", args.String(),
					pErr.GoError(), recording)
			}
		}
	}

	n.metrics.callComplete(timeutil.Since(tStart), pErr)
	br.Error = pErr

	return br, nil
}

// maybeProxyRequest is called after the server returned an error and it
// attempts to proxy the request if it can. We attempt o proxy requests if two
// primary conditions are met:
// 1) The ProxyRangeInfo header is set on the request indicating the client
// would like us to proxy this request if we can't evaluate it.
// 2) Local evaluation has resulted in a NotLeaseHolderError which matches the
// ProxyRangeInfo from the client.
// If these conditions are met, attempt to send the request through our local
// DistSender stack and use that result instead of our error.
func (n *Node) maybeProxyRequest(
	ctx context.Context, ba *kvpb.BatchRequest, pErr *kvpb.Error,
) *kvpb.BatchResponse {
	// NB: We don't handle StoreNotFound or RangeNotFound errors. If we want to
	// support proxy requests through non-replicas we could proxy those errors
	// as well.
	var nlhe *kvpb.NotLeaseHolderError
	if ok := errors.As(pErr.GetDetail(), &nlhe); !ok {
		log.VEventf(ctx, 2, "non-proxyable errors %v", pErr)
		return nil
	}
	// If because we think the client has
	// stale information, see if our information would update the clients
	// state. If so, rather than proxying this request, fail back to the
	// client first.
	leaseCompatible := nlhe.Lease != nil && ba.ProxyRangeInfo.Lease.Sequence >= nlhe.Lease.Sequence
	descCompatible := ba.ProxyRangeInfo.Desc.Generation >= nlhe.RangeDesc.Generation
	if !leaseCompatible || !descCompatible {
		log.VEventf(
			ctx,
			2,
			"out-of-date client information on proxy request %v != %v",
			ba.ProxyRangeInfo,
			pErr,
		)
		return nil
	}

	log.VEventf(ctx, 2, "proxy request for %v after local error %v", ba, pErr)
	br, pErr := n.proxySender.Send(ctx, ba)
	if pErr == nil {
		log.VEvent(ctx, 2, "proxy request completed")
		return br
	}
	// It is rare to get here on a proxy request because wrapping normally
	// happens in DistSender.sendProxyRequest. Pessimistically wrap the error
	// and convert this to a ProxyFailedError which may become an ambiguous
	// error on the other side.
	log.VEventf(ctx, 2, "proxy attempt resulted in error %v", pErr)
	br = &kvpb.BatchResponse{}
	br.Error = kvpb.NewError(kvpb.NewProxyFailedError(pErr.GoError()))
	return br
}

// getLocalityComparison takes gatewayNodeID as input and returns the locality
// comparison result between the gateway node and the current node. This result
// indicates whether the two nodes are located in different regions or zones.
func (n *Node) getLocalityComparison(
	ctx context.Context, gatewayNodeID roachpb.NodeID,
) roachpb.LocalityComparisonType {
	gossip := n.storeCfg.Gossip
	if gossip == nil {
		log.VInfof(ctx, 2, "gossip is not configured")
		return roachpb.LocalityComparisonType_UNDEFINED
	}

	// In separate-process multi-tenant mode the gatewayNodeID is 0.
	if gatewayNodeID == 0 {
		if _, ok := roachpb.ClientTenantFromContext(ctx); ok {
			return roachpb.LocalityComparisonType_UNDEFINED
		}
	}
	gatewayNodeDesc, err := gossip.GetNodeDescriptor(gatewayNodeID)
	if err != nil {
		log.VInfof(ctx, 2,
			"failed to perform look up for node descriptor %v", err)
		return roachpb.LocalityComparisonType_UNDEFINED
	}

	comparisonResult, regionValid, zoneValid := n.Descriptor.Locality.CompareWithLocality(gatewayNodeDesc.Locality)
	if !regionValid {
		log.VInfof(ctx, 5, "unable to determine if the given nodes are cross region")
	}
	if !zoneValid {
		log.VInfof(ctx, 5, "unable to determine if the given nodes are cross zone")
	}

	return comparisonResult
}

// incrementBatchCounters increments counters to track the batch and composite
// request methods.
func (n *Node) incrementBatchCounters(ba *kvpb.BatchRequest) {
	n.metrics.BatchCount.Inc(1)
	for _, ru := range ba.Requests {
		m := ru.GetInner().Method()
		n.metrics.MethodCounts[m].Inc(1)
	}
}

// Batch implements the kvpb.InternalServer interface.
func (n *Node) Batch(ctx context.Context, args *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
	n.incrementBatchCounters(args)

	// NB: Node.Batch is called directly for "local" calls. We don't want to
	// carry the associated log tags forward as doing so makes adding additional
	// log tags more expensive and makes local calls differ from remote calls.
	ctx = n.storeCfg.AmbientCtx.ResetAndAnnotateCtxPrealloc(ctx)

	comparisonResult := n.getLocalityComparison(ctx, args.GatewayNodeID)
	n.metrics.updateCrossLocalityMetricsOnBatchRequest(comparisonResult, int64(args.Size()))

	tenantID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok {
		tenantID = roachpb.SystemTenantID
	} else {
		// We had this tag before the ResetAndAnnotateCtx() call above.
		ctx = logtags.AddTag(ctx, "tenant", tenantID)
	}

	// If the node is collecting a CPU profile with labels, and the sender has set
	// pprof labels in the BatchRequest, then we apply them to the context that is
	// going to execute the BatchRequest. These labels will help correlate server
	// side CPU profile samples to the sender.
	if len(args.ProfileLabels) != 0 && n.execCfg.Settings.CPUProfileType() == cluster.CPUProfileWithLabels {
		var undo func()
		ctx, undo = pprofutil.SetProfilerLabels(ctx, args.ProfileLabels...)
		defer undo()
	}

	// Requests from tenants don't have gateway node id set but are required for
	// the QPS based rebalancing to work. The GatewayNodeID is used as a proxy
	// for the locality of the origin of the request. The replica stats aggregate
	// all incoming BatchRequests and which localities they come from in order to
	// compute per second stats used for the rebalancing decisions.
	if args.GatewayNodeID == 0 && tenantID != roachpb.SystemTenantID {
		args.GatewayNodeID = n.Descriptor.NodeID
	}

	br, err := n.batchInternal(ctx, tenantID, args)

	// We always return errors via BatchResponse.Error so structure is
	// preserved; plain errors are presumed to be from the RPC
	// framework and not from cockroach.
	if err != nil {
		if br == nil {
			br = &kvpb.BatchResponse{}
		}
		if br.Error != nil {
			log.Fatalf(
				ctx, "attempting to return both a plain error (%s) and kvpb.Error (%s)", err, br.Error,
			)
		}
		br.Error = kvpb.NewError(err)
	}

	n.metrics.updateCrossLocalityMetricsOnBatchResponse(comparisonResult, int64(br.Size()))
	if buildutil.CrdbTestBuild && br.Error != nil && n.testingErrorEvent != nil {
		n.testingErrorEvent(ctx, args, errors.DecodeError(ctx, br.Error.EncodedError))
	}
	return br, nil
}

// BatchStream implements the kvpb.InternalServer interface.
func (n *Node) BatchStream(stream kvpb.Internal_BatchStreamServer) error {
	return n.batchStreamImpl(stream, func(ba *kvpb.BatchRequest) error {
		return stream.RecvMsg(ba)
	})
}

func (n *Node) batchStreamImpl(
	stream interface {
		Context() context.Context
		Send(response *kvpb.BatchResponse) error
	},
	recvMsg func(*kvpb.BatchRequest) error,
) error {
	ctx := stream.Context()
	for {
		argsAlloc := new(struct {
			args kvpb.BatchRequest
			reqs [1]kvpb.RequestUnion
		})
		args := &argsAlloc.args
		args.Requests = argsAlloc.reqs[:0]

		err := recvMsg(args)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		br, err := n.Batch(ctx, args)
		if err != nil {
			return err
		}
		err = stream.Send(br)
		if err != nil {
			return err
		}
	}
}

func (n *Node) AsDRPCBatchServer() kvpb.DRPCBatchServer {
	return (*drpcNode)(n)
}

type drpcNode Node

func (n *drpcNode) Batch(
	ctx context.Context, request *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	return (*Node)(n).Batch(ctx, request)
}

func (n *drpcNode) BatchStream(stream kvpb.DRPCBatch_BatchStreamStream) error {
	return (*Node)(n).batchStreamImpl(stream, func(ba *kvpb.BatchRequest) error {
		return stream.(interface {
			RecvMsg(request *kvpb.BatchRequest) error
		}).RecvMsg(ba)
	})
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

type redactOpt bool

const (
	redactIfTenantRequest         redactOpt = true
	dontRedactEvenIfTenantRequest redactOpt = false
)

// finish finishes the span. If the span was recording and br is not nil, the
// recording is written to br.CollectedSpans.
func (sp *spanForRequest) finish(br *kvpb.BatchResponse, redactOpt redactOpt) {
	var rec tracingpb.Recording
	// If we don't have a response, there's nothing to attach a trace to.
	// Nothing more for us to do.
	sp.needRecording = sp.needRecording && br != nil

	if !sp.needRecording {
		sp.sp.Finish()
		return
	}

	rec = sp.sp.FinishAndGetConfiguredRecording()
	if rec != nil {
		// Decide if the trace for this RPC, if any, will need to be redacted. In
		// general, responses sent to a tenant are redacted unless indicated
		// otherwise by the cluster setting below.
		//
		// Even if the recording sent to a tenant is redacted (anything sensitive
		// is stripped out of the verbose messages), structured payloads
		// stay untouched.
		needRedaction := sp.tenID != roachpb.SystemTenantID && redactOpt == redactIfTenantRequest
		if needRedaction {
			redactRecording(rec)
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
	ctx context.Context, tenID roachpb.TenantID, ba *kvpb.BatchRequest,
) (context.Context, spanForRequest) {
	return setupSpanForIncomingRPC(ctx, tenID, ba, n.storeCfg.AmbientCtx.Tracer)
}

func setupSpanForIncomingRPC(
	ctx context.Context, tenID roachpb.TenantID, ba *kvpb.BatchRequest, tr *tracing.Tracer,
) (context.Context, spanForRequest) {
	var newSpan *tracing.Span
	remoteParent := !ba.TraceInfo.Empty()
	if !remoteParent {
		// This is either a local request which circumvented gRPC, or a remote
		// request that didn't specify tracing information. We make a child span
		// if the incoming request would like to be traced.
		ctx, newSpan = tracing.ChildSpan(ctx,
			grpcinterceptor.BatchMethodName, tracing.WithServerSpanKind)
	} else {
		// Non-local call. Tracing information comes from the request proto.

		// Sanity check - we're not expecting a span in the context. If there was
		// one, it'd be unclear what needRecordingCollection should be set to.
		parentSpan := tracing.SpanFromContext(ctx)
		if parentSpan != nil {
			log.Fatalf(ctx, "unexpected span found in non-local RPC: %s", parentSpan)
		}

		ctx, newSpan = tr.StartSpanCtx(
			ctx, grpcinterceptor.BatchMethodName,
			tracing.WithRemoteParentFromTraceInfo(ba.TraceInfo),
			tracing.WithServerSpanKind)
	}

	if newSpan != nil {
		newSpan.SetLazyTag("request", ba.ShallowCopy())
	}
	return ctx, spanForRequest{
		// For non-local requests, we'll need to attach the recording to the
		// outgoing BatchResponse if the request is traced. We ignore whether the
		// request is traced or not here; if it isn't, the recording will be empty.
		needRecording: remoteParent,
		tenID:         tenID,
		sp:            newSpan,
	}
}

func tenantPrefix(tenID roachpb.TenantID) roachpb.RSpan {
	// TODO(nvanbenschoten): consider caching this span.
	prefix := roachpb.RKey(keys.MakeTenantPrefix(tenID))
	return roachpb.RSpan{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
	}
}

// filterRangeLookupResultsForTenant extracts the tenant ID from the context.
// It filters descs to only include the prefix which have a start key in the
// tenant's span. If there is no tenant in the context, it will filter all
// the descriptors.
func filterRangeLookupResponseForTenant(
	ctx context.Context, descs []roachpb.RangeDescriptor,
) []roachpb.RangeDescriptor {
	tenID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok {
		// If we do not know the tenant, don't permit any pre-fetching.
		return []roachpb.RangeDescriptor{}
	}
	rs := tenantPrefix(tenID)
	truncated := descs[:0]
	// We say that any range which has a start key within the tenant prefix is
	// fair game for the tenant to know about.
	for _, d := range descs {
		if !rs.ContainsKey(d.StartKey) {
			break
		}
		truncated = append(truncated, d)
	}
	return truncated
}

// RangeLookup implements the kvpb.InternalServer interface.
func (n *Node) RangeLookup(
	ctx context.Context, req *kvpb.RangeLookupRequest,
) (*kvpb.RangeLookupResponse, error) {
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
	resp := new(kvpb.RangeLookupResponse)
	if err != nil {
		resp.Error = kvpb.NewError(err)
	} else {
		resp.Descriptors = rs
		resp.PrefetchedDescriptors = filterRangeLookupResponseForTenant(ctx, preRs)
	}
	return resp, nil
}

// lockedMuxStream provides support for concurrent calls to Send. The underlying
// MuxRangeFeedServer (default grpc.Stream) is not safe for concurrent calls to
// Send.
type lockedMuxStream struct {
	wrapped kvpb.Internal_MuxRangeFeedServer
	sendMu  syncutil.Mutex
}

func (s *lockedMuxStream) SendIsThreadSafe() {}

func (s *lockedMuxStream) Send(e *kvpb.MuxRangeFeedEvent) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return s.wrapped.Send(e)
}

// perConsumerLimiter is a ConcurrentRequestLimiter for a given mux rangefeed
// consumer. It is stored in the Node as long as there are active MuxRangeFeed
// requests the related ConsumerID.
type perConsumerLimiter struct {
	l limit.ConcurrentRequestLimiter
	c int
}

func (n *Node) perConsumerCatchupScanLimiter(
	consumerID int64, sv *settings.Values,
) *limit.ConcurrentRequestLimiter {
	n.perConsumerCatchupLimiterMu.Lock()
	defer n.perConsumerCatchupLimiterMu.Unlock()
	if _, ok := n.perConsumerCatchupLimiterMu.limiters[consumerID]; ok {
		n.perConsumerCatchupLimiterMu.limiters[consumerID].c++
	} else {
		n.perConsumerCatchupLimiterMu.limiters[consumerID] = &perConsumerLimiter{
			l: makePerConsumerScanLimiter(consumerID, sv),
			c: 1,
		}
	}
	return &n.perConsumerCatchupLimiterMu.limiters[consumerID].l
}

func (n *Node) releasePerConsumerCatchup(consumerID int64) {
	n.perConsumerCatchupLimiterMu.Lock()
	defer n.perConsumerCatchupLimiterMu.Unlock()
	if l, ok := n.perConsumerCatchupLimiterMu.limiters[consumerID]; ok {
		l.c--
		if l.c == 0 {
			delete(n.perConsumerCatchupLimiterMu.limiters, consumerID)
		}
	}
}

func makePerConsumerScanLimiter(
	consumerID int64, sv *settings.Values,
) limit.ConcurrentRequestLimiter {
	getLimit := func() int {
		if lim := kvserver.PerConsumerCatchupLimit.Get(sv); lim > 0 {
			return int(lim)
		}
		// If the setting is disable with an in-flight limiter, set the
		// limit to infinity.
		return math.MaxInt
	}
	l := limit.MakeConcurrentRequestLimiter(
		fmt.Sprintf("PerConsumerCatchupLimit-%d", consumerID),
		getLimit())
	kvserver.PerConsumerCatchupLimit.SetOnChange(sv, func(ctx context.Context) {
		l.SetLimit(getLimit())
	})
	return l
}

// defaultRangefeedConsumerID returns a random ConsumerID. Used by
// MuxRangeFeed calls where the user hasn't specified a consumer ID.
func (n *Node) defaultRangefeedConsumerID() int64 {
	return unique.GenerateUniqueInt(
		unique.ProcessUniqueID(n.execCfg.NodeInfo.NodeID.SQLInstanceID()))
}

// MuxRangeFeed implements the roachpb.InternalServer interface.
func (n *Node) MuxRangeFeed(muxStream kvpb.Internal_MuxRangeFeedServer) error {
	lockedMuxStream := &lockedMuxStream{wrapped: muxStream}

	// All context created below should derive from this context, which is
	// cancelled once MuxRangeFeed exits.
	ctx, cancel := context.WithCancel(n.AnnotateCtx(muxStream.Context()))
	defer cancel()

	sm := &rangefeed.StreamManager{}
	if kvserver.RangefeedUseBufferedSender.Get(&n.storeCfg.Settings.SV) {
		sm = rangefeed.NewStreamManager(rangefeed.NewBufferedSender(lockedMuxStream), n.metrics)
	} else {
		sm = rangefeed.NewStreamManager(rangefeed.NewUnbufferedSender(lockedMuxStream), n.metrics)
	}

	if err := sm.Start(ctx, n.stopper); err != nil {
		return err
	}
	defer sm.Stop(ctx)

	var limiter *limit.ConcurrentRequestLimiter
	var consumerID int64
	defer func() {
		n.releasePerConsumerCatchup(consumerID)
	}()

	for {
		select {
		case err := <-sm.Error():
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopper.ShouldQuiesce():
			return stop.ErrUnavailable
		default:
			req, err := muxStream.Recv()
			if err != nil {
				return err
			}

			if req.CloseStream {
				// For client close stream requests, streams are disconnected by calling
				// the Disconnector (registration.Disconnect).
				sm.DisconnectStream(req.StreamID,
					kvpb.NewError(kvpb.NewRangeFeedRetryError(kvpb.RangeFeedRetryError_REASON_RANGEFEED_CLOSED)))
				continue
			}

			tags := &logtags.Buffer{}
			tags = tags.Add("r", req.RangeID)
			tags = tags.Add("sm", req.Replica.StoreID)
			tags = tags.Add("sid", req.StreamID)
			if req.ConsumerID != 0 {
				tags = tags.Add("cid", req.ConsumerID)
			}
			streamCtx := logtags.AddTags(ctx, tags)

			streamSink := sm.NewStream(req.StreamID, req.RangeID)

			// Get the per-consumer catchup limiter if it is
			// enabled. We currently assume that a single
			// MuxRangeFeed call will only contain streams for the
			// same consumer.
			if kvserver.PerConsumerCatchupLimit.Get(n.execCfg.SV()) > 0 {
				if consumerID == 0 {
					if req.ConsumerID == 0 {
						req.ConsumerID = n.defaultRangefeedConsumerID()
					}
					consumerID = req.ConsumerID
				}
				if req.ConsumerID != 0 && consumerID != req.ConsumerID {
					log.Warningf(ctx, "ignoring previously unseen consumer ID %d, using %d",
						req.ConsumerID, consumerID)
				}

				if limiter == nil {
					limiter = n.perConsumerCatchupScanLimiter(consumerID, n.execCfg.SV())
				}
			}

			// Rangefeed attempts to register rangefeed a request over the specified
			// span. If registration fails, it returns an error. Otherwise, it returns
			// nil error with its disconnector without blocking on rangefeed
			// completion. Events are then sent to the provided streamSink.
			// Disconnector returned can be used to shut down rangefeed from the
			// stream manager. If rangefeed disconnects with an error after being
			// successfully registered, it calls streamSink.SendError.
			if disconnector, err := n.stores.RangeFeed(streamCtx, req, streamSink, limiter); err != nil {
				streamSink.SendError(kvpb.NewError(err))
			} else {
				sm.AddStream(req.StreamID, disconnector)
			}
		}
	}
}

// ResetQuorum implements the kvpb.InternalServer interface.
func (n *Node) ResetQuorum(
	ctx context.Context, req *kvpb.ResetQuorumRequest,
) (_ *kvpb.ResetQuorumResponse, rErr error) {
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
	available := desc.Replicas().CanMakeProgress(func(rDesc roachpb.ReplicaDescriptor) bool {
		return n.storeCfg.NodeLiveness.GetNodeVitalityFromCache(rDesc.NodeID).IsLive(livenesspb.ReplicaProgress)
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
		n.clusterID.Get(),
		n.storeCfg.Settings,
		n.storeCfg.Tracer(),
		conn,
		n.storeCfg.Clock.Now(),
		desc,
		toReplicaDescriptor,
	); err != nil {
		return nil, err
	}
	log.Infof(ctx, "sent empty snapshot to %s", toReplicaDescriptor)

	return &kvpb.ResetQuorumResponse{}, nil
}

// GossipSubscription implements the kvpb.InternalServer interface.
func (n *Node) GossipSubscription(
	args *kvpb.GossipSubscriptionRequest, stream kvpb.Internal_GossipSubscriptionServer,
) error {
	ctx := n.storeCfg.AmbientCtx.AnnotateCtx(stream.Context())
	ctxDone := ctx.Done()

	_, isSecondaryTenant := roachpb.ClientTenantFromContext(ctx)

	// Register a callback for each of the requested patterns. We don't want to
	// block the gossip callback goroutine on a slow consumer, so we instead
	// handle all communication asynchronously. We could pick a channel size and
	// say that if the channel ever blocks, terminate the subscription. Doing so
	// feels fragile, though, especially during the initial information dump.
	// Instead, we say that if the channel ever blocks for more than some
	// duration, terminate the subscription.
	entC := make(chan *kvpb.GossipSubscriptionEvent, 256)
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
			//nolint:deferloop TODO(#137605)
			defer unregister()
		default:
			callback := func(key string, content roachpb.Value) {
				callbackMu.Lock()
				defer callbackMu.Unlock()
				if entCClosed {
					return
				}
				var event kvpb.GossipSubscriptionEvent
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
			//nolint:deferloop TODO(#137605)
			defer unregister()
		}
	}
	handleSystemConfigUpdate := func() error {
		cfg := n.storeCfg.SystemConfigProvider.GetSystemConfig()
		ents := cfg.SystemConfigEntries
		if isSecondaryTenant {
			ents = kvtenant.GossipSubscriptionSystemConfigMask.Apply(ents)
		}
		var event kvpb.GossipSubscriptionEvent
		var content roachpb.Value
		if err := content.SetProto(&ents); err != nil {
			event.Error = kvpb.NewError(errors.Wrap(err, "could not marshal system config"))
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
				err := kvpb.NewErrorf("subscription terminated due to slow consumption")
				log.Warningf(ctx, "%v", err)
				e = &kvpb.GossipSubscriptionEvent{Error: err}
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

func (n *Node) waitForTenantWatcherReadiness(
	ctx context.Context,
) (*tenantsettingswatcher.Watcher, *tenantcapabilitieswatcher.Watcher, error) {
	settingsWatcher := n.tenantSettingsWatcher
	if err := settingsWatcher.WaitForStart(ctx); err != nil {
		return nil, nil, err
	}

	infoWatcher := n.tenantInfoWatcher
	if err := infoWatcher.WaitForStart(ctx); err != nil {
		return nil, nil, err
	}

	return settingsWatcher, infoWatcher, nil
}

// TenantSettings implements the kvpb.InternalServer interface.
func (n *Node) TenantSettings(
	args *kvpb.TenantSettingsRequest, stream kvpb.Internal_TenantSettingsServer,
) error {
	ctx := n.storeCfg.AmbientCtx.AnnotateCtx(stream.Context())
	ctxDone := ctx.Done()

	settingsWatcher, infoWatcher, err := n.waitForTenantWatcherReadiness(ctx)
	if err != nil {
		return stream.Send(&kvpb.TenantSettingsEvent{
			Error: errors.EncodeError(ctx, err),
		})
	}

	// Do we even have a record for this tenant?
	tInfo, infoCh, found := infoWatcher.GetInfo(args.TenantID)
	if !found {
		return stream.Send(&kvpb.TenantSettingsEvent{
			Error: errors.EncodeError(ctx, &kvpb.MissingRecordError{}),
		})
	}

	sendSettings := func(precedence kvpb.TenantSettingsEvent_Precedence, overrides []kvpb.TenantSetting, incremental bool) error {
		log.VInfof(ctx, 1, "sending precedence %d (incremental=%v): %v", precedence, overrides, incremental)
		return stream.Send(&kvpb.TenantSettingsEvent{
			EventType:   kvpb.TenantSettingsEvent_SETTING_EVENT,
			Precedence:  precedence,
			Incremental: incremental,
			Overrides:   overrides,
		})
	}

	sendTenantInfo := func(
		fakePrecedence kvpb.TenantSettingsEvent_Precedence,
		tInfo tenantcapabilities.Entry,
	) error {
		log.VInfof(ctx, 1, "sending tenant info: %+v", tInfo)
		// Note: we are piggy-backing on the TenantSetting streaming RPC
		// to send non-setting data. This must be careful to work on
		// SQL servers running previous versions of the CockroachDB code.
		// To that end, we make the TenantSettingEvent "look like"
		// a no-op setting change.
		return stream.Send(&kvpb.TenantSettingsEvent{
			EventType: kvpb.TenantSettingsEvent_METADATA_EVENT,
			// IMPORTANT: setting Incremental to true ensures existing
			// settings are preserved client-side in previous-version
			// clients that do not know about the EventType field.
			Incremental: true,
			// An empty Overrides slice results in no-op client-side.
			Overrides:  nil,
			Precedence: fakePrecedence,

			// These are the actual fields for our update.
			Name:         tInfo.Name,
			Capabilities: tInfo.TenantCapabilities,
			// TODO(knz): remove the cast after we fix the dependency cycle
			// between the protobufs.
			ServiceMode: uint32(tInfo.ServiceMode),
			DataState:   uint32(tInfo.DataState),
			// Flow the cluster init grace period end ts. Secondary tenant cannot
			// access the KV location where this is stored.
			ClusterInitGracePeriodEndTS: n.licenseEnforcer.GetClusterInitGracePeriodEndTS().Unix(),
		})
	}

	// Sanity check: this ensures that someone notices if the proto
	// definition changes but the code below is not adapted.
	if numPrecedences := len(kvpb.TenantSettingsEvent_Precedence_value); numPrecedences != 3 {
		err := errors.AssertionFailedf("programming error: expected 3 precedence values, got %d", numPrecedences)
		logcrash.ReportOrPanic(ctx, &n.execCfg.Settings.SV, "%w", err)
		return err
	}

	// Send the initial state.

	// Note: for compatibility with pre-23.1 tenant clients, it is
	// important that all event types that convey data about the
	// initial state of a tenant service be sent after the first
	// SETTING_EVENT message that communicates overrides (for one of
	// the two precedence levels), and before the second one (for the
	// other precedence level).
	//
	// This is necessary because of a combination of factors:
	//
	// - For compatibility with older version tenant clients,
	//   all non-setting event types must fake being a no-op
	//   setting event (see the docstring on the EventType field).
	//   A no-op fake setting event must have `Incremental` set to
	//   `true`.
	// - Meanwhile, older version tenant clients also assert that the
	//   very first message sent by the server must have `Incremental`
	//   set to `false`.
	//
	//   This means we can only send events of other types after the
	//   first setting overrides event.
	//
	// - Then, separately, newer version tenant clients also
	//   synchronize their startup on the reception of a tenant
	//   setting event for each precedence level. This is because
	//   these tenant clients must, in turn, remain compatible with
	//   older version *KV servers* that do not send other event types
	//   and send just 2 setting overrides events initially.
	//
	//   This means we cannot send other event types after the second
	//   setting overrides event.

	// Send the setting overrides for one precedence level.
	const firstPrecedenceLevel = kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES
	allOverrides, allCh := settingsWatcher.GetAllTenantOverrides(ctx)

	// Inject the current storage logical version as an override; as the
	// tenant server needs this to start up.
	verSetting, versionUpdateCh := n.getVersionSettingWithUpdateCh(ctx)
	allOverrides = append(allOverrides, verSetting)
	if err := sendSettings(kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES, allOverrides, false /* incremental */); err != nil {
		return err
	}

	// Send the initial tenant metadata. See the explanatory comment
	// above for details.
	if err := sendTenantInfo(firstPrecedenceLevel, tInfo); err != nil {
		return err
	}

	// Then send the initial setting overrides for the other precedence
	// level. This is the payload that will let the tenant client
	// connector signal readiness.
	tenantOverrides, tenantCh := settingsWatcher.GetTenantOverrides(ctx, args.TenantID)
	if err := sendSettings(kvpb.TenantSettingsEvent_TENANT_SPECIFIC_OVERRIDES, tenantOverrides, false /* incremental */); err != nil {
		return err
	}

	for {
		select {
		case <-versionUpdateCh:
			// The storage version has changed, send it again.
			verSetting, versionUpdateCh = n.getVersionSettingWithUpdateCh(ctx)
			if err := sendSettings(kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES,
				[]kvpb.TenantSetting{verSetting},
				true /* incremental */); err != nil {
				return err
			}

		case <-infoCh:
			// Tenant metadata has changed, send it again.
			tInfo, infoCh, _ = infoWatcher.GetInfo(args.TenantID)
			const anyPrecedenceLevel = kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES
			if err := sendTenantInfo(anyPrecedenceLevel, tInfo); err != nil {
				return err
			}

		case <-allCh:
			// All-tenant overrides have changed, send them again.
			// TODO(multitenant): We can optimize this by only sending the delta since the last
			// update, with Incremental set to true.

			// Inject the current storage logical version as an override to
			// work around the situation where the `system.tenant_settings`
			// has a wrong version data (see #125702).
			//
			// TODO(multitenant): remove this override when the minimum
			// supported version is 24.1+.
			verSetting, versionUpdateCh = n.getVersionSettingWithUpdateCh(ctx)
			allOverrides, allCh = settingsWatcher.GetAllTenantOverrides(ctx)
			actualOverrides := append(
				append([]kvpb.TenantSetting{}, allOverrides...),
				verSetting,
			)
			if err := sendSettings(kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES, actualOverrides, false /* incremental */); err != nil {
				return err
			}

		case <-tenantCh:
			// Tenant-specific overrides have changed, send them again.
			// TODO(multitenant): We can optimize this by only sending the delta since the last
			// update, with Incremental set to true.
			tenantOverrides, tenantCh = settingsWatcher.GetTenantOverrides(ctx, args.TenantID)
			if err := sendSettings(kvpb.TenantSettingsEvent_TENANT_SPECIFIC_OVERRIDES, tenantOverrides, false /* incremental */); err != nil {
				return err
			}

		case <-ctxDone:
			return ctx.Err()

		case <-n.stopper.ShouldQuiesce():
			return stop.ErrUnavailable
		}
	}
}

// getVersionSettingWithUpdateCh returns the current encoded cluster
// version as a TenantSetting, and a channel that is closed whenever
// the version changes.
func (n *Node) getVersionSettingWithUpdateCh(
	ctx context.Context,
) (kvpb.TenantSetting, <-chan struct{}) {
	n.versionUpdateMu.Lock()
	defer n.versionUpdateMu.Unlock()

	setting := kvpb.TenantSetting{
		InternalKey: clusterversion.KeyVersionSetting,
		Value: settings.EncodedValue{
			Type:  settings.VersionSettingValueType,
			Value: n.versionUpdateMu.encodedVersion,
		},
	}

	return setting, n.versionUpdateMu.updateCh
}

func (n *Node) notifyClusterVersionChange(
	ctx context.Context, activeVersion clusterversion.ClusterVersion,
) {
	n.versionUpdateMu.Lock()
	defer n.versionUpdateMu.Unlock()

	encodedVersion, err := protoutil.Marshal(&activeVersion)
	if err != nil {
		logcrash.ReportOrPanic(ctx, &n.execCfg.Settings.SV, "%w", err)
		return
	}
	n.versionUpdateMu.encodedVersion = string(encodedVersion)
	// Notify listeners.
	close(n.versionUpdateMu.updateCh)
	n.versionUpdateMu.updateCh = make(chan struct{})
}

// Join implements the kvpb.InternalServer service. This is the
// "connectivity" API; individual CRDB servers are passed in a --join list and
// the join targets are addressed through this API.
func (n *Node) Join(
	ctx context.Context, req *kvpb.JoinNodeRequest,
) (*kvpb.JoinNodeResponse, error) {
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
	// upgrades. See https://github.com/cockroachdb/cockroach/pull/48843 for
	// details.
	if err := n.storeCfg.NodeLiveness.CreateLivenessRecord(ctx, nodeID); err != nil {
		return nil, err
	}

	log.Infof(ctx, "allocated IDs: n%d, s%d", nodeID, storeID)

	return &kvpb.JoinNodeResponse{
		ClusterID:     n.clusterID.Get().GetBytes(),
		NodeID:        int32(nodeID),
		StoreID:       int32(storeID),
		ActiveVersion: &activeVersion.Version,
	}, nil
}

// TokenBucket is part of the kvpb.InternalServer service.
func (n *Node) TokenBucket(
	ctx context.Context, in *kvpb.TokenBucketRequest,
) (*kvpb.TokenBucketResponse, error) {
	// Check tenant ID. Note that in production configuration, the tenant ID has
	// already been checked in the RPC layer (see rpc.tenantAuthorizer).
	if in.TenantID == 0 || in.TenantID == roachpb.SystemTenantID.ToUint64() {
		return &kvpb.TokenBucketResponse{
			Error: errors.EncodeError(ctx, errors.Errorf(
				"token bucket request with invalid tenant ID %d", in.TenantID,
			)),
		}, nil
	}
	tenantID := roachpb.MustMakeTenantID(in.TenantID)
	return n.tenantUsage.TokenBucketRequest(ctx, tenantID, in), nil
}

// NewTenantUsageServer is a hook for CCL code which implements the tenant usage
// server.
var NewTenantUsageServer = func(
	settings *cluster.Settings,
	db *kv.DB,
	ief isql.DB,
) multitenant.TenantUsageServer {
	return dummyTenantUsageServer{}
}

// dummyTenantUsageServer is a stub implementation of TenantUsageServer that
// errors out on all APIs.
type dummyTenantUsageServer struct{}

// TokenBucketRequest is defined in the TenantUsageServer interface.
func (dummyTenantUsageServer) TokenBucketRequest(
	ctx context.Context, tenantID roachpb.TenantID, in *kvpb.TokenBucketRequest,
) *kvpb.TokenBucketResponse {
	return &kvpb.TokenBucketResponse{
		Error: errors.EncodeError(ctx, errors.New("tenant usage requires a CCL binary")),
	}
}

// ReconfigureTokenBucket is defined in the TenantUsageServer interface.
func (dummyTenantUsageServer) ReconfigureTokenBucket(
	ctx context.Context,
	txn isql.Txn,
	tenantID roachpb.TenantID,
	availableTokens float64,
	refillRate float64,
	maxBurstTokens float64,
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

// GetSpanConfigs implements the kvpb.InternalServer interface.
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

// GetAllSystemSpanConfigsThatApply implements the kvpb.InternalServer
// interface.
func (n *Node) GetAllSystemSpanConfigsThatApply(
	ctx context.Context, req *roachpb.GetAllSystemSpanConfigsThatApplyRequest,
) (*roachpb.GetAllSystemSpanConfigsThatApplyResponse, error) {
	spanConfigs, err := n.spanConfigAccessor.GetAllSystemSpanConfigsThatApply(ctx, req.TenantID)
	if err != nil {
		return nil, err
	}

	return &roachpb.GetAllSystemSpanConfigsThatApplyResponse{
		SpanConfigs: spanConfigs,
	}, nil
}

// UpdateSpanConfigs implements the kvpb.InternalServer interface.
func (n *Node) UpdateSpanConfigs(
	ctx context.Context, req *roachpb.UpdateSpanConfigsRequest,
) (*roachpb.UpdateSpanConfigsResponse, error) {
	toUpsert, err := spanconfig.EntriesToRecords(req.ToUpsert)
	if err != nil {
		return nil, err
	}
	toDelete, err := spanconfig.TargetsFromProtos(req.ToDelete)
	if err != nil {
		return nil, err
	}
	if err := n.spanConfigAccessor.UpdateSpanConfigRecords(
		ctx, toDelete, toUpsert, req.MinCommitTimestamp, req.MaxCommitTimestamp,
	); err != nil {
		return &roachpb.UpdateSpanConfigsResponse{
			Error: errors.EncodeError(ctx, err),
		}, nil
	}
	return &roachpb.UpdateSpanConfigsResponse{}, nil
}

// SpanConfigConformance implements the kvpb.InternalServer interface.
func (n *Node) SpanConfigConformance(
	ctx context.Context, req *roachpb.SpanConfigConformanceRequest,
) (*roachpb.SpanConfigConformanceResponse, error) {
	if n.storeCfg.SpanConfigSubscriber.LastUpdated().IsEmpty() {
		return nil, errors.Newf("haven't (yet) subscribed to span configs")
	}

	report, err := n.spanConfigReporter.SpanConfigConformance(ctx, req.Spans)
	if err != nil {
		return nil, err
	}
	return &roachpb.SpanConfigConformanceResponse{Report: report}, nil
}

// GetRangeDescriptors implements the kvpb.InternalServer interface.
func (n *Node) GetRangeDescriptors(
	args *kvpb.GetRangeDescriptorsRequest, stream kvpb.Internal_GetRangeDescriptorsServer,
) error {

	iter, err := n.execCfg.RangeDescIteratorFactory.NewLazyIterator(stream.Context(), args.Span, int(args.BatchSize))
	if err != nil {
		return err
	}

	var rangeDescriptors []roachpb.RangeDescriptor
	for iter.Valid() {
		rangeDescriptors = append(rangeDescriptors, iter.CurRangeDescriptor())
		if args.BatchSize > 0 && len(rangeDescriptors) >= int(args.BatchSize) {
			if err := stream.Send(&kvpb.GetRangeDescriptorsResponse{
				RangeDescriptors: rangeDescriptors,
			}); err != nil {
				return err
			}
			rangeDescriptors = make([]roachpb.RangeDescriptor, 0, len(rangeDescriptors))
		}
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return stream.Send(&kvpb.GetRangeDescriptorsResponse{
		RangeDescriptors: rangeDescriptors,
	})
}
