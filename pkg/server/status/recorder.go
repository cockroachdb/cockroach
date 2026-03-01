// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package status

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/ts/tsutil"
	"github.com/cockroachdb/cockroach/pkg/util/cgroups"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	// Import the logmetrics package to trigger its own init function, which inits and injects
	// metrics functionality into pkg/util/log.
	_ "github.com/cockroachdb/cockroach/pkg/util/log/logmetrics"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

const (
	// storeTimeSeriesPrefix is the common prefix for time series keys which
	// record store-specific data.
	storeTimeSeriesPrefix = "cr.store.%s"
	// nodeTimeSeriesPrefix is the common prefix for time series keys which
	// record node-specific data.
	nodeTimeSeriesPrefix = "cr.node.%s"
	// clusterTimeSeriesPrefix is the common prefix for time series keys which
	// record cluster-wide data.
	clusterTimeSeriesPrefix = "cr.cluster.%s"
	advertiseAddrLabelKey   = "advertise-addr"
	httpAddrLabelKey        = "http-addr"
	sqlAddrLabelKey         = "sql-addr"

	disableNodeAndTenantLabelsEnvVar = "COCKROACH_DISABLE_NODE_AND_TENANT_METRIC_LABELS"
)

// This option is provided as an escape hatch for customers who have
// custom scrape logic that adds relevant labels already.
var disableNodeAndTenantLabels = envutil.EnvOrDefaultBool(disableNodeAndTenantLabelsEnvVar, false)

// storeMetrics is the minimum interface of the storage.Store object needed by
// MetricsRecorder to provide status summaries. This is used instead of Store
// directly in order to simplify testing.
type storeMetrics interface {
	StoreID() roachpb.StoreID
	Descriptor(context.Context, bool) (*roachpb.StoreDescriptor, error)
	Registry() *metric.Registry
}

// ChildMetricsEnabled enables exporting of additional prometheus time series with extra labels
var ChildMetricsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel, "server.child_metrics.enabled",
	"enables the exporting of child metrics, additional prometheus time series with extra labels",
	false,
	settings.WithPublic)

// includeAggregateMetricsEnabled enables the exporting of the aggregate time series when child
// metrics are enabled.
var includeAggregateMetricsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel, "server.child_metrics.include_aggregate.enabled",
	"include the reporting of the aggregate time series when child metrics are enabled. This cluster setting "+
		"has no effect if child metrics are disabled.",
	true,
	settings.WithPublic)

// bugfix149481Enabled is a (temporary) cluster setting that fixes
// https://github.com/cockroachdb/cockroach/issues/149481. It is true (enabled) by default on master,
// and false on backports.
var bugfix149481Enabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel, "server.child_metrics.bugfix_missing_sql_metrics_149481.enabled",
	"fixes bug where certain SQL metrics are not being reported, see: "+
		"https://github.com/cockroachdb/cockroach/issues/149481",
	true,
	settings.WithVisibility(settings.Reserved))

// ChildMetricsStorageEnabled controls whether to record high-cardinality child metrics
// into the time series database. This is separate from ChildMetricsEnabled which controls
// Prometheus exports, allowing independent control of child metrics recording vs export.
// This setting enables debugging of changefeeds and should not be considered functionality
// to expand support for.
var ChildMetricsStorageEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel, "timeseries.persist_child_metrics.enabled",
	"enables the collection of high-cardinality child metrics into the time series database",
	false,
	settings.WithVisibility(settings.Reserved))

// MetricsRecorder is used to periodically record the information in a number of
// metric registries.
//
// Two types of registries are maintained: "node-level" registries, provided by
// node-level systems, and "store-level" registries which are provided by each
// store hosted by the node. There are slight differences in the way these are
// recorded, and they are thus kept separate.
type MetricsRecorder struct {
	*HealthChecker
	nodeLiveness *liveness.NodeLiveness
	remoteClocks *rpc.RemoteClockMonitor
	settings     *cluster.Settings
	clock        hlc.WallClock

	// Counts to help optimize slice allocation. Should only be accessed atomically.
	lastDataCount        int64
	lastSummaryCount     int64
	lastNodeMetricCount  int64
	lastStoreMetricCount int64

	// tenantID is the tenantID of the tenant this recorder is attached to.
	tenantID roachpb.TenantID

	// tenantNameContainer holds the tenant name of the tenant this recorder
	// is attached to. It will be used to label metrics that are tenant-specific.
	tenantNameContainer *roachpb.TenantNameContainer

	// prometheusExporter merges metrics into families and generates the
	// prometheus text format. It has a ScrapeAndPrintAsText method for thread safe
	// scrape and print so there is no need to have additional lock here.
	prometheusExporter metric.PrometheusExporter

	// mu synchronizes the reading of node/store registries against the adding of
	// nodes/stores. Consequently, almost all uses of it only need to take an
	// RLock on it.
	mu struct {
		syncutil.RWMutex
		sync.Once
		// nodeRegistry holds metrics that are specific to the storage and KV layer.
		// Do not use this for metrics that could possibly be reported by secondary
		// tenants, i.e. those also registered in server/tenant.go per tenant server.
		nodeRegistry *metric.Registry
		// appRegistry holds application-level metrics. These are the metrics
		// that are also registered in tenant.go anew for each tenant.
		appRegistry *metric.Registry
		// sysRegistry holds process-level metrics. These are metrics
		// that are collected once per process and are not specific to
		// any particular tenant.
		sysRegistry *metric.Registry
		// logRegistry contains the global metrics registry used by the logging
		// package. NB: The underlying metrics are global, but each server gets
		// its own separate registry to avoid things such as colliding labels.
		logRegistry *metric.Registry
		// clusterMetricsRegistry holds metrics that apply to cluster-wide
		// constructs (eg. backup schedules, jobs) rather than per-node
		// resources.
		clusterMetricsRegistry metric.RegistryReader
		desc                   roachpb.NodeDescriptor
		startedAt              int64

		// storeRegistries contains a registry for each store on the node. These
		// are not stored as subregistries, but rather are treated as wholly
		// independent.
		storeRegistries map[roachpb.StoreID]*metric.Registry
		stores          map[roachpb.StoreID]storeMetrics

		// tenantRegistries contains the registries for shared-process tenants.
		tenantRegistries map[roachpb.TenantID]*metric.TenantRegistries
	}

	// WriteNodeStatus is a potentially long-running method (with a network
	// round-trip) that requires a mutex to be safe for concurrent usage. We
	// therefore give it its own mutex to avoid blocking other methods.
	writeSummaryMu syncutil.Mutex

	// childMetricNameCache caches the encoded names for child metrics to avoid
	// rebuilding them on every recording. Uses syncutil.Map for lock-free reads.
	// Stores full cache entries to detect hash collisions.
	childMetricNameCache syncutil.Map[uint64, cacheEntry]
}

// NewMetricsRecorder initializes a new MetricsRecorder object that uses the
// given clock.
//
// If both nodeLiveness and remoteClocks are not-nil, the node status generated
// by GenerateNodeStatus() will contain info about RPC lantency to all currently
// live nodes.
func NewMetricsRecorder(
	tenantID roachpb.TenantID,
	tenantNameContainer *roachpb.TenantNameContainer,
	nodeLiveness *liveness.NodeLiveness,
	remoteClocks *rpc.RemoteClockMonitor,
	clock hlc.WallClock,
	settings *cluster.Settings,
) *MetricsRecorder {
	mr := &MetricsRecorder{
		HealthChecker:       NewHealthChecker(trackedMetrics),
		nodeLiveness:        nodeLiveness,
		remoteClocks:        remoteClocks,
		settings:            settings,
		clock:               clock,
		tenantID:            tenantID,
		tenantNameContainer: tenantNameContainer,
		prometheusExporter:  metric.MakePrometheusExporter(),
	}
	mr.mu.storeRegistries = make(map[roachpb.StoreID]*metric.Registry)
	mr.mu.stores = make(map[roachpb.StoreID]storeMetrics)
	mr.mu.tenantRegistries = make(map[roachpb.TenantID]*metric.TenantRegistries)
	return mr
}

// AddTenantRegistry adds shared-process tenant's registry.
func (mr *MetricsRecorder) AddTenantRegistry(
	tenantID roachpb.TenantID, reg *metric.TenantRegistries,
) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if !disableNodeAndTenantLabels {
		// If there are no in-process tenants running, we don't set the
		// tenant label on the system tenant metrics until a secondary
		// tenant is initialized.
		mr.mu.Do(func() {
			mr.mu.nodeRegistry.AddLabel("tenant", catconstants.SystemTenantName)
			mr.mu.appRegistry.AddLabel("tenant", catconstants.SystemTenantName)
			mr.mu.logRegistry.AddLabel("tenant", catconstants.SystemTenantName)
			mr.mu.sysRegistry.AddLabel("tenant", catconstants.SystemTenantName)
			mr.mu.clusterMetricsRegistry.AddLabel("tenant", catconstants.SystemTenantName)
		})
	}
	mr.mu.tenantRegistries[tenantID] = reg
}

// RemoveTenantRegistry removes shared-process tenant's registry.
func (mr *MetricsRecorder) RemoveTenantRegistry(tenantID roachpb.TenantID) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	delete(mr.mu.tenantRegistries, tenantID)
}

// AppRegistry returns the metric registry for application-level metrics.
func (mr *MetricsRecorder) AppRegistry() *metric.Registry {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	return mr.mu.appRegistry
}

// NodeRegistry returns the metric registry for node-level metrics.
func (mr *MetricsRecorder) NodeRegistry() *metric.Registry {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	return mr.mu.logRegistry
}

// StoreRegistry returns the metric registry for store-level metrics
// corresponding to the provided store ID.
func (mr *MetricsRecorder) StoreRegistry(id roachpb.StoreID) *metric.Registry {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	return mr.mu.storeRegistries[id]
}

// ClusterMetricRegistry returns the metric registry for cluster-level metrics
// for the specified tenant ID.
func (mr *MetricsRecorder) ClusterMetricRegistry(id roachpb.TenantID) metric.RegistryReader {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if id == mr.tenantID {
		return mr.mu.clusterMetricsRegistry
	}
	if registries, ok := mr.mu.tenantRegistries[id]; ok {
		return registries.ClusterMetricsRegistry()
	}
	return nil
}

// AddNode adds various metric registries an initialized server, along
// with its descriptor and start time.
// The registries are:
//
// - node registry - storage/KV metrics relating to a KV node.
// - app registry - application-level metrics relating to a SQL/HTTP service.
// - log registry - metrics from the logging system.
// - sys registry - metrics about system-wide properties.
func (mr *MetricsRecorder) AddNode(
	nodeReg, appReg, logReg, sysReg *metric.Registry,
	clusterMetricReg metric.RegistryReader,
	desc roachpb.NodeDescriptor,
	startedAt int64,
	advertiseAddr, httpAddr, sqlAddr string,
) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.mu.nodeRegistry = nodeReg
	mr.mu.appRegistry = appReg
	mr.mu.logRegistry = logReg
	mr.mu.sysRegistry = sysReg
	mr.mu.clusterMetricsRegistry = clusterMetricReg
	mr.mu.desc = desc
	mr.mu.startedAt = startedAt

	// Create node ID gauge metric with host as a label.
	metadata := metric.Metadata{
		Name:        "node-id",
		Help:        "node ID with labels for advertised RPC and HTTP addresses",
		Measurement: "Node ID",
		Unit:        metric.Unit_CONST,
	}

	metadata.AddLabel(advertiseAddrLabelKey, advertiseAddr)
	metadata.AddLabel(httpAddrLabelKey, httpAddr)
	metadata.AddLabel(sqlAddrLabelKey, sqlAddr)
	nodeIDGauge := metric.NewGauge(metadata)
	nodeIDGauge.Update(int64(desc.NodeID))
	nodeReg.AddMetric(nodeIDGauge)

	if !disableNodeAndTenantLabels {
		nodeIDInt := int(desc.NodeID)
		if nodeIDInt != 0 {
			nodeReg.AddLabel("node_id", strconv.Itoa(int(desc.NodeID)))
			sysReg.AddLabel("node_id", strconv.Itoa(int(desc.NodeID)))
			appReg.AddLabel("node_id", strconv.Itoa(int(desc.NodeID)))
			logReg.AddLabel("node_id", strconv.Itoa(int(desc.NodeID)))
			clusterMetricReg.AddLabel("node_id", strconv.Itoa(int(desc.NodeID)))
			for _, s := range mr.mu.storeRegistries {
				s.AddLabel("node_id", strconv.Itoa(int(desc.NodeID)))
			}
		}
		if mr.tenantNameContainer != nil && mr.tenantNameContainer.String() != catconstants.SystemTenantName {
			nodeReg.AddLabel("tenant", mr.tenantNameContainer)
			sysReg.AddLabel("tenant", mr.tenantNameContainer)
			appReg.AddLabel("tenant", mr.tenantNameContainer)
			logReg.AddLabel("tenant", mr.tenantNameContainer)
			clusterMetricReg.AddLabel("tenant", mr.tenantNameContainer)
		}
	}
}

// AddStore adds the Registry from the provided store as a store-level registry
// in this recorder. A reference to the store is kept for the purpose of
// gathering some additional information which is present in store status
// summaries.
// Stores should only be added to the registry after they have been started.
func (mr *MetricsRecorder) AddStore(store storeMetrics) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	storeID := store.StoreID()
	store.Registry().AddLabel("store", strconv.Itoa(int(storeID)))
	// If AddNode has already been called, we need to add the node_id label here.
	// This can happen when stores are initialized asynchronously after node start.
	// There's no risk of duplicate labels: either the store is added before AddNode
	// runs (in which case desc.NodeID is 0 here, and AddNode adds the label), or it's
	// added after (in which case we add it here, and AddNode never saw this store).
	if !disableNodeAndTenantLabels && mr.mu.desc.NodeID != 0 {
		store.Registry().AddLabel("node_id", strconv.Itoa(int(mr.mu.desc.NodeID)))
	}
	mr.mu.storeRegistries[storeID] = store.Registry()
	mr.mu.stores[storeID] = store
}

// MarshalJSON returns an appropriate JSON representation of the current values
// of the metrics being tracked by this recorder.
func (mr *MetricsRecorder) MarshalJSON() ([]byte, error) {
	mr.mu.RLock()
	defer mr.mu.RUnlock()
	if mr.mu.nodeRegistry == nil {
		// We haven't yet processed initialization information; return an empty
		// JSON object.
		if log.V(1) {
			log.Dev.Warning(context.TODO(), "MetricsRecorder.MarshalJSON() called before NodeID allocation")
		}
		return []byte("{}"), nil
	}
	topLevel := map[string]interface{}{
		fmt.Sprintf("node.%d", mr.mu.desc.NodeID):     mr.mu.nodeRegistry,
		fmt.Sprintf("node.%d.log", mr.mu.desc.NodeID): mr.mu.logRegistry,
	}
	// Add collection of stores to top level. JSON requires that keys be strings,
	// so we must convert the store ID to a string.
	storeLevel := make(map[string]interface{})
	for id, reg := range mr.mu.storeRegistries {
		storeLevel[strconv.Itoa(int(id))] = reg
	}
	topLevel["stores"] = storeLevel
	return json.Marshal(topLevel)
}

// ScrapeIntoPrometheus updates the passed-in prometheusExporter's metrics
// snapshot.
func (mr *MetricsRecorder) ScrapeIntoPrometheus(pm *metric.PrometheusExporter) {
	mr.ScrapeIntoPrometheusWithStaticLabels(false)(pm)
}

func (mr *MetricsRecorder) ScrapeIntoPrometheusWithStaticLabels(
	useStaticLabels bool,
) func(pm *metric.PrometheusExporter) {
	return func(pm *metric.PrometheusExporter) {
		mr.mu.RLock()
		defer mr.mu.RUnlock()

		includeChildMetrics := ChildMetricsEnabled.Get(&mr.settings.SV)
		includeAggregateMetrics := includeAggregateMetricsEnabled.Get(&mr.settings.SV)
		reinitialisableBugFixEnabled := bugfix149481Enabled.Get(&mr.settings.SV)
		scrapeOptions := []metric.ScrapeOption{
			metric.WithIncludeChildMetrics(includeChildMetrics),
			metric.WithIncludeAggregateMetrics(includeAggregateMetrics),
			metric.WithUseStaticLabels(useStaticLabels),
			metric.WithReinitialisableBugFixEnabled(reinitialisableBugFixEnabled),
		}
		if mr.mu.nodeRegistry == nil {
			// We haven't yet processed initialization information; output nothing.
			if log.V(1) {
				log.Dev.Warning(context.TODO(), "MetricsRecorder asked to scrape metrics before NodeID allocation")
			}
		}
		pm.ScrapeRegistry(mr.mu.nodeRegistry, scrapeOptions...)
		pm.ScrapeRegistry(mr.mu.appRegistry, scrapeOptions...)
		pm.ScrapeRegistry(mr.mu.logRegistry, scrapeOptions...)
		pm.ScrapeRegistry(mr.mu.sysRegistry, scrapeOptions...)
		pm.ScrapeRegistry(mr.mu.clusterMetricsRegistry, scrapeOptions...)
		for _, reg := range mr.mu.storeRegistries {
			pm.ScrapeRegistry(reg, scrapeOptions...)
		}
		for _, tenantRegistry := range mr.mu.tenantRegistries {
			pm.ScrapeRegistry(tenantRegistry.AppRegistry(), scrapeOptions...)
			pm.ScrapeRegistry(tenantRegistry.ClusterMetricsRegistry(), scrapeOptions...)
		}
	}
}

// PrintAsText writes the current metrics values as plain-text to the writer.
// We write metrics to a temporary buffer which is then copied to the writer.
// This is to avoid hanging requests from holding the lock.
func (mr *MetricsRecorder) PrintAsText(
	w io.Writer, contentType expfmt.Format, useStaticLabels bool,
) error {
	var buf bytes.Buffer
	if err := mr.prometheusExporter.ScrapeAndPrintAsText(&buf, contentType, mr.ScrapeIntoPrometheusWithStaticLabels(useStaticLabels)); err != nil {
		return err
	}
	_, err := buf.WriteTo(w)
	return err
}

// ExportToGraphite sends the current metric values to a Graphite server.
// It creates a new PrometheusExporter each time to avoid needing to worry
// about races with mr.promMu.prometheusExporter. We are not as worried
// about the extra memory allocations.
func (mr *MetricsRecorder) ExportToGraphite(
	ctx context.Context, endpoint string, pm *metric.PrometheusExporter,
) error {
	mr.ScrapeIntoPrometheus(pm)
	graphiteExporter := metric.MakeGraphiteExporter(pm)
	return graphiteExporter.Push(ctx, endpoint)
}

// GetTimeSeriesData serializes registered metrics for consumption by
// CockroachDB's time series system. GetTimeSeriesData implements the DataSource
// interface of the ts package.
func (mr *MetricsRecorder) GetTimeSeriesData(childMetrics bool) []tspb.TimeSeriesData {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	if mr.mu.nodeRegistry == nil {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Dev.Warning(context.TODO(), "MetricsRecorder.GetTimeSeriesData() called before NodeID allocation")
		}
		return nil
	}

	now := mr.clock.Now()
	lastDataCount := atomic.LoadInt64(&mr.lastDataCount)
	data := make([]tspb.TimeSeriesData, 0, lastDataCount)

	if childMetrics {
		if !ChildMetricsStorageEnabled.Get(&mr.settings.SV) {
			return nil
		}

		// Record child metrics from app registry for system tenant only.
		recorder := registryRecorder{
			registry:             mr.mu.appRegistry,
			format:               nodeTimeSeriesPrefix,
			source:               mr.mu.desc.NodeID.String(),
			timestampNanos:       now.UnixNano(),
			childMetricNameCache: &mr.childMetricNameCache,
		}
		recorder.recordChangefeedChildMetrics(&data)

		// Record child metrics from app-level registries for secondary tenants
		for tenantID, r := range mr.mu.tenantRegistries {
			tenantRecorder := registryRecorder{
				registry:             r.AppRegistry(),
				format:               nodeTimeSeriesPrefix,
				source:               tsutil.MakeTenantSource(mr.mu.desc.NodeID.String(), tenantID.String()),
				timestampNanos:       now.UnixNano(),
				childMetricNameCache: &mr.childMetricNameCache,
			}
			tenantRecorder.recordChangefeedChildMetrics(&data)
		}

		atomic.CompareAndSwapInt64(&mr.lastDataCount, lastDataCount, int64(len(data)))
		return data
	}

	// Record time series from node-level registries.
	recorder := registryRecorder{
		registry:       mr.mu.nodeRegistry,
		format:         nodeTimeSeriesPrefix,
		source:         mr.mu.desc.NodeID.String(),
		timestampNanos: now.UnixNano(),
	}
	recorder.record(&data)
	// Now record the app metrics for the system tenant.
	recorder.registry = mr.mu.appRegistry
	recorder.record(&data)
	// Now record the log metrics.
	recorder.registry = mr.mu.logRegistry
	recorder.record(&data)
	// Now record the system metrics.
	recorder.registry = mr.mu.sysRegistry
	recorder.record(&data)

	cmRecorder := registryRecorder{
		registry:       mr.mu.clusterMetricsRegistry,
		format:         clusterTimeSeriesPrefix,
		source:         mr.mu.desc.NodeID.String(),
		timestampNanos: now.UnixNano(),
	}
	cmRecorder.record(&data)

	// Record time series from app-level registries for secondary tenants.
	for tenantID, r := range mr.mu.tenantRegistries {
		tenantAppRecorder := registryRecorder{
			registry:       r.AppRegistry(),
			format:         nodeTimeSeriesPrefix,
			source:         tsutil.MakeTenantSource(mr.mu.desc.NodeID.String(), tenantID.String()),
			timestampNanos: now.UnixNano(),
		}
		tenantAppRecorder.record(&data)

		tenantClusterMetricRecorder := registryRecorder{
			registry:       r.ClusterMetricsRegistry(),
			format:         clusterTimeSeriesPrefix,
			source:         tsutil.MakeTenantSource(mr.mu.desc.NodeID.String(), tenantID.String()),
			timestampNanos: now.UnixNano(),
		}
		tenantClusterMetricRecorder.record(&data)
	}

	// Record time series from store-level registries.
	tIDLabel := multitenant.TenantIDLabel
	for storeID, r := range mr.mu.storeRegistries {
		storeRecorder := registryRecorder{
			registry:       r,
			format:         storeTimeSeriesPrefix,
			source:         storeID.String(),
			timestampNanos: now.UnixNano(),
		}
		storeRecorder.record(&data)

		// Now record secondary tenant store metrics, if any exist in the process.
		for tenantID := range mr.mu.tenantRegistries {
			tenantStoreRecorder := registryRecorder{
				registry:       r,
				format:         storeTimeSeriesPrefix,
				source:         tsutil.MakeTenantSource(storeID.String(), tenantID.String()),
				timestampNanos: now.UnixNano(),
			}
			tenantID := tenantID.String()
			tenantStoreRecorder.recordChild(&data, kvbase.TenantsStorageMetricsSet, &prometheusgo.LabelPair{
				Name:  &tIDLabel,
				Value: &tenantID,
			})
		}
	}
	atomic.CompareAndSwapInt64(&mr.lastDataCount, lastDataCount, int64(len(data)))
	return data
}

// GetMetricsMetadata returns the metadata from all metrics tracked in the node's
// nodeRegistry and a randomly selected storeRegistry.
//
// If the argument is true, both node-level and app-level metrics are
// combined in the first return value.
//
// The third return value combines all process-wide metrics (log and
// system metrics).
func (mr *MetricsRecorder) GetMetricsMetadata(
	combined bool,
) (nodeMetrics, appMetrics, srvMetrics map[string]metric.Metadata) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if mr.mu.nodeRegistry == nil {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Dev.Warning(context.TODO(), "MetricsRecorder.GetMetricsMetadata() called before NodeID allocation")
		}
		return nil, nil, nil
	}

	nodeMetrics = make(map[string]metric.Metadata)
	if combined {
		appMetrics = nodeMetrics
		srvMetrics = nodeMetrics
	} else {
		appMetrics = make(map[string]metric.Metadata)
		srvMetrics = make(map[string]metric.Metadata)
	}

	mr.mu.nodeRegistry.WriteMetricsMetadata(nodeMetrics)
	mr.mu.appRegistry.WriteMetricsMetadata(appMetrics)
	mr.mu.logRegistry.WriteMetricsMetadata(srvMetrics)
	mr.mu.sysRegistry.WriteMetricsMetadata(srvMetrics)
	mr.mu.clusterMetricsRegistry.WriteMetricsMetadata(srvMetrics)

	mr.writeStoreMetricsMetadata(nodeMetrics)
	return nodeMetrics, appMetrics, srvMetrics
}

// GetRecordedMetricNames takes a map of metric metadata and returns a map
// of the metadata name to the name the metric is recorded with in tsdb.
func (mr *MetricsRecorder) GetRecordedMetricNames(
	allMetadata map[string]metric.Metadata,
) map[string]string {
	storeMetricsMap := make(map[string]metric.Metadata)
	tsDbMetricNames := make(map[string]string, len(allMetadata))
	mr.writeStoreMetricsMetadata(storeMetricsMap)
	for metricName, metadata := range allMetadata {
		prefix := nodeTimeSeriesPrefix
		if _, ok := storeMetricsMap[metricName]; ok {
			prefix = storeTimeSeriesPrefix
		}
		if metadata.MetricType == prometheusgo.MetricType_HISTOGRAM {
			for _, metricComputer := range metric.HistogramMetricComputers {
				computedMetricName := metricName + metricComputer.Suffix
				tsDbMetricNames[computedMetricName] = fmt.Sprintf(prefix, computedMetricName)
			}
		} else {
			tsDbMetricNames[metricName] = fmt.Sprintf(prefix, metricName)
		}

	}
	return tsDbMetricNames
}

// writeStoreMetricsMetadata Gets a store from mr.mu.storeRegistries and writes
// the metrics metadata to the provided map.
func (mr *MetricsRecorder) writeStoreMetricsMetadata(metricsMetadata map[string]metric.Metadata) {
	if len(mr.mu.storeRegistries) == 0 {
		return
	}

	// All store registries should have the same metadata, so only the metadata
	// from the first store is used to write to metricsMetadata.
	for _, registry := range mr.mu.storeRegistries {
		registry.WriteMetricsMetadata(metricsMetadata)
		return
	}
}

// getNetworkActivity produces a map of network activity from this node to all
// other nodes. Latencies are stored as nanos.
func (mr *MetricsRecorder) getNetworkActivity(
	ctx context.Context,
) map[roachpb.NodeID]statuspb.NodeStatus_NetworkActivity {
	activity := make(map[roachpb.NodeID]statuspb.NodeStatus_NetworkActivity)
	if mr.nodeLiveness != nil {
		var currentAverages map[roachpb.NodeID]time.Duration
		if mr.remoteClocks != nil {
			currentAverages = mr.remoteClocks.AllLatencies()
		}
		for nodeID, entry := range mr.nodeLiveness.ScanNodeVitalityFromCache() {
			na := statuspb.NodeStatus_NetworkActivity{}
			if entry.IsLive(livenesspb.NetworkMap) {
				if latency, ok := currentAverages[nodeID]; ok {
					na.Latency = latency.Nanoseconds()
				}
			}
			activity[nodeID] = na
		}
	}
	return activity
}

// GenerateNodeStatus returns a status summary message for the node. The summary
// includes the recent values of metrics for both the node and all of its
// component stores. When the node isn't initialized yet, nil is returned.
func (mr *MetricsRecorder) GenerateNodeStatus(ctx context.Context) *statuspb.NodeStatus {
	activity := mr.getNetworkActivity(ctx)

	mr.mu.RLock()
	defer mr.mu.RUnlock()

	if mr.mu.nodeRegistry == nil {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Dev.Warning(ctx, "attempt to generate status summary before NodeID allocation.")
		}
		return nil
	}

	now := mr.clock.Now()

	lastSummaryCount := atomic.LoadInt64(&mr.lastSummaryCount)
	lastNodeMetricCount := atomic.LoadInt64(&mr.lastNodeMetricCount)
	lastStoreMetricCount := atomic.LoadInt64(&mr.lastStoreMetricCount)

	systemMemory, _, err := GetTotalMemoryWithoutLogging()
	if err != nil {
		log.Dev.Errorf(ctx, "could not get total system memory: %v", err)
	}

	// Generate a node status with no store data.
	nodeStat := &statuspb.NodeStatus{
		Desc:              mr.mu.desc,
		BuildInfo:         build.GetInfo(),
		UpdatedAt:         now.UnixNano(),
		StartedAt:         mr.mu.startedAt,
		StoreStatuses:     make([]statuspb.StoreStatus, 0, lastSummaryCount),
		Metrics:           make(map[string]float64, lastNodeMetricCount),
		Args:              os.Args,
		Env:               flattenStrings(envutil.GetEnvVarsUsed()),
		Activity:          activity,
		NumCpus:           int32(system.NumCPU()),
		NumVcpus:          GetVCPUs(ctx),
		TotalSystemMemory: systemMemory,
	}

	eachRecordableValue(mr.mu.nodeRegistry, func(name string, val float64) {
		nodeStat.Metrics[name] = val
	})
	eachRecordableValue(mr.mu.appRegistry, func(name string, val float64) {
		nodeStat.Metrics[name] = val
	})
	eachRecordableValue(mr.mu.logRegistry, func(name string, val float64) {
		nodeStat.Metrics[name] = val
	})
	eachRecordableValue(mr.mu.sysRegistry, func(name string, val float64) {
		nodeStat.Metrics[name] = val
	})

	// Generate status summaries for stores.
	for storeID, r := range mr.mu.storeRegistries {
		storeMetrics := make(map[string]float64, lastStoreMetricCount)
		eachRecordableValue(r, func(name string, val float64) {
			storeMetrics[name] = val
		})

		// Gather descriptor from store.
		descriptor, err := mr.mu.stores[storeID].Descriptor(ctx, false /* useCached */)
		if err != nil {
			log.Dev.Errorf(ctx, "could not record status summaries: Store %d could not return descriptor, error: %s", storeID, err)
			continue
		}

		nodeStat.StoreStatuses = append(nodeStat.StoreStatuses, statuspb.StoreStatus{
			Desc:    *descriptor,
			Metrics: storeMetrics,
		})
	}

	atomic.CompareAndSwapInt64(
		&mr.lastSummaryCount, lastSummaryCount, int64(len(nodeStat.StoreStatuses)))
	atomic.CompareAndSwapInt64(
		&mr.lastNodeMetricCount, lastNodeMetricCount, int64(len(nodeStat.Metrics)))
	if len(nodeStat.StoreStatuses) > 0 {
		atomic.CompareAndSwapInt64(
			&mr.lastStoreMetricCount, lastStoreMetricCount, int64(len(nodeStat.StoreStatuses[0].Metrics)))
	}

	return nodeStat
}

func flattenStrings(s []redact.RedactableString) []string {
	res := make([]string, len(s))
	for i, v := range s {
		res[i] = v.StripMarkers()
	}
	return res
}

// WriteNodeStatus writes the supplied summary to the given client. If mustExist
// is true, the key must already exist and must not change while being updated,
// otherwise an error is returned -- if false, the status is always written.
func (mr *MetricsRecorder) WriteNodeStatus(
	ctx context.Context, db *kv.DB, nodeStatus statuspb.NodeStatus, mustExist bool,
) error {
	mr.writeSummaryMu.Lock()
	defer mr.writeSummaryMu.Unlock()
	key := keys.NodeStatusKey(nodeStatus.Desc.NodeID)
	// We use an inline value to store only a single version of the node status.
	// There's not much point in keeping the historical versions as we keep
	// all of the constituent data as timeseries. Further, due to the size
	// of the build info in the node status, writing one of these every 10s
	// will generate more versions than will easily fit into a range over
	// the course of a day.
	if mustExist {
		entry, err := db.Get(ctx, key)
		if err != nil {
			return err
		}
		if entry.Value == nil {
			return errors.New("status entry not found, node may have been decommissioned")
		}
		err = db.CPutInline(ctx, key, &nodeStatus, entry.Value.TagAndDataBytes())
		if detail := (*kvpb.ConditionFailedError)(nil); errors.As(err, &detail) {
			if detail.ActualValue == nil {
				return errors.New("status entry not found, node may have been decommissioned")
			}
			return errors.New("status entry unexpectedly changed during update")
		} else if err != nil {
			return err
		}
	} else {
		if err := db.PutInline(ctx, key, &nodeStatus); err != nil {
			return err
		}
	}
	if log.V(2) {
		statusJSON, err := json.Marshal(&nodeStatus)
		if err != nil {
			log.Dev.Errorf(ctx, "error marshaling nodeStatus to json: %s", err)
		}
		log.Dev.Infof(ctx, "node %d status: %s", nodeStatus.Desc.NodeID, statusJSON)
	}
	return nil
}

// registryRecorder is a helper class for recording time series datapoints
// from a metrics Registry.
type registryRecorder struct {
	registry       metric.RegistryReader
	format         string
	source         string
	timestampNanos int64
	// childMetricNameCache is an optional cache for encoded child metric names.
	// If nil, no caching will be performed.
	childMetricNameCache *syncutil.Map[uint64, cacheEntry]
}

// extractValue extracts the metric value(s) for the given metric and passes it, along with the metric name, to the
// provided callback function.
func extractValue(name string, mtr interface{}, fn func(string, float64)) error {
	switch mtr := mtr.(type) {
	case metric.WindowedHistogram:
		// Use cumulative stats here. Count and Sum must be calculated against the cumulative histogram.
		cumulative, ok := mtr.(metric.CumulativeHistogram)
		if !ok {
			return errors.Newf(`extractValue called on histogram metric %q that does not implement the
				CumulativeHistogram interface. All histogram metrics are expected to implement this interface`, name)
		}
		cumulativeSnapshot := cumulative.CumulativeSnapshot()
		// Use windowed stats for avg and quantiles
		windowedSnapshot := mtr.WindowedSnapshot()
		for _, c := range metric.HistogramMetricComputers {
			if c.IsSummaryMetric {
				fn(name+c.Suffix, c.ComputedMetric(windowedSnapshot))
			} else {
				fn(name+c.Suffix, c.ComputedMetric(cumulativeSnapshot))
			}
		}
	case metric.PrometheusExportable:
		// NB: this branch is intentionally at the bottom since all metrics implement it.
		m := mtr.ToPrometheusMetric()
		if m.Gauge != nil {
			v := *m.Gauge.Value
			if math.IsInf(v, 0) || math.IsNaN(v) {
				v = 0
			}
			fn(name, v)
		} else if m.Counter != nil {
			fn(name, *m.Counter.Value)
		}
	case metric.PrometheusVector:
		// NOOP - We don't record metric.PrometheusVector into TSDB. These metrics
		// are only exported as prometheus metrics via metric.PrometheusExporter.
		return nil

	default:
		return errors.Errorf("cannot extract value for type %T", mtr)
	}
	return nil
}

// eachRecordableValue visits each metric in the registry, calling the supplied
// function once for each recordable value represented by that metric. This is
// useful to expand certain metric types (such as histograms) into multiple
// recordable values.
func eachRecordableValue(reg metric.RegistryReader, fn func(string, float64)) {
	reg.Each(func(name string, mtr interface{}) {
		if err := extractValue(name, mtr, fn); err != nil {
			log.Dev.Warningf(context.TODO(), "%v", err)
			return
		}
	})
}

func (rr registryRecorder) record(dest *[]tspb.TimeSeriesData) {
	eachRecordableValue(rr.registry, func(name string, val float64) {
		*dest = append(*dest, tspb.TimeSeriesData{
			Name:   fmt.Sprintf(rr.format, name),
			Source: rr.source,
			Datapoints: []tspb.TimeSeriesDatapoint{
				{
					TimestampNanos: rr.timestampNanos,
					Value:          val,
				},
			},
		})
	})
}

// recordChild filters the metrics in the registry down to those provided in
// the metricsFilter argument, and iterates through any child metrics that
// may exist on said metric. Child metrics whose label sets contains a match
// against the childLabelFilter are recorded into the provided dest slice of
// type tspb.TimeSeriesData.
//
// NB: Only available for Counter and Gauge metrics.
func (rr registryRecorder) recordChild(
	dest *[]tspb.TimeSeriesData,
	metricsFilter map[string]struct{},
	childLabelFilter *prometheusgo.LabelPair,
) {
	labels := rr.registry.GetLabels()
	rr.registry.Select(metricsFilter, func(name string, v interface{}) {
		prom, ok := v.(metric.PrometheusExportable)
		if !ok {
			return
		}
		promIter, ok := v.(metric.PrometheusIterable)
		if !ok {
			return
		}
		m := prom.ToPrometheusMetric()
		m.Label = append(labels, prom.GetLabels(false /* useStaticLabels */)...)

		processChildMetric := func(metric *prometheusgo.Metric) {
			found := false
			for _, label := range metric.Label {
				if label.GetName() == childLabelFilter.GetName() &&
					label.GetValue() == childLabelFilter.GetValue() {
					found = true
					break
				}
			}
			if !found {
				return
			}
			var value float64
			if metric.Gauge != nil {
				value = *metric.Gauge.Value
			} else if metric.Counter != nil {
				value = *metric.Counter.Value
			} else {
				return
			}
			*dest = append(*dest, tspb.TimeSeriesData{
				Name:   fmt.Sprintf(rr.format, prom.GetName(false /* useStaticLabels */)),
				Source: rr.source,
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: rr.timestampNanos,
						Value:          value,
					},
				},
			})
		}
		promIter.Each(m.Label, processChildMetric)
	})
}

func hashLabels(labels []*prometheusgo.LabelPair) uint64 {
	h := fnv.New64a()
	for _, label := range labels {
		h.Write([]byte(label.GetName()))
		h.Write([]byte(label.GetValue()))
	}
	return h.Sum64()
}

// cacheEntry holds a cached metric name along with the labels that produced it,
// used for verification when hash collisions occur.
type cacheEntry struct {
	labels []*prometheusgo.LabelPair
	name   string
}

// labelsEqual returns true if two label slices are equal.
func labelsEqual(a, b []*prometheusgo.LabelPair) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].GetName() != b[i].GetName() || a[i].GetValue() != b[i].GetValue() {
			return false
		}
	}
	return true
}

// getOrComputeMetricName looks up the encoded metric name in the cache,
// or computes it using the provided computeFn if not found.
// Verifies labels on cache hit to detect hash collisions.
func getOrComputeMetricName(
	cache *syncutil.Map[uint64, cacheEntry],
	labels []*prometheusgo.LabelPair,
	computeFn func() string,
) string {
	if cache == nil {
		return computeFn()
	}
	labelHash := hashLabels(labels)
	if cached, ok := cache.Load(labelHash); ok {
		if labelsEqual(cached.labels, labels) {
			return cached.name
		}
		// Hash collision detected - proceed to compute
	}
	name := computeFn()
	cache.Store(labelHash, &cacheEntry{
		labels: labels,
		name:   name,
	})
	return name
}

// recordChangefeedChildMetrics iterates through changefeed metrics in the registry and processes child metrics
// for those that have TsdbRecordLabeled set to true in their metadata.
// Records up to 1024 child metrics per metric to prevent unbounded memory usage and performance issues.
func (rr registryRecorder) recordChangefeedChildMetrics(dest *[]tspb.TimeSeriesData) {
	maxChildMetricsPerMetric := 1024

	labels := rr.registry.GetLabels()
	rr.registry.Each(func(name string, v interface{}) {
		if _, allowed := tsutil.AllowedChildMetrics[name]; !allowed {
			return
		}
		// Check if the metric has child collection enabled in its metadata
		iterable, isIterable := v.(metric.Iterable)
		if !isIterable {
			return
		}
		metadata := iterable.GetMetadata()
		if !metadata.GetTsdbRecordLabeled() {
			return // Skip this metric if child collection is not enabled
		}

		// Handle AggHistogram - use direct child access for per-child snapshots
		if aggHist, isAggHist := v.(*aggmetric.AggHistogram); isAggHist {
			var childMetricsCount int
			aggHist.EachChild(func(labelNames, labelVals []string, child *aggmetric.Histogram) {
				if childMetricsCount >= maxChildMetricsPerMetric {
					return
				}

				// Create label pairs for this child
				childLabels := make([]*prometheusgo.LabelPair, len(labels), len(labels)+len(labelVals))
				copy(childLabels, labels)

				for i, val := range labelVals {
					if i < len(labelNames) {
						name := labelNames[i]
						value := val
						childLabels = append(childLabels, &prometheusgo.LabelPair{
							Name:  &name,
							Value: &value,
						})
					}
				}

				// Check cache for encoded name
				baseName := getOrComputeMetricName(rr.childMetricNameCache, childLabels, func() string {
					return metadata.Name + metric.EncodeLabeledName(&prometheusgo.Metric{Label: childLabels})
				})
				// Record all histogram computed metrics using child-specific snapshots
				for _, c := range metric.HistogramMetricComputers {
					var snapshot metric.HistogramSnapshot
					if c.IsSummaryMetric {
						snapshot = child.WindowedSnapshot()
					} else {
						snapshot = child.CumulativeSnapshot()
					}
					count, _ := snapshot.Total()
					if count < 0 {
						continue // Skip malformed snapshots
					}
					value := c.ComputedMetric(snapshot)
					metricName := baseName + c.Suffix
					*dest = append(*dest, tspb.TimeSeriesData{
						Name:   fmt.Sprintf(rr.format, metricName),
						Source: rr.source,
						Datapoints: []tspb.TimeSeriesDatapoint{
							{
								TimestampNanos: rr.timestampNanos,
								Value:          value,
							},
						},
					})
				}
				childMetricsCount++
			})
			return
		}

		// Handle Counter and Gauge metrics via Prometheus export
		prom, ok := v.(metric.PrometheusExportable)
		if !ok {
			return
		}
		promIter, ok := v.(metric.PrometheusIterable)
		if !ok {
			return
		}
		m := prom.ToPrometheusMetric()
		m.Label = append(labels, prom.GetLabels(false /* useStaticLabels */)...)

		var childMetricsCount int
		processChildMetric := func(childMetric *prometheusgo.Metric) {
			if childMetricsCount >= maxChildMetricsPerMetric {
				return
			}

			var value float64
			if childMetric.Gauge != nil {
				value = *childMetric.Gauge.Value
			} else if childMetric.Counter != nil {
				value = *childMetric.Counter.Value
			} else {
				return
			}

			// Check cache for encoded name
			metricName := getOrComputeMetricName(rr.childMetricNameCache, childMetric.Label, func() string {
				return prom.GetName(false /* useStaticLabels */) + metric.EncodeLabeledName(childMetric)
			})

			*dest = append(*dest, tspb.TimeSeriesData{
				Name:   fmt.Sprintf(rr.format, metricName),
				Source: rr.source,
				Datapoints: []tspb.TimeSeriesDatapoint{
					{
						TimestampNanos: rr.timestampNanos,
						Value:          value,
					},
				},
			})
			childMetricsCount++
		}
		promIter.Each(m.Label, processChildMetric)
	})
}

// GetTotalMemory returns either the total system memory (in bytes) or if
// possible the cgroups available memory.
func GetTotalMemory(ctx context.Context) (int64, error) {
	memory, warning, err := GetTotalMemoryWithoutLogging()
	if err != nil {
		return 0, err
	}
	if warning != "" {
		log.Dev.Infof(ctx, "%s", warning)
	}
	return memory, nil
}

// GetTotalMemoryWithoutLogging is the same as GetTotalMemory, but returns any warning
// as a string instead of logging it.
func GetTotalMemoryWithoutLogging() (int64, string, error) {
	totalMem, err := func() (int64, error) {
		mem := gosigar.Mem{}
		if err := mem.Get(); err != nil {
			return 0, err
		}
		if mem.Total > math.MaxInt64 {
			return 0, fmt.Errorf("inferred memory size %s exceeds maximum supported memory size %s",
				humanize.IBytes(mem.Total), humanize.Bytes(math.MaxInt64))
		}
		return int64(mem.Total), nil
	}()
	if err != nil {
		return 0, "", err
	}
	checkTotal := func(x int64, warning string) (int64, string, error) {
		if x <= 0 {
			// https://github.com/elastic/gosigar/issues/72
			return 0, warning, fmt.Errorf("inferred memory size %d is suspicious, considering invalid", x)
		}
		return x, warning, nil
	}
	if runtime.GOOS != "linux" {
		return checkTotal(totalMem, "")
	}
	cgAvlMem, warning, err := cgroups.GetMemoryLimit()
	if err != nil {
		return checkTotal(totalMem,
			fmt.Sprintf("available memory from cgroups is unsupported, using system memory %s instead: %v",
				humanizeutil.IBytes(totalMem), err))
	}
	// Let's special case unlimited memory from cgroups to get a more accurate error message.
	// When memory limit isn't set, cgroups returns 2^63-1 rounded to page size multiple (i.e., 4096).
	if cgAvlMem == 0x7FFFFFFFFFFFF000 {
		return checkTotal(totalMem,
			fmt.Sprintf("available memory from cgroups (%s) is unlimited ('systemd' without MemoryMax?), using system memory %s instead: %s",
				humanize.IBytes(uint64(cgAvlMem)), humanizeutil.IBytes(totalMem), warning))
	}
	if cgAvlMem == 0 || (totalMem > 0 && cgAvlMem > totalMem) {
		return checkTotal(totalMem,
			fmt.Sprintf("available memory from cgroups (%s) is unsupported, using system memory %s instead: %s",
				humanize.IBytes(uint64(cgAvlMem)), humanizeutil.IBytes(totalMem), warning))
	}
	return checkTotal(cgAvlMem, "")
}
