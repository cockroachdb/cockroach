// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package status

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
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
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	"github.com/prometheus/common/expfmt"
)

const (
	// storeTimeSeriesPrefix is the common prefix for time series keys which
	// record store-specific data.
	storeTimeSeriesPrefix = "cr.store.%s"
	// nodeTimeSeriesPrefix is the common prefix for time series keys which
	// record node-specific data.
	nodeTimeSeriesPrefix = "cr.node.%s"

	advertiseAddrLabelKey = "advertise-addr"
	httpAddrLabelKey      = "http-addr"
	sqlAddrLabelKey       = "sql-addr"

	disableNodeAndTenantLabelsEnvVar = "COCKROACH_DISABLE_NODE_AND_TENANT_METRIC_LABELS"
)

// This option is provided as an escape hatch for customers who have
// custom scrape logic that adds relevant labels already.
var disableNodeAndTenantLabels = envutil.EnvOrDefaultBool(disableNodeAndTenantLabelsEnvVar, false)

type quantile struct {
	suffix   string
	quantile float64
}

var recordHistogramQuantiles = []quantile{
	{"-max", 100},
	{"-p99.999", 99.999},
	{"-p99.99", 99.99},
	{"-p99.9", 99.9},
	{"-p99", 99},
	{"-p90", 90},
	{"-p75", 75},
	{"-p50", 50},
}

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
	settings.TenantWritable, "server.child_metrics.enabled",
	"enables the exporting of child metrics, additional prometheus time series with extra labels",
	false).WithPublic()

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
		// nodeRegistry contains, as subregistries, the multiple component-specific
		// registries which are recorded as "node level" metrics.
		nodeRegistry *metric.Registry
		desc         roachpb.NodeDescriptor
		startedAt    int64

		// storeRegistries contains a registry for each store on the node. These
		// are not stored as subregistries, but rather are treated as wholly
		// independent.
		storeRegistries map[roachpb.StoreID]*metric.Registry
		stores          map[roachpb.StoreID]storeMetrics

		// tenantRegistries contains the registries for shared-process tenants.
		tenantRegistries map[roachpb.TenantID]*metric.Registry
	}

	// WriteNodeStatus is a potentially long-running method (with a network
	// round-trip) that requires a mutex to be safe for concurrent usage. We
	// therefore give it its own mutex to avoid blocking other methods.
	writeSummaryMu syncutil.Mutex
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
	mr.mu.tenantRegistries = make(map[roachpb.TenantID]*metric.Registry)
	return mr
}

// AddTenantRegistry adds shared-process tenant's registry.
func (mr *MetricsRecorder) AddTenantRegistry(tenantID roachpb.TenantID, rec *metric.Registry) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if !disableNodeAndTenantLabels {
		// If there are no in-process tenants running, we don't set the
		// tenant label on the system tenant metrics until a seconary
		// tenant is initialized.
		mr.mu.Do(func() {
			mr.mu.nodeRegistry.AddLabel("tenant", catconstants.SystemTenantName)
		})
	}
	mr.mu.tenantRegistries[tenantID] = rec
}

// RemoveTenantRegistry removes shared-process tenant's registry.
func (mr *MetricsRecorder) RemoveTenantRegistry(tenantID roachpb.TenantID) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	delete(mr.mu.tenantRegistries, tenantID)
}

// AddNode adds the Registry from an initialized node, along with its descriptor
// and start time.
func (mr *MetricsRecorder) AddNode(
	reg *metric.Registry,
	desc roachpb.NodeDescriptor,
	startedAt int64,
	advertiseAddr, httpAddr, sqlAddr string,
) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.mu.nodeRegistry = reg
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
	reg.AddMetric(nodeIDGauge)

	if !disableNodeAndTenantLabels {
		nodeIDInt := int(desc.NodeID)
		if nodeIDInt != 0 {
			reg.AddLabel("node_id", strconv.Itoa(int(desc.NodeID)))
			// We assume that all stores have been added to the registry
			// prior to calling `AddNode`.
			for _, s := range mr.mu.storeRegistries {
				s.AddLabel("node_id", strconv.Itoa(int(desc.NodeID)))
			}
		}
		if mr.tenantNameContainer != nil && mr.tenantNameContainer.String() != catconstants.SystemTenantName {
			reg.AddLabel("tenant", mr.tenantNameContainer)
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
			log.Warning(context.TODO(), "MetricsRecorder.MarshalJSON() called before NodeID allocation")
		}
		return []byte("{}"), nil
	}
	topLevel := map[string]interface{}{
		fmt.Sprintf("node.%d", mr.mu.desc.NodeID): mr.mu.nodeRegistry,
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
	mr.mu.RLock()
	defer mr.mu.RUnlock()
	if mr.mu.nodeRegistry == nil {
		// We haven't yet processed initialization information; output nothing.
		if log.V(1) {
			log.Warning(context.TODO(), "MetricsRecorder asked to scrape metrics before NodeID allocation")
		}
	}
	includeChildMetrics := ChildMetricsEnabled.Get(&mr.settings.SV)
	pm.ScrapeRegistry(mr.mu.nodeRegistry, includeChildMetrics)
	for _, reg := range mr.mu.storeRegistries {
		pm.ScrapeRegistry(reg, includeChildMetrics)
	}
	for _, tenantRegistry := range mr.mu.tenantRegistries {
		pm.ScrapeRegistry(tenantRegistry, includeChildMetrics)
	}
}

// PrintAsText writes the current metrics values as plain-text to the writer.
// We write metrics to a temporary buffer which is then copied to the writer.
// This is to avoid hanging requests from holding the lock.
func (mr *MetricsRecorder) PrintAsText(w io.Writer, contentType expfmt.Format) error {
	var buf bytes.Buffer
	if err := mr.prometheusExporter.ScrapeAndPrintAsText(&buf, contentType, mr.ScrapeIntoPrometheus); err != nil {
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
func (mr *MetricsRecorder) GetTimeSeriesData() []tspb.TimeSeriesData {
	mr.mu.RLock()
	defer mr.mu.RUnlock()

	if mr.mu.nodeRegistry == nil {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Warning(context.TODO(), "MetricsRecorder.GetTimeSeriesData() called before NodeID allocation")
		}
		return nil
	}

	lastDataCount := atomic.LoadInt64(&mr.lastDataCount)
	data := make([]tspb.TimeSeriesData, 0, lastDataCount)

	// Record time series from node-level registries for system tenant.
	now := mr.clock.Now()
	recorder := registryRecorder{
		registry:       mr.mu.nodeRegistry,
		format:         nodeTimeSeriesPrefix,
		source:         mr.mu.desc.NodeID.String(),
		timestampNanos: now.UnixNano(),
	}
	recorder.record(&data)

	// Record time series from node-level registries for secondary tenants.
	for tenantID, r := range mr.mu.tenantRegistries {
		tenantRecorder := registryRecorder{
			registry:       r,
			format:         nodeTimeSeriesPrefix,
			source:         tsutil.MakeTenantSource(mr.mu.desc.NodeID.String(), tenantID.String()),
			timestampNanos: now.UnixNano(),
		}
		tenantRecorder.record(&data)
	}

	// Record time series from store-level registries.
	for storeID, r := range mr.mu.storeRegistries {
		storeRecorder := registryRecorder{
			registry:       r,
			format:         storeTimeSeriesPrefix,
			source:         storeID.String(),
			timestampNanos: now.UnixNano(),
		}
		storeRecorder.record(&data)
	}
	atomic.CompareAndSwapInt64(&mr.lastDataCount, lastDataCount, int64(len(data)))
	return data
}

// GetMetricsMetadata returns the metadata from all metrics tracked in the node's
// nodeRegistry and a randomly selected storeRegistry.
func (mr *MetricsRecorder) GetMetricsMetadata() map[string]metric.Metadata {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if mr.mu.nodeRegistry == nil {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Warning(context.TODO(), "MetricsRecorder.GetMetricsMetadata() called before NodeID allocation")
		}
		return nil
	}

	metrics := make(map[string]metric.Metadata)

	mr.mu.nodeRegistry.WriteMetricsMetadata(metrics)

	// Get a random storeID.
	var sID roachpb.StoreID

	storeFound := false
	for storeID := range mr.mu.storeRegistries {
		sID = storeID
		storeFound = true
		break
	}

	// Get metric metadata from that store because all stores have the same metadata.
	if storeFound {
		mr.mu.storeRegistries[sID].WriteMetricsMetadata(metrics)
	}

	return metrics
}

// getNetworkActivity produces a map of network activity from this node to all
// other nodes. Latencies are stored as nanos.
func (mr *MetricsRecorder) getNetworkActivity(
	ctx context.Context,
) map[roachpb.NodeID]statuspb.NodeStatus_NetworkActivity {
	activity := make(map[roachpb.NodeID]statuspb.NodeStatus_NetworkActivity)
	if mr.nodeLiveness != nil {
		isLiveMap := mr.nodeLiveness.GetIsLiveMap()

		var currentAverages map[roachpb.NodeID]time.Duration
		if mr.remoteClocks != nil {
			currentAverages = mr.remoteClocks.AllLatencies()
		}
		for nodeID, entry := range isLiveMap {
			na := statuspb.NodeStatus_NetworkActivity{}
			if entry.IsLive {
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
			log.Warning(ctx, "attempt to generate status summary before NodeID allocation.")
		}
		return nil
	}

	now := mr.clock.Now()

	lastSummaryCount := atomic.LoadInt64(&mr.lastSummaryCount)
	lastNodeMetricCount := atomic.LoadInt64(&mr.lastNodeMetricCount)
	lastStoreMetricCount := atomic.LoadInt64(&mr.lastStoreMetricCount)

	systemMemory, _, err := GetTotalMemoryWithoutLogging()
	if err != nil {
		log.Errorf(ctx, "could not get total system memory: %v", err)
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
		TotalSystemMemory: systemMemory,
	}

	eachRecordableValue(mr.mu.nodeRegistry, func(name string, val float64) {
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
			log.Errorf(ctx, "could not record status summaries: Store %d could not return descriptor, error: %s", storeID, err)
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
			log.Errorf(ctx, "error marshaling nodeStatus to json: %s", err)
		}
		log.Infof(ctx, "node %d status: %s", nodeStatus.Desc.NodeID, statusJSON)
	}
	return nil
}

// registryRecorder is a helper class for recording time series datapoints
// from a metrics Registry.
type registryRecorder struct {
	registry       *metric.Registry
	format         string
	source         string
	timestampNanos int64
}

func extractValue(name string, mtr interface{}, fn func(string, float64)) error {
	switch mtr := mtr.(type) {
	case metric.WindowedHistogram:
		// Use cumulative stats here
		count, sum := mtr.Total()
		fn(name+"-count", float64(count))
		fn(name+"-sum", sum)
		// Use windowed stats for avg and quantiles
		avg := mtr.MeanWindowed()
		if math.IsNaN(avg) || math.IsInf(avg, +1) || math.IsInf(avg, -1) {
			avg = 0
		}
		fn(name+"-avg", avg)
		for _, pt := range recordHistogramQuantiles {
			fn(name+pt.suffix, mtr.ValueAtQuantileWindowed(pt.quantile))
		}
	case metric.PrometheusExportable:
		// NB: this branch is intentionally at the bottom since all metrics implement it.
		m := mtr.ToPrometheusMetric()
		if m.Gauge != nil {
			fn(name, *m.Gauge.Value)
		} else if m.Counter != nil {
			fn(name, *m.Counter.Value)
		}

	default:
		return errors.Errorf("cannot extract value for type %T", mtr)
	}
	return nil
}

// eachRecordableValue visits each metric in the registry, calling the supplied
// function once for each recordable value represented by that metric. This is
// useful to expand certain metric types (such as histograms) into multiple
// recordable values.
func eachRecordableValue(reg *metric.Registry, fn func(string, float64)) {
	reg.Each(func(name string, mtr interface{}) {
		if err := extractValue(name, mtr, fn); err != nil {
			log.Warningf(context.TODO(), "%v", err)
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

// GetTotalMemory returns either the total system memory (in bytes) or if
// possible the cgroups available memory.
func GetTotalMemory(ctx context.Context) (int64, error) {
	memory, warning, err := GetTotalMemoryWithoutLogging()
	if err != nil {
		return 0, err
	}
	if warning != "" {
		log.Infof(ctx, "%s", warning)
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
