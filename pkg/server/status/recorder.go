// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package status

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
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
)

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
	Descriptor() (*roachpb.StoreDescriptor, error)
	MVCCStats() enginepb.MVCCStats
	Registry() *metric.Registry
}

// MetricsRecorder is used to periodically record the information in a number of
// metric registries.
//
// Two types of registries are maintained: "node-level" registries, provided by
// node-level systems, and "store-level" registries which are provided by each
// store hosted by the node. There are slight differences in the way these are
// recorded, and they are thus kept separate.
type MetricsRecorder struct {
	mu struct {
		syncutil.Mutex
		// prometheusExporter merges metrics into families and generates the
		// prometheus text format.
		prometheusExporter metric.PrometheusExporter
		// nodeRegistry contains, as subregistries, the multiple component-specific
		// registries which are recorded as "node level" metrics.
		nodeRegistry *metric.Registry
		desc         roachpb.NodeDescriptor
		startedAt    int64

		// storeRegistries contains a registry for each store on the node. These
		// are not stored as subregistries, but rather are treated as wholly
		// independent.
		storeRegistries map[roachpb.StoreID]*metric.Registry
		clock           *hlc.Clock
		stores          map[roachpb.StoreID]storeMetrics

		// Counts to help optimize slice allocation.
		lastDataCount        int
		lastSummaryCount     int
		lastNodeMetricCount  int
		lastStoreMetricCount int
	}
	// WriteStatusSummary is a potentially long-running method (with a network
	// round-trip) that requires a mutex to be safe for concurrent usage. We
	// therefore give it its own mutex to avoid blocking other methods.
	writeSummaryMu syncutil.Mutex
}

// NewMetricsRecorder initializes a new MetricsRecorder object that uses the
// given clock.
func NewMetricsRecorder(clock *hlc.Clock) *MetricsRecorder {
	mr := &MetricsRecorder{}
	mr.mu.storeRegistries = make(map[roachpb.StoreID]*metric.Registry)
	mr.mu.stores = make(map[roachpb.StoreID]storeMetrics)
	mr.mu.prometheusExporter = metric.MakePrometheusExporter()
	mr.mu.clock = clock
	return mr
}

// AddNode adds the Registry from an initialized node, along with its descriptor
// and start time.
func (mr *MetricsRecorder) AddNode(
	reg *metric.Registry,
	desc roachpb.NodeDescriptor,
	startedAt int64,
	advertiseAddr, httpAddr string,
) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.mu.nodeRegistry = reg
	mr.mu.desc = desc
	mr.mu.startedAt = startedAt

	// Create node ID gauge metric with host as a label.
	metadata := metric.Metadata{
		Name: "node-id",
		Help: "node ID with labels for advertised RPC and HTTP addresses",
	}
	metadata.AddLabel(advertiseAddrLabelKey, advertiseAddr)
	metadata.AddLabel(httpAddrLabelKey, httpAddr)
	nodeIDGauge := metric.NewGauge(metadata)
	nodeIDGauge.Update(int64(desc.NodeID))
	reg.AddMetric(nodeIDGauge)
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
	mr.mu.Lock()
	defer mr.mu.Unlock()
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

// scrapePrometheusLocked updates the prometheusExporter's metrics snapshot.
func (mr *MetricsRecorder) scrapePrometheusLocked() {
	if mr.mu.nodeRegistry == nil {
		// We haven't yet processed initialization information; output nothing.
		if log.V(1) {
			log.Warning(context.TODO(), "MetricsRecorder asked to scrape metrics before NodeID allocation")
		}
	}

	mr.mu.prometheusExporter.ScrapeRegistry(mr.mu.nodeRegistry)
	for _, reg := range mr.mu.storeRegistries {
		mr.mu.prometheusExporter.ScrapeRegistry(reg)
	}
}

// PrintAsText writes the current metrics values as plain-text to the writer.
func (mr *MetricsRecorder) PrintAsText(w io.Writer) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.scrapePrometheusLocked()
	return mr.mu.prometheusExporter.PrintAsText(w)
}

// GetTimeSeriesData serializes registered metrics for consumption by
// CockroachDB's time series system.
func (mr *MetricsRecorder) GetTimeSeriesData() []tspb.TimeSeriesData {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if mr.mu.nodeRegistry == nil {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Warning(context.TODO(), "MetricsRecorder.GetTimeSeriesData() called before NodeID allocation")
		}
		return nil
	}

	data := make([]tspb.TimeSeriesData, 0, mr.mu.lastDataCount)

	// Record time series from node-level registries.
	now := mr.mu.clock.PhysicalNow()
	recorder := registryRecorder{
		registry:       mr.mu.nodeRegistry,
		format:         nodeTimeSeriesPrefix,
		source:         strconv.FormatInt(int64(mr.mu.desc.NodeID), 10),
		timestampNanos: now,
	}
	recorder.record(&data)

	// Record time series from store-level registries.
	for storeID, r := range mr.mu.storeRegistries {
		storeRecorder := registryRecorder{
			registry:       r,
			format:         storeTimeSeriesPrefix,
			source:         strconv.FormatInt(int64(storeID), 10),
			timestampNanos: now,
		}
		storeRecorder.record(&data)
	}
	mr.mu.lastDataCount = len(data)
	return data
}

// GetStatusSummary returns a status summary messages for the node. The summary
// includes the recent values of metrics for both the node and all of its
// component stores.
func (mr *MetricsRecorder) GetStatusSummary() *NodeStatus {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if mr.mu.nodeRegistry == nil {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Warning(context.TODO(), "attempt to generate status summary before NodeID allocation.")
		}
		return nil
	}

	now := mr.mu.clock.PhysicalNow()

	// Generate a node status with no store data.
	nodeStat := &NodeStatus{
		Desc:          mr.mu.desc,
		BuildInfo:     build.GetInfo(),
		UpdatedAt:     now,
		StartedAt:     mr.mu.startedAt,
		StoreStatuses: make([]StoreStatus, 0, mr.mu.lastSummaryCount),
		Metrics:       make(map[string]float64, mr.mu.lastNodeMetricCount),
		Args:          os.Args,
		Env:           envutil.GetEnvVarsUsed(),
	}

	eachRecordableValue(mr.mu.nodeRegistry, func(name string, val float64) {
		nodeStat.Metrics[name] = val
	})

	// Generate status summaries for stores.
	for storeID, r := range mr.mu.storeRegistries {
		storeMetrics := make(map[string]float64, mr.mu.lastStoreMetricCount)
		eachRecordableValue(r, func(name string, val float64) {
			storeMetrics[name] = val
		})

		// Gather descriptor from store.
		descriptor, err := mr.mu.stores[storeID].Descriptor()
		if err != nil {
			log.Errorf(context.TODO(), "Could not record status summaries: Store %d could not return descriptor, error: %s", storeID, err)
			continue
		}

		nodeStat.StoreStatuses = append(nodeStat.StoreStatuses, StoreStatus{
			Desc:    *descriptor,
			Metrics: storeMetrics,
		})
	}
	mr.mu.lastSummaryCount = len(nodeStat.StoreStatuses)
	mr.mu.lastNodeMetricCount = len(nodeStat.Metrics)
	if len(nodeStat.StoreStatuses) > 0 {
		mr.mu.lastStoreMetricCount = len(nodeStat.StoreStatuses[0].Metrics)
	}
	return nodeStat
}

// WriteStatusSummary generates a summary and immediately writes it to the given
// client.
func (mr *MetricsRecorder) WriteStatusSummary(ctx context.Context, db *client.DB) error {
	mr.writeSummaryMu.Lock()
	defer mr.writeSummaryMu.Unlock()

	nodeStatus := mr.GetStatusSummary()
	if nodeStatus != nil {
		key := keys.NodeStatusKey(nodeStatus.Desc.NodeID)
		// We use PutInline to store only a single version of the node status.
		// There's not much point in keeping the historical versions as we keep
		// all of the constituent data as timeseries. Further, due to the size
		// of the build info in the node status, writing one of these every 10s
		// will generate more versions than will easily fit into a range over
		// the course of a day.
		if err := db.PutInline(ctx, key, nodeStatus); err != nil {
			return err
		}
		if log.V(2) {
			statusJSON, err := json.Marshal(nodeStatus)
			if err != nil {
				log.Errorf(ctx, "error marshaling nodeStatus to json: %s", err)
			}
			log.Infof(ctx, "node %d status: %s", nodeStatus.Desc.NodeID, statusJSON)
		}
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

func extractValue(mtr interface{}) (float64, error) {
	// TODO(tschottdorf|mrtracy): consider moving this switch to an interface
	// implemented by the individual metric types.
	switch mtr := mtr.(type) {
	case float64:
		return mtr, nil
	case *metric.CounterWithRates:
		return float64(mtr.Count()), nil
	case *metric.Counter:
		return float64(mtr.Count()), nil
	case *metric.Gauge:
		return float64(mtr.Value()), nil
	case *metric.GaugeFloat64:
		return mtr.Value(), nil
	default:
		return 0, errors.Errorf("cannot extract value for type %T", mtr)
	}
}

// eachRecordableValue visits each metric in the registry, calling the supplied
// function once for each recordable value represented by that metric. This is
// useful to expand certain metric types (such as histograms) into multiple
// recordable values.
func eachRecordableValue(reg *metric.Registry, fn func(string, float64)) {
	reg.Each(func(name string, mtr interface{}) {
		if histogram, ok := mtr.(*metric.Histogram); ok {
			// TODO(mrtracy): Where should this comment go for better
			// visibility?
			//
			// Proper support of Histograms for time series is difficult and
			// likely not worth the trouble. Instead, we aggregate a windowed
			// histogram at fixed quantiles. If the scraping window and the
			// histogram's eviction duration are similar, this should give
			// good results; if the two durations are very different, we either
			// report stale results or report only the more recent data.
			//
			// Additionally, we can only aggregate max/min of the quantiles;
			// roll-ups don't know that and so they will return mathematically
			// nonsensical values, but that seems acceptable for the time
			// being.
			curr, _ := histogram.Windowed()
			for _, pt := range recordHistogramQuantiles {
				fn(name+pt.suffix, float64(curr.ValueAtQuantile(pt.quantile)))
			}
		} else {
			val, err := extractValue(mtr)
			if err != nil {
				log.Warning(context.TODO(), err)
				return
			}
			fn(name, val)
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
