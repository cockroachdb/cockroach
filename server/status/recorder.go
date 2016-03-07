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
	"strconv"
	"sync"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
)

const (
	// storeTimeSeriesPrefix is the common prefix for time series keys which
	// record store-specific data.
	storeTimeSeriesPrefix = "cr.store.%s"
	// nodeTimeSeriesPrefix is the common prefix for time series keys which
	// record node-specific data.
	nodeTimeSeriesPrefix = "cr.node.%s"
	// runtimeStatTimeSeriesFmt is the current format for time series keys which
	// record runtime system stats on a node.
	runtimeStatTimeSeriesNameFmt = "cr.node.sys.%s"
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
	MVCCStats() engine.MVCCStats
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
	// nodeRegistry contains, as subregistries, the multiple component-specific
	// registries which are recorded as "node level" metrics.
	nodeRegistry *metric.Registry

	// Fields below are locked by this mutex.
	mu struct {
		sync.Mutex
		// storeRegistries contains a registry for each store on the node. These
		// are not stored as subregistries, but rather are treated as wholly
		// independent.
		storeRegistries map[roachpb.StoreID]*metric.Registry
		nodeID          roachpb.NodeID
		clock           *hlc.Clock

		// Counts to help optimize slice allocation.
		lastDataCount    int
		lastSummaryCount int

		// TODO(mrtracy): These are stored to support the current structure of
		// status summaries. These should be removed as part of #4465.
		startedAt int64
		desc      roachpb.NodeDescriptor
		stores    map[roachpb.StoreID]storeMetrics
	}
}

// NewMetricsRecorder initializes a new MetricsRecorder object that uses the
// given clock.
func NewMetricsRecorder(clock *hlc.Clock) *MetricsRecorder {
	mr := &MetricsRecorder{
		nodeRegistry: metric.NewRegistry(),
	}
	mr.mu.storeRegistries = make(map[roachpb.StoreID]*metric.Registry)
	mr.mu.stores = make(map[roachpb.StoreID]storeMetrics)
	mr.mu.clock = clock
	return mr
}

// AddNodeRegistry adds a node-level registry to this recorder. Each node-level
// registry has a 'prefix format' which is used to add a prefix to the name of
// all metrics in that registry while recording (see the metric.Registry object
// for more information on prefix format strings).
func (mr *MetricsRecorder) AddNodeRegistry(prefixFmt string, registry *metric.Registry) {
	mr.nodeRegistry.MustAdd(prefixFmt, registry)
}

// AddStore adds the Registry from the provided store as a store-level registry
// in this recoder. A reference to the store is kept for the purpose of
// gathering some additional information which is present in store status
// summaries.
// Stores should only be added to the registry after they have been started.
// TODO(mrtracy): Store references should not be necessary after #4465.
func (mr *MetricsRecorder) AddStore(store storeMetrics) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	storeID := store.StoreID()
	mr.mu.storeRegistries[storeID] = store.Registry()
	mr.mu.stores[storeID] = store
}

// NodeStarted should be called on the recorder once the associated node has
// received its Node ID; this indicates that it is appropriate to begin
// recording statistics for this node.
func (mr *MetricsRecorder) NodeStarted(desc roachpb.NodeDescriptor, startedAt int64) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.mu.desc = desc
	mr.mu.nodeID = desc.NodeID
	mr.mu.startedAt = startedAt
}

// MarshalJSON returns an appropriate JSON representation of the current values
// of the metrics being tracked by this recorder.
func (mr *MetricsRecorder) MarshalJSON() ([]byte, error) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if mr.mu.nodeID == 0 {
		// We haven't yet processed initialization information; return an empty
		// JSON object.
		if log.V(1) {
			log.Warning("MetricsRecorder.MarshalJSON() called before NodeID allocation")
		}
		return []byte("{}"), nil
	}
	topLevel := map[string]interface{}{
		fmt.Sprintf("node.%d", mr.mu.nodeID): mr.nodeRegistry,
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

// GetTimeSeriesData serializes registered metrics for consumption by
// CockroachDB's time series system.
func (mr *MetricsRecorder) GetTimeSeriesData() []ts.TimeSeriesData {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if mr.mu.desc.NodeID == 0 {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Warning("MetricsRecorder.GetTimeSeriesData() called before NodeID allocation")
		}
		return nil
	}

	data := make([]ts.TimeSeriesData, 0, mr.mu.lastDataCount)

	// Record time series from node-level registries.
	now := mr.mu.clock.PhysicalNow()
	recorder := registryRecorder{
		registry:       mr.nodeRegistry,
		format:         nodeTimeSeriesPrefix,
		source:         strconv.FormatInt(int64(mr.mu.nodeID), 10),
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

// GetStatusSummaries returns a status summary messages for the node, along with
// a status summary for every individual store within the node.
// TODO(mrtracy): The status summaries deserve a near-term, significant
// overhaul. Their primary usage is as an indicator of the most recent metrics
// of a node or store - they are essentially a "vertical" query of several
// time series for a single node or store, returning only the most recent value
// of each series. The structure should be modified to reflect that: there is no
// reason for them to have a strict schema of fields. (Github Issue #4465)
func (mr *MetricsRecorder) GetStatusSummaries() (*NodeStatus, []storage.StoreStatus) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if mr.mu.nodeID == 0 {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Warning("MetricsRecorder.GetStatusSummaries called before NodeID allocation.")
		}
		return nil, nil
	}

	now := mr.mu.clock.PhysicalNow()

	// Generate an node status with no store data.
	nodeStat := &NodeStatus{
		Desc:      mr.mu.desc,
		UpdatedAt: now,
		StartedAt: mr.mu.startedAt,
		StoreIDs:  make([]roachpb.StoreID, 0, mr.mu.lastSummaryCount),
	}

	storeStats := make([]storage.StoreStatus, 0, mr.mu.lastSummaryCount)
	// Generate status summaries for stores, while accumulating data into the
	// NodeStatus.
	for storeID, r := range mr.mu.storeRegistries {
		nodeStat.StoreIDs = append(nodeStat.StoreIDs, storeID)

		// Gather MVCCStats from the store directly.
		stats := mr.mu.stores[storeID].MVCCStats()

		// Gather updates from a few specific gauges.
		// TODO(mrtracy): This is the worst hack present in supporting the
		// current status summary format. It will be removed as part of #4465.
		rangeCounter := r.GetCounter("ranges")
		if rangeCounter == nil {
			log.Errorf("Could not record status summaries: Store %d did not have 'ranges' counter in registry.", storeID)
			return nil, nil
		}
		gaugeNames := []string{"ranges.leader", "ranges.replicated", "ranges.available"}
		gauges := make(map[string]*metric.Gauge)
		for _, name := range gaugeNames {
			gauge := r.GetGauge(name)
			if gauge == nil {
				log.Errorf("Could not record status summaries: Store %d did not have '%s' gauge in registry.", storeID, name)
				return nil, nil
			}
			gauges[name] = gauge
		}

		// Gather descriptor from store.
		descriptor, err := mr.mu.stores[storeID].Descriptor()
		if err != nil {
			log.Errorf("Could not record status summaries: Store %d could not return descriptor, error: %s", storeID, err)
		}

		status := storage.StoreStatus{
			Desc:                 *descriptor,
			NodeID:               mr.mu.nodeID,
			UpdatedAt:            now,
			StartedAt:            mr.mu.startedAt,
			Stats:                stats,
			RangeCount:           int32(rangeCounter.Count()),
			LeaderRangeCount:     int32(gauges[gaugeNames[0]].Value()),
			ReplicatedRangeCount: int32(gauges[gaugeNames[1]].Value()),
			AvailableRangeCount:  int32(gauges[gaugeNames[2]].Value()),
		}
		nodeStat.Stats.Add(stats)
		nodeStat.RangeCount += status.RangeCount
		nodeStat.LeaderRangeCount += status.LeaderRangeCount
		nodeStat.ReplicatedRangeCount += status.ReplicatedRangeCount
		nodeStat.AvailableRangeCount += status.AvailableRangeCount
		storeStats = append(storeStats, status)
	}
	return nodeStat, storeStats
}

// registryRecorder is a helper class for recording time series datapoints
// from a metrics Registry.
type registryRecorder struct {
	registry       *metric.Registry
	format         string
	source         string
	timestampNanos int64
}

func (rr registryRecorder) record(dest *[]ts.TimeSeriesData) {
	rr.registry.Each(func(name string, m interface{}) {
		data := ts.TimeSeriesData{
			Name:   fmt.Sprintf(rr.format, name),
			Source: rr.source,
			Datapoints: []*ts.TimeSeriesDatapoint{
				{
					TimestampNanos: rr.timestampNanos,
				},
			},
		}
		// The method for extracting data differs based on the type of metric.
		// TODO(tschottdorf): should make this based on interfaces.
		switch mtr := m.(type) {
		case float64:
			data.Datapoints[0].Value = mtr
		case *metric.Rates:
			data.Datapoints[0].Value = float64(mtr.Count())
		case *metric.Counter:
			data.Datapoints[0].Value = float64(mtr.Count())
		case *metric.Gauge:
			data.Datapoints[0].Value = float64(mtr.Value())
		case *metric.Histogram:
			h := mtr.Current()
			for _, pt := range recordHistogramQuantiles {
				d := *util.CloneProto(&data).(*ts.TimeSeriesData)
				d.Name += pt.suffix
				d.Datapoints[0].Value = float64(h.ValueAtQuantile(pt.quantile))
				*dest = append(*dest, d)
			}
			return
		default:
			log.Warningf("cannot serialize for time series: %T", mtr)
			return
		}
		*dest = append(*dest, data)
	})
}
