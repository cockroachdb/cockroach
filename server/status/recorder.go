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
		lastDataCount        int
		lastSummaryCount     int
		lastNodeMetricCount  int
		lastStoreMetricCount int

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

// GetStatusSummary returns a status summary messages for the node. The summary
// includes the recent values of metrics for both the node and all of its
// component stores.
func (mr *MetricsRecorder) GetStatusSummary() *NodeStatus {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if mr.mu.nodeID == 0 {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Warning("MetricsRecorder.GetStatusSummary called before NodeID allocation.")
		}
		return nil
	}

	now := mr.mu.clock.PhysicalNow()

	// Generate an node status with no store data.
	nodeStat := &NodeStatus{
		Desc:          mr.mu.desc,
		BuildInfo:     util.GetBuildInfo(),
		UpdatedAt:     now,
		StartedAt:     mr.mu.startedAt,
		StoreStatuses: make([]StoreStatus, 0, mr.mu.lastSummaryCount),
		Metrics:       make(map[string]float64, mr.mu.lastNodeMetricCount),
	}

	eachRecordableValue(mr.nodeRegistry, func(name string, val float64) {
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
			log.Errorf("Could not record status summaries: Store %d could not return descriptor, error: %s", storeID, err)
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
	case *metric.Rates:
		return float64(mtr.Count()), nil
	case *metric.Counter:
		return float64(mtr.Count()), nil
	case *metric.Gauge:
		return float64(mtr.Value()), nil
	case *metric.GaugeFloat64:
		return mtr.Value(), nil
	default:
		return 0, util.Errorf("cannot extract value for type %T", mtr)
	}
}

// eachRecordableValue visits each metric in the registry, calling the supplied
// function once for each recordable value represented by that metric. This is
// useful to expand certain metric types (such as histograms) into multiple
// recordable values.
func eachRecordableValue(reg *metric.Registry, fn func(string, float64)) {
	reg.Each(func(name string, mtr interface{}) {
		if histogram, ok := mtr.(*metric.Histogram); ok {
			curr := histogram.Current()
			for _, pt := range recordHistogramQuantiles {
				fn(name+pt.suffix, float64(curr.ValueAtQuantile(pt.quantile)))
			}
		} else {
			val, err := extractValue(mtr)
			if err != nil {
				log.Warning(err)
				return
			}
			fn(name, val)
		}
	})
}

func (rr registryRecorder) record(dest *[]ts.TimeSeriesData) {
	eachRecordableValue(rr.registry, func(name string, val float64) {
		*dest = append(*dest, ts.TimeSeriesData{
			Name:   fmt.Sprintf(rr.format, name),
			Source: rr.source,
			Datapoints: []ts.TimeSeriesDatapoint{
				{
					TimestampNanos: rr.timestampNanos,
					Value:          val,
				},
			},
		})
	})
}
