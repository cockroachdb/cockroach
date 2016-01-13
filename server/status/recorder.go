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
	"strconv"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/ts"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/gogo/protobuf/proto"
)

const (
	// storeTimeSeriesPrefix is the common prefix for time series keys which
	// record store-specific data.
	storeTimeSeriesPrefix = "cr.store."
	// nodeTimeSeriesPrefix is the common prefix for time series keys which
	// record node-specific data.
	nodeTimeSeriesPrefix = "cr.node."
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

// NodeStatusRecorder is used to periodically persist the status of a node as a
// set of time series data.
type NodeStatusRecorder struct {
	*NodeStatusMonitor
	clock            *hlc.Clock
	source           string // Source string used when storing time series data for this node.
	lastDataCount    int
	lastSummaryCount int
}

// NewNodeStatusRecorder instantiates a recorder for the supplied monitor.
func NewNodeStatusRecorder(monitor *NodeStatusMonitor, clock *hlc.Clock) *NodeStatusRecorder {
	return &NodeStatusRecorder{
		NodeStatusMonitor: monitor,
		clock:             clock,
	}
}

// GetTimeSeriesData returns a slice of interesting TimeSeriesData from the
// encapsulated NodeStatusMonitor.
func (nsr *NodeStatusRecorder) GetTimeSeriesData() []ts.TimeSeriesData {
	nsr.RLock()
	defer nsr.RUnlock()

	if nsr.desc.NodeID == 0 {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Warning("NodeStatusRecorder.GetTimeSeriesData called before StartNode event received.")
		}
		return nil
	}
	if nsr.source == "" {
		nsr.source = strconv.FormatInt(int64(nsr.desc.NodeID), 10)
	}

	data := make([]ts.TimeSeriesData, 0, nsr.lastDataCount)

	// Record node stats.
	now := nsr.clock.PhysicalNow()
	recorder := registryRecorder{
		registry:       nsr.registry,
		prefix:         nodeTimeSeriesPrefix,
		source:         nsr.source,
		timestampNanos: now,
	}
	recorder.record(&data)

	// Record per store stats.
	nsr.visitStoreMonitors(func(ssm *StoreStatusMonitor) {
		now := nsr.clock.PhysicalNow()
		storeRecorder := registryRecorder{
			registry:       ssm.registry,
			prefix:         storeTimeSeriesPrefix,
			source:         strconv.FormatInt(int64(ssm.ID), 10),
			timestampNanos: now,
		}
		storeRecorder.record(&data)
	})
	nsr.lastDataCount = len(data)
	return data
}

// GetStatusSummaries returns a status summary messages for the node, along with
// a status summary for every individual store within the node.
func (nsr *NodeStatusRecorder) GetStatusSummaries() (*NodeStatus, []storage.StoreStatus) {
	nsr.RLock()
	defer nsr.RUnlock()

	if nsr.desc.NodeID == 0 {
		// We haven't yet processed initialization information; do nothing.
		if log.V(1) {
			log.Warning("NodeStatusRecorder.GetStatusSummaries called before StartNode event received.")
		}
		return nil, nil
	}

	now := nsr.clock.PhysicalNow()

	// Generate an node status with no store data.
	nodeStat := &NodeStatus{
		Desc:      nsr.desc,
		UpdatedAt: now,
		StartedAt: nsr.startedAt,
		StoreIDs:  make([]roachpb.StoreID, 0, nsr.lastSummaryCount),
	}

	storeStats := make([]storage.StoreStatus, 0, nsr.lastSummaryCount)
	// Generate status summaries for stores, while accumulating data into the
	// NodeStatus.
	nsr.visitStoreMonitors(func(ssm *StoreStatusMonitor) {
		// Accumulate per-store values into node status.
		// TODO(mrtracy): A number of the fields on the protocol buffer are
		// Int32s when they would be more easily represented as Int64.
		nodeStat.StoreIDs = append(nodeStat.StoreIDs, ssm.ID)
		nodeStat.Stats.Add(ssm.stats)
		nodeStat.RangeCount += int32(ssm.rangeCount.Count())
		nodeStat.LeaderRangeCount += int32(ssm.leaderRangeCount.Value())
		nodeStat.ReplicatedRangeCount += int32(ssm.replicatedRangeCount.Value())
		nodeStat.AvailableRangeCount += int32(ssm.availableRangeCount.Value())

		// Its difficult to guarantee that we have the store descriptor yet; we
		// may not have processed a StoreStatusEvent yet for this store. Just
		// skip the store summary in this case.
		if ssm.desc == nil {
			return
		}
		status := storage.StoreStatus{
			Desc:                 *ssm.desc,
			NodeID:               nsr.desc.NodeID,
			UpdatedAt:            now,
			StartedAt:            ssm.startedAt,
			Stats:                ssm.stats,
			RangeCount:           int32(ssm.rangeCount.Count()),
			LeaderRangeCount:     int32(ssm.leaderRangeCount.Value()),
			ReplicatedRangeCount: int32(ssm.replicatedRangeCount.Value()),
			AvailableRangeCount:  int32(ssm.availableRangeCount.Value()),
		}
		storeStats = append(storeStats, status)
	})
	return nodeStat, storeStats
}

// registryRecorder is a helper class for recording time series datapoints
// from a metrics Registry.
type registryRecorder struct {
	registry       *metric.Registry
	prefix         string
	source         string
	timestampNanos int64
}

func (rr registryRecorder) record(dest *[]ts.TimeSeriesData) {
	rr.registry.Each(func(name string, m interface{}) {
		data := ts.TimeSeriesData{
			Name:   rr.prefix + name,
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
				d := *proto.Clone(&data).(*ts.TimeSeriesData)
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
