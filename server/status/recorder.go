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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package status

import (
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/hlc"
)

const (
	// storeTimeSeriesNameFmt is the current format for cockroach's
	// store-specific time series keys. Each key has a prefix of "cr.store",
	// followed by the name of the specific stat, followed by the StoreID.
	//
	// For example, the livebytes stats for Store with ID 1 would be stored with
	// key:
	//		cr.store.livebytes.1
	//
	// This format has been chosen to put the StoreID as the suffix of keys, in
	// anticipation of an initially simple query system where only key suffixes
	// can be wildcarded.
	storeTimeSeriesNameFmt = "cr.store.%s.%d"
	// nodeTimeSeriesFmt is the current format for time series keys which record
	// node-specific data.
	nodeTimeSeriesNameFmt = "cr.node.%s.%d"
	// runtimeStatTimeSeriesFmt is the current format for time series keys which
	// record runtime system stats on a node.
	runtimeStatTimeSeriesNameFmt = "cr.node.sys.%s.%d"
)

// NodeStatusRecorder is used to periodically persist the status of a node as a
// set of time series data.
type NodeStatusRecorder struct {
	*NodeStatusMonitor
	clock         *hlc.Clock
	lastDataCount int
}

// NewNodeStatusRecorder instantiates a recorder for the supplied monitor.
func NewNodeStatusRecorder(monitor *NodeStatusMonitor, clock *hlc.Clock) *NodeStatusRecorder {
	return &NodeStatusRecorder{
		NodeStatusMonitor: monitor,
		clock:             clock,
	}
}

// recordInt records a single int64 value from the NodeStatusMonitor as a
// proto.TimeSeriesData object.
func (nsr *NodeStatusRecorder) recordInt(timestampNanos int64, name string,
	data int64) proto.TimeSeriesData {
	return proto.TimeSeriesData{
		Name: fmt.Sprintf(nodeTimeSeriesNameFmt, name, nsr.nodeID),
		Datapoints: []*proto.TimeSeriesDatapoint{
			{
				TimestampNanos: timestampNanos,
				Value:          float64(data),
			},
		},
	}
}

// GetTimeSeriesData returns a slice of interesting TimeSeriesData from the
// encapsulated NodeStatusMonitor.
func (nsr *NodeStatusRecorder) GetTimeSeriesData() []proto.TimeSeriesData {
	data := make([]proto.TimeSeriesData, 0, nsr.lastDataCount)
	// Record node stats.
	if nsr.nodeID > 0 {
		now := nsr.clock.PhysicalNow()
		data = append(data, nsr.recordInt(now, "calls.success", atomic.LoadInt64(&nsr.callCount)))
		data = append(data, nsr.recordInt(now, "calls.error", atomic.LoadInt64(&nsr.callErrors)))
	}
	// Record per store stats.
	nsr.VisitStoreMonitors(func(ssm *StoreStatusMonitor) {
		now := nsr.clock.PhysicalNow()
		ssr := storeStatusRecorder{ssm, now}
		data = append(data, ssr.recordInt("livebytes", ssr.stats.LiveBytes))
		data = append(data, ssr.recordInt("keybytes", ssr.stats.KeyBytes))
		data = append(data, ssr.recordInt("valbytes", ssr.stats.ValBytes))
		data = append(data, ssr.recordInt("intentbytes", ssr.stats.IntentBytes))
		data = append(data, ssr.recordInt("livecount", ssr.stats.LiveCount))
		data = append(data, ssr.recordInt("keycount", ssr.stats.KeyCount))
		data = append(data, ssr.recordInt("valcount", ssr.stats.ValCount))
		data = append(data, ssr.recordInt("intentcount", ssr.stats.IntentCount))
		data = append(data, ssr.recordInt("intentage", ssr.stats.IntentAge))
		data = append(data, ssr.recordInt("gcbytesage", ssr.stats.GCBytesAge))
		data = append(data, ssr.recordInt("lastupdatenanos", ssr.stats.LastUpdateNanos))
		data = append(data, ssr.recordInt("ranges", ssr.rangeCount))
		data = append(data, ssr.recordInt("ranges.leader", int64(ssr.leaderRangeCount)))
		data = append(data, ssr.recordInt("ranges.replicated", int64(ssr.replicatedRangeCount)))
		data = append(data, ssr.recordInt("ranges.available", int64(ssr.availableRangeCount)))

		// Record statistics from descriptor.
		if ssr.desc != nil {
			capacity := ssr.desc.Capacity
			data = append(data, ssr.recordInt("capacity", int64(capacity.Capacity)))
			data = append(data, ssr.recordInt("capacity.available", int64(capacity.Available)))
		}
	})
	nsr.lastDataCount = len(data)
	return data
}

// storeStatusRecorder is a helper class for recording time series datapoints
// from a single StoreStatusMonitor.
type storeStatusRecorder struct {
	*StoreStatusMonitor
	timestampNanos int64
}

// recordInt records a single int64 value from the StoreStatusMonitor as a
// proto.TimeSeriesData object.
func (ssr *storeStatusRecorder) recordInt(name string, data int64) proto.TimeSeriesData {
	return proto.TimeSeriesData{
		Name: fmt.Sprintf(storeTimeSeriesNameFmt, name, ssr.ID),
		Datapoints: []*proto.TimeSeriesDatapoint{
			{
				TimestampNanos: ssr.timestampNanos,
				Value:          float64(data),
			},
		},
	}
}
