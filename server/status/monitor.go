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
	"sync"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/tracer"
)

// NodeStatusMonitor monitors the status of a server node. Status information
// is collected from event feeds provided by lower level components.
//
// This structure contains collections of other StatusMonitor types which monitor
// interesting subsets of data on the node. NodeStatusMonitor is responsible
// for passing event feed data to these subset structures for accumulation.
type NodeStatusMonitor struct {
	mLatency metric.Histograms
	mSuccess metric.Rates
	mError   metric.Rates

	sync.RWMutex // Mutex to guard the following fields
	registry     *metric.Registry
	metaRegistry *metric.Registry
	stores       map[roachpb.StoreID]*StoreStatusMonitor
	desc         roachpb.NodeDescriptor
	startedAt    int64
}

// NewNodeStatusMonitor initializes a new NodeStatusMonitor instance.
func NewNodeStatusMonitor(metaRegistry *metric.Registry) *NodeStatusMonitor {
	registry := metric.NewRegistry()
	return &NodeStatusMonitor{
		stores:       make(map[roachpb.StoreID]*StoreStatusMonitor),
		metaRegistry: metaRegistry,
		registry:     registry,

		mLatency: registry.Latency("exec.latency."),
		mSuccess: registry.Rates("exec.success."),
		mError:   registry.Rates("exec.error."),
	}
}

// GetStoreMonitor is a helper method which retrieves the StoreStatusMonitor for the
// given StoreID, creating it if it does not already exist.
func (nsm *NodeStatusMonitor) GetStoreMonitor(id roachpb.StoreID) *StoreStatusMonitor {
	nsm.RLock()
	s, ok := nsm.stores[id]
	nsm.RUnlock()
	if ok {
		return s
	}

	// Rare case where store did not already exist, we need to take an actual
	// lock.
	nsm.Lock()
	defer nsm.Unlock()
	if s, ok = nsm.stores[id]; ok {
		return s
	}
	s = NewStoreStatusMonitor(id, nsm.metaRegistry)
	nsm.stores[id] = s
	return s
}

// visitStoreMonitors calls the supplied visitor function with every
// StoreStatusMonitor currently in this monitor's collection. A lock is taken on
// each StoreStatusMonitor before it is passed to the visitor function.
func (nsm *NodeStatusMonitor) visitStoreMonitors(visitor func(*StoreStatusMonitor)) {
	for _, ssm := range nsm.stores {
		ssm.Lock()
		visitor(ssm)
		ssm.Unlock()
	}
}

// StartMonitorFeed starts a goroutine which processes events published to the
// supplied Subscription. The goroutine will continue running until the
// Subscription's Events feed is closed.
func (nsm *NodeStatusMonitor) StartMonitorFeed(feed *util.Feed) {
	feed.Subscribe(func(event interface{}) {
		ProcessNodeEvent(nsm, event)
		storage.ProcessStoreEvent(nsm, event)
	})
}

// OnRegisterRange receives RegisterRangeEvents retrieved from a storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnRegisterRange(event *storage.RegisterRangeEvent) {
	nsm.GetStoreMonitor(event.StoreID).registerRange(event)
}

// OnUpdateRange receives UpdateRangeEvents retrieved from a storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnUpdateRange(event *storage.UpdateRangeEvent) {
	nsm.GetStoreMonitor(event.StoreID).updateRange(event)
}

// OnRemoveRange receives RemoveRangeEvents retrieved from a storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnRemoveRange(event *storage.RemoveRangeEvent) {
	nsm.GetStoreMonitor(event.StoreID).removeRange(event)
}

// OnSplitRange receives SplitRangeEvents retrieved from a storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnSplitRange(event *storage.SplitRangeEvent) {
	nsm.GetStoreMonitor(event.StoreID).splitRange(event)
}

// OnMergeRange receives MergeRangeEvents retrieved from a storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnMergeRange(event *storage.MergeRangeEvent) {
	nsm.GetStoreMonitor(event.StoreID).mergeRange(event)
}

// OnStartStore receives StartStoreEvents retrieved from a storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnStartStore(event *storage.StartStoreEvent) {
	ssm := nsm.GetStoreMonitor(event.StoreID)
	ssm.Lock()
	defer ssm.Unlock()
	ssm.startedAt = event.StartedAt
}

// OnBeginScanRanges receives BeginScanRangesEvents retrieved from a storage
// event subscription. This method is part of the implementation of
// store.StoreEventListener.
// TODO(mrtracy): We have clearly moved away from the model of having multiple
// range-data listeners. This event should be removed from the feeds, as well as
// this monitor.
func (nsm *NodeStatusMonitor) OnBeginScanRanges(event *storage.BeginScanRangesEvent) {
	nsm.GetStoreMonitor(event.StoreID).beginScanRanges(event)
}

// OnEndScanRanges receives EndScanRangesEvents retrieved from a storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnEndScanRanges(event *storage.EndScanRangesEvent) {
	nsm.GetStoreMonitor(event.StoreID).endScanRanges(event)
}

// OnStoreStatus receives StoreStatusEvents retrieved from a storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnStoreStatus(event *storage.StoreStatusEvent) {
	ssm := nsm.GetStoreMonitor(event.Desc.StoreID)
	ssm.Lock()
	defer ssm.Unlock()
	ssm.desc = event.Desc
	// Update capacity gauges on the store monitor.
	ssm.capacity.Update(ssm.desc.Capacity.Capacity)
	ssm.available.Update(ssm.desc.Capacity.Available)
}

// OnReplicationStatus receives ReplicationStatusEvents retrieved from a storage
// event subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnReplicationStatus(event *storage.ReplicationStatusEvent) {
	ssm := nsm.GetStoreMonitor(event.StoreID)
	ssm.Lock()
	defer ssm.Unlock()
	ssm.leaderRangeCount.Update(event.LeaderRangeCount)
	ssm.replicatedRangeCount.Update(event.ReplicatedRangeCount)
	ssm.availableRangeCount.Update(event.AvailableRangeCount)
}

// OnStartNode receives StartNodeEvents from a node event subscription. This
// method is part of the implementation of NodeEventListener.
func (nsm *NodeStatusMonitor) OnStartNode(event *StartNodeEvent) {
	nsm.Lock()
	defer nsm.Unlock()
	nsm.startedAt = event.StartedAt
	nsm.desc = event.Desc
	// Outputs using format `<prefix>.<metric>.<id>`.
	nsm.metaRegistry.MustAdd(nodeTimeSeriesPrefix+"%s."+event.Desc.NodeID.String(),
		nsm.registry)
}

// OnCallSuccess receives CallSuccessEvents from a node event subscription. This
// method is part of the implementation of NodeEventListener.
func (nsm *NodeStatusMonitor) OnCallSuccess(event *CallSuccessEvent) {
	nsm.mSuccess.Add(1.0)
	nsm.mLatency.RecordValue(event.Duration.Nanoseconds())
}

// OnCallError receives CallErrorEvents from a node event subscription. This
// method is part of the implementation of NodeEventListener.
func (nsm *NodeStatusMonitor) OnCallError(event *CallErrorEvent) {
	nsm.mError.Add(1.0)
	nsm.mLatency.RecordValue(event.Duration.Nanoseconds())
}

// OnTrace receives Trace objects from a node event subscription. This method
// is part of the implementation of NodeEventListener.
func (nsm *NodeStatusMonitor) OnTrace(trace *tracer.Trace) {
	if log.V(2) {
		log.Infof("received trace:\n%s", trace)
	}
}

// StoreStatusMonitor monitors the status of a single store on the server.
// Status information is collected from event feeds provided by lower level
// components.
type StoreStatusMonitor struct {
	// Range data metrics.
	rangeCount           *metric.Counter
	leaderRangeCount     *metric.Gauge
	replicatedRangeCount *metric.Gauge
	availableRangeCount  *metric.Gauge

	// Storage metrics.
	liveBytes       *metric.Gauge
	keyBytes        *metric.Gauge
	valBytes        *metric.Gauge
	intentBytes     *metric.Gauge
	liveCount       *metric.Gauge
	keyCount        *metric.Gauge
	valCount        *metric.Gauge
	intentCount     *metric.Gauge
	intentAge       *metric.Gauge
	gcBytesAge      *metric.Gauge
	lastUpdateNanos *metric.Gauge
	capacity        *metric.Gauge
	available       *metric.Gauge

	sync.Mutex // Mutex to guard the following fields
	registry   *metric.Registry
	stats      engine.MVCCStats
	ID         roachpb.StoreID
	desc       *roachpb.StoreDescriptor
	startedAt  int64
}

// NewStoreStatusMonitor constructs a StoreStatusMonitor with the given ID.
func NewStoreStatusMonitor(id roachpb.StoreID, metaRegistry *metric.Registry) *StoreStatusMonitor {
	registry := metric.NewRegistry()
	// Format as `cr.store.<metric>.<id>` in output, in analogy to the time
	// series data written.
	metaRegistry.MustAdd(storeTimeSeriesPrefix+"%s."+id.String(), registry)
	return &StoreStatusMonitor{
		ID:                   id,
		registry:             registry,
		rangeCount:           registry.Counter("ranges"),
		leaderRangeCount:     registry.Gauge("ranges.leader"),
		replicatedRangeCount: registry.Gauge("ranges.replicated"),
		availableRangeCount:  registry.Gauge("ranges.available"),
		liveBytes:            registry.Gauge("livebytes"),
		keyBytes:             registry.Gauge("keybytes"),
		valBytes:             registry.Gauge("valbytes"),
		intentBytes:          registry.Gauge("intentbytes"),
		liveCount:            registry.Gauge("livecount"),
		keyCount:             registry.Gauge("keycount"),
		valCount:             registry.Gauge("valcount"),
		intentCount:          registry.Gauge("intentcount"),
		intentAge:            registry.Gauge("intentage"),
		gcBytesAge:           registry.Gauge("gcbytesage"),
		lastUpdateNanos:      registry.Gauge("lastupdatenanos"),
		capacity:             registry.Gauge("capacity"),
		available:            registry.Gauge("capacity.available"),
	}
}

func (ssm *StoreStatusMonitor) registerRange(event *storage.RegisterRangeEvent) {
	ssm.Lock()
	defer ssm.Unlock()
	ssm.stats.Add(&event.Stats)
	ssm.rangeCount.Inc(1)
	ssm.updateStorageGaugesLocked()
}

func (ssm *StoreStatusMonitor) updateRange(event *storage.UpdateRangeEvent) {
	ssm.Lock()
	defer ssm.Unlock()
	ssm.stats.Add(&event.Delta)
	ssm.updateStorageGaugesLocked()
}

func (ssm *StoreStatusMonitor) removeRange(event *storage.RemoveRangeEvent) {
	ssm.Lock()
	defer ssm.Unlock()
	ssm.stats.Subtract(&event.Stats)
	ssm.updateStorageGaugesLocked()
	ssm.rangeCount.Dec(1)
}

func (ssm *StoreStatusMonitor) splitRange(event *storage.SplitRangeEvent) {
	ssm.rangeCount.Inc(1)
}

func (ssm *StoreStatusMonitor) mergeRange(event *storage.MergeRangeEvent) {
	ssm.rangeCount.Dec(1)
}

func (ssm *StoreStatusMonitor) updateStorageGaugesLocked() {
	ssm.liveBytes.Update(ssm.stats.LiveBytes)
	ssm.keyBytes.Update(ssm.stats.KeyBytes)
	ssm.valBytes.Update(ssm.stats.ValBytes)
	ssm.intentBytes.Update(ssm.stats.IntentBytes)
	ssm.liveCount.Update(ssm.stats.LiveCount)
	ssm.keyCount.Update(ssm.stats.KeyCount)
	ssm.valCount.Update(ssm.stats.ValCount)
	ssm.intentCount.Update(ssm.stats.IntentCount)
	ssm.intentAge.Update(ssm.stats.IntentAge)
	ssm.gcBytesAge.Update(ssm.stats.GCBytesAge)
	ssm.lastUpdateNanos.Update(ssm.stats.LastUpdateNanos)
}

func (ssm *StoreStatusMonitor) beginScanRanges(event *storage.BeginScanRangesEvent) {
	// TODO(mrtracy): Remove these events completely.
}

func (ssm *StoreStatusMonitor) endScanRanges(event *storage.EndScanRangesEvent) {
	// TODO(mrtracy): Remove these events completely.
}
