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
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
)

// StoreStatusMonitor monitors the status of a single store on the server.
// Status information is collected from event feeds provided by lower level
// components.
type StoreStatusMonitor struct {
	rangeDataAccumulator
}

// NodeStatusMonitor monitors the status of a server node. Status information
// is collected from event feeds provided by lower level components.
//
// This structure contains collections of other StatusMonitor types which monitor
// interesting subsets of data on the node. NodeStatusMonitor is responsible
// for passing event feed data to these subset structures for accumulation.
type NodeStatusMonitor struct {
	stores map[proto.StoreID]*StoreStatusMonitor
}

// NewNodeStatusMonitor initializes a new NodeStatusMonitor instance.
func NewNodeStatusMonitor() *NodeStatusMonitor {
	return &NodeStatusMonitor{
		stores: make(map[proto.StoreID]*StoreStatusMonitor),
	}
}

// getStore is a helper method which retrieves the StoreStatusMonitor for the
// given StoreID, creating it if it does not already exist.
func (nsm *NodeStatusMonitor) getStore(id proto.StoreID) *StoreStatusMonitor {
	if s, ok := nsm.stores[id]; ok {
		return s
	}
	s := &StoreStatusMonitor{}
	nsm.stores[id] = s
	return s
}

// StartMonitorFeed begins processing events published to the supplied
// Subscription. This method will continue running until the Subscription's
// events feed is closed.
func (nsm *NodeStatusMonitor) StartMonitorFeed(sub *util.Subscription) {
	storage.ProcessStoreEvents(nsm, sub)
}

// OnAddRange receives AddRangeEvents retrieved from an storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnAddRange(event *storage.AddRangeEvent) {
	nsm.getStore(event.StoreID).addRange(event)
}

// OnUpdateRange receives UpdateRangeEvents retrieved from an storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnUpdateRange(event *storage.UpdateRangeEvent) {
	nsm.getStore(event.StoreID).updateRange(event)
}

// OnRemoveRange receives RemoveRangeEvents retrieved from an storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnRemoveRange(event *storage.RemoveRangeEvent) {
	nsm.getStore(event.StoreID).removeRange(event)
}

// OnSplitRange receives SplitRangeEvents retrieved from an storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnSplitRange(event *storage.SplitRangeEvent) {
	nsm.getStore(event.StoreID).splitRange(event)
}

// OnMergeRange receives MergeRangeEvents retrieved from an storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnMergeRange(event *storage.MergeRangeEvent) {
	nsm.getStore(event.StoreID).mergeRange(event)
}

// OnStartStore receives StartStoreEvents retrieved from an storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnStartStore(event *storage.StartStoreEvent) {
	nsm.getStore(event.StoreID)
}

// OnBeginScanRanges receives BeginScanRangesEvents retrieved from an storage
// event subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnBeginScanRanges(event *storage.BeginScanRangesEvent) {
	nsm.getStore(event.StoreID).beginScanRanges(event)
}

// OnEndScanRanges receives EndScanRangesEvents retrieved from an storage event
// subscription. This method is part of the implementation of
// store.StoreEventListener.
func (nsm *NodeStatusMonitor) OnEndScanRanges(event *storage.EndScanRangesEvent) {
	nsm.getStore(event.StoreID).endScanRanges(event)
}

// rangeDataAccumulator maintains a set of accumulated stats for a set of
// ranges, computed from an incoming stream of storage events. Stats will be
// changed by any events sent to this type; higher level components are
// responsible for selecting the specific ranges accumulated by a
// rangeDataAccumulator instance.
type rangeDataAccumulator struct {
	stats      proto.MVCCStats
	rangeCount int64
	// 'scanning' is a special mode used to initialize a rangeDataAccumulator.
	// During typical operation stats are monitored using per-operation deltas;
	// however, when a rangeDataAccumulator is initialized it must first read
	// the total value of all stats at the time when it is created.
	//
	// The scanning mode is used to facilitate this: the underlying store will
	// initiate a scan with "beginScanRanges", and then send an AddRangeEvent
	// for each range in the store.
	//
	// During a scan it is not possible for ranges to be added, removed, split
	// or merged; however, it is possible for UpdateRangeEvents to occur during
	// a scan. The seenScan collection is used to properly handle
	// UpdateRangeEvents in this case.
	isScanning bool
	seenScan   map[int64]struct{}
}

func (rda *rangeDataAccumulator) addRange(event *storage.AddRangeEvent) {
	if rda.isScanning {
		rda.seenScan[event.Desc.RaftID] = struct{}{}
		rda.rangeCount++
		rda.stats.Accumulate(&event.Stats)
	}
}

func (rda *rangeDataAccumulator) updateRange(event *storage.UpdateRangeEvent) {
	if rda.isScanning {
		// Skip if we are in an active scan and have not yet accumulated the
		// data for this range.
		if _, seen := rda.seenScan[event.Desc.RaftID]; !seen {
			return
		}
	}
	rda.stats.Accumulate(&event.Delta)
}

func (rda *rangeDataAccumulator) removeRange(event *storage.RemoveRangeEvent) {
	rda.stats.Subtract(&event.Stats)
	rda.rangeCount--
}

func (rda *rangeDataAccumulator) splitRange(event *storage.SplitRangeEvent) {
	rda.rangeCount++
}

func (rda *rangeDataAccumulator) mergeRange(event *storage.MergeRangeEvent) {
	rda.rangeCount--
}

func (rda *rangeDataAccumulator) beginScanRanges(event *storage.BeginScanRangesEvent) {
	rda.isScanning = true
	rda.stats = proto.MVCCStats{}
	rda.rangeCount = 0
	rda.seenScan = make(map[int64]struct{})
}

func (rda *rangeDataAccumulator) endScanRanges(event *storage.EndScanRangesEvent) {
	rda.isScanning = false
	rda.seenScan = nil
}
