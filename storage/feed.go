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
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage

import (
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

// RegisterRangeEvent occurs in two scenarios. Firstly, while a store
// broadcasts its list of ranges to initialize one or more new accumulators
// (with Scan set to true), or secondly, when a new range is initialized on
// the store (for example through replication), with Scan set to false. This
// event includes the Range's RangeDescriptor and current MVCCStats.
type RegisterRangeEvent struct {
	StoreID roachpb.StoreID
	Desc    *roachpb.RangeDescriptor
	Stats   engine.MVCCStats
	Scan    bool
}

// UpdateRangeEvent occurs whenever a Range is modified. This structure
// includes the basic range information, but also includes a second set of
// MVCCStats containing the delta from the Range's previous stats. If the
// update did not modify any statistics, this delta may be nil.
type UpdateRangeEvent struct {
	StoreID roachpb.StoreID
	Desc    *roachpb.RangeDescriptor
	Stats   engine.MVCCStats
	Method  roachpb.Method
	Delta   engine.MVCCStats
}

// RemoveRangeEvent occurs whenever a Range is removed from a store. This
// structure includes the Range's RangeDescriptor and the Range's previous
// MVCCStats before it was removed.
type RemoveRangeEvent struct {
	StoreID roachpb.StoreID
	Desc    *roachpb.RangeDescriptor
	Stats   engine.MVCCStats
}

// SplitRangeEvent occurs whenever a range is split in two. This Event actually
// contains two other events: an UpdateRangeEvent for the Range which
// originally existed, and a RegisterRangeEvent for the range created via
// the split.
type SplitRangeEvent struct {
	StoreID  roachpb.StoreID
	Original UpdateRangeEvent
	New      RegisterRangeEvent
}

// MergeRangeEvent occurs whenever a range is merged into another. This Event
// contains two component events: an UpdateRangeEvent for the range which
// subsumed the other, and a RemoveRangeEvent for the range that was subsumed.
type MergeRangeEvent struct {
	StoreID roachpb.StoreID
	Merged  UpdateRangeEvent
	Removed RemoveRangeEvent
}

// StartStoreEvent occurs whenever a store is initially started.
type StartStoreEvent struct {
	StoreID   roachpb.StoreID
	StartedAt int64
}

// StoreStatusEvent contains the current descriptor for the given store.
//
// Because the descriptor contains information that cannot currently be computed
// from other events, this event should be periodically broadcast by the store
// independently of other operations.
type StoreStatusEvent struct {
	Desc *roachpb.StoreDescriptor
}

// ReplicationStatusEvent contains statistics on the replication status of the
// ranges in the store.
//
// Because these statistics cannot currently be computed from other events, this
// event should be periodically broadcast by the store independently of other
// operations.
type ReplicationStatusEvent struct {
	StoreID roachpb.StoreID

	// Per-range availability information, which is currently computed by
	// periodically polling the ranges of each store.
	// TODO(mrtracy): See if this information could be computed incrementally
	// from other events.
	LeaderRangeCount     int64
	ReplicatedRangeCount int64
	AvailableRangeCount  int64
}

// BeginScanRangesEvent occurs when the store is about to scan over all ranges.
// During such a scan, each existing range will be published to the feed as a
// RegisterRangeEvent with the Scan flag set. This is used because downstream
// consumers may be tracking statistics via the Deltas in UpdateRangeEvent;
// this event informs subscribers to clear currently cached values.
type BeginScanRangesEvent struct {
	StoreID roachpb.StoreID
}

// EndScanRangesEvent occurs when the store has finished scanning all ranges.
// Every BeginScanRangeEvent will eventually be followed by an
// EndScanRangeEvent.
type EndScanRangesEvent struct {
	StoreID roachpb.StoreID
}

// StoreEventFeed is a helper structure which publishes store-specific events to
// a util.Feed. The target feed may be shared by multiple StoreEventFeeds. If
// the target feed is nil, event methods become no-ops.
type StoreEventFeed struct {
	id roachpb.StoreID
	f  *util.Feed
}

// NewStoreEventFeed creates a new StoreEventFeed which publishes events for a
// specific store to the supplied feed.
func NewStoreEventFeed(id roachpb.StoreID, feed *util.Feed) StoreEventFeed {
	return StoreEventFeed{
		id: id,
		f:  feed,
	}
}

// registerRange publishes a RegisterRangeEvent to this feed which describes a
// range on the store. See RegisterRangeEvent for details.
func (sef StoreEventFeed) registerRange(rng *Replica, scan bool) {
	sef.f.Publish(makeRegisterRangeEvent(sef.id, rng, scan))
}

// updateRange publishes an UpdateRangeEvent to this feed which describes a change
// to the supplied Range.
func (sef StoreEventFeed) updateRange(rng *Replica, method roachpb.Method, delta *engine.MVCCStats) {
	sef.f.Publish(makeUpdateRangeEvent(sef.id, rng, method, delta))
}

// removeRange publishes a RemoveRangeEvent to this feed which describes the
// removal of the supplied Range.
func (sef StoreEventFeed) removeRange(rng *Replica) {
	sef.f.Publish(makeRemoveRangeEvent(sef.id, rng))
}

// splitRange publishes a SplitRangeEvent to this feed which describes a split
// involving the supplied Ranges.
func (sef StoreEventFeed) splitRange(rngOrig, rngNew *Replica) {
	sef.f.Publish(makeSplitRangeEvent(sef.id, rngOrig, rngNew))
}

// mergeRange publishes a MergeRangeEvent to this feed which describes a merger
// of the supplied Ranges.
func (sef StoreEventFeed) mergeRange(rngMerged, rngRemoved *Replica) {
	sef.f.Publish(makeMergeRangeEvent(sef.id, rngMerged, rngRemoved))
}

// startStore publishes a StartStoreEvent to this feed.
func (sef StoreEventFeed) startStore(startedAt int64) {
	sef.f.Publish(&StartStoreEvent{
		StoreID:   sef.id,
		StartedAt: startedAt,
	})
}

// storeStatus publishes a StoreStatusEvent to this feed.
func (sef StoreEventFeed) storeStatus(desc *roachpb.StoreDescriptor) {
	sef.f.Publish(&StoreStatusEvent{
		Desc: desc,
	})
}

// replicationStatus publishes a ReplicationStatusEvent to this feed.
func (sef StoreEventFeed) replicationStatus(leaders, replicated, available int64) {
	sef.f.Publish(&ReplicationStatusEvent{
		StoreID:              sef.id,
		LeaderRangeCount:     leaders,
		ReplicatedRangeCount: replicated,
		AvailableRangeCount:  available,
	})
}

// beginScanRanges publishes a BeginScanRangesEvent to this feed.
func (sef StoreEventFeed) beginScanRanges() {
	sef.f.Publish(&BeginScanRangesEvent{sef.id})
}

// endScanRanges publishes an EndScanRangesEvent to this feed.
func (sef StoreEventFeed) endScanRanges() {
	sef.f.Publish(&EndScanRangesEvent{sef.id})
}

// StoreEventListener is an interface that can be implemented by objects which
// listen for events published by stores.
type StoreEventListener interface {
	OnRegisterRange(event *RegisterRangeEvent)
	OnUpdateRange(event *UpdateRangeEvent)
	OnRemoveRange(event *RemoveRangeEvent)
	OnSplitRange(event *SplitRangeEvent)
	OnMergeRange(event *MergeRangeEvent)
	OnStartStore(event *StartStoreEvent)
	OnBeginScanRanges(event *BeginScanRangesEvent)
	OnEndScanRanges(event *EndScanRangesEvent)
	OnStoreStatus(event *StoreStatusEvent)
	OnReplicationStatus(event *ReplicationStatusEvent)
}

// ProcessStoreEvent dispatches an event on the StoreEventListener.
func ProcessStoreEvent(l StoreEventListener, event interface{}) {
	switch specificEvent := event.(type) {
	case *StartStoreEvent:
		l.OnStartStore(specificEvent)
	case *RegisterRangeEvent:
		l.OnRegisterRange(specificEvent)
	case *UpdateRangeEvent:
		l.OnUpdateRange(specificEvent)
	case *RemoveRangeEvent:
		l.OnRemoveRange(specificEvent)
	case *SplitRangeEvent:
		l.OnSplitRange(specificEvent)
	case *MergeRangeEvent:
		l.OnMergeRange(specificEvent)
	case *BeginScanRangesEvent:
		l.OnBeginScanRanges(specificEvent)
	case *EndScanRangesEvent:
		l.OnEndScanRanges(specificEvent)
	case *StoreStatusEvent:
		l.OnStoreStatus(specificEvent)
	case *ReplicationStatusEvent:
		l.OnReplicationStatus(specificEvent)
	}
}

func makeRegisterRangeEvent(id roachpb.StoreID, rng *Replica, scan bool) *RegisterRangeEvent {
	return &RegisterRangeEvent{
		StoreID: id,
		Desc:    rng.Desc(),
		Stats:   rng.stats.GetMVCC(),
		Scan:    scan,
	}
}

func makeUpdateRangeEvent(id roachpb.StoreID, rng *Replica, method roachpb.Method, delta *engine.MVCCStats) *UpdateRangeEvent {
	return &UpdateRangeEvent{
		StoreID: id,
		Desc:    rng.Desc(),
		Stats:   rng.stats.GetMVCC(),
		Method:  method,
		Delta:   *delta,
	}
}

func makeRemoveRangeEvent(id roachpb.StoreID, rng *Replica) *RemoveRangeEvent {
	return &RemoveRangeEvent{
		StoreID: id,
		Desc:    rng.Desc(),
		Stats:   rng.stats.GetMVCC(),
	}
}

func makeSplitRangeEvent(id roachpb.StoreID, rngOrig, rngNew *Replica) *SplitRangeEvent {
	sre := &SplitRangeEvent{
		StoreID: id,
		Original: UpdateRangeEvent{
			Desc:  rngOrig.Desc(),
			Stats: rngOrig.stats.GetMVCC(),
		},
		New: RegisterRangeEvent{
			Desc:  rngNew.Desc(),
			Stats: rngNew.stats.GetMVCC(),
		},
	}
	// Size delta of original range is the additive inverse of stats for
	// the new range.
	sre.Original.Delta.Subtract(sre.New.Stats)
	return sre
}

func makeMergeRangeEvent(id roachpb.StoreID, rngMerged, rngRemoved *Replica) *MergeRangeEvent {
	mre := &MergeRangeEvent{
		StoreID: id,
		Merged: UpdateRangeEvent{
			Desc:  rngMerged.Desc(),
			Stats: rngMerged.stats.GetMVCC(),
		},
		Removed: RemoveRangeEvent{
			Desc:  rngRemoved.Desc(),
			Stats: rngRemoved.stats.GetMVCC(),
		},
	}
	mre.Merged.Delta = mre.Removed.Stats
	return mre
}
