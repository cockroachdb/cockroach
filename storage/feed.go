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
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage

import (
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
)

// AddRangeEvent occurs when a new range is added to a store.  This event
// includes the Range's RangeDescriptor and current MVCCStats.
type AddRangeEvent struct {
	StoreID proto.StoreID
	Desc    *proto.RangeDescriptor
	Stats   proto.MVCCStats
}

// UpdateRangeEvent occurs whenever a Range is modified. This structure includes
// the same information as AddRangeEvent, but also includes a second set of
// MVCCStats containing the delta from the Range's previous stats. If the
// update did not modify any statistics, this delta may be nil.
type UpdateRangeEvent struct {
	StoreID proto.StoreID
	Desc    *proto.RangeDescriptor
	Stats   proto.MVCCStats
	Method  proto.Method
	Delta   proto.MVCCStats
}

// RemoveRangeEvent occurs whenever a Range is removed from a store. This
// structure includes the Range's RangeDescriptor and the Range's previous
// MVCCStats before it was removed.
type RemoveRangeEvent struct {
	StoreID proto.StoreID
	Desc    *proto.RangeDescriptor
	Stats   proto.MVCCStats
}

// SplitRangeEvent occurs whenever a range is split in two. This Event actually
// contains two other events: an UpdateRangeEvent for the Range which
// originally existed, and a AddRangeEvent for the range that was created via
// the split.
type SplitRangeEvent struct {
	StoreID  proto.StoreID
	Original UpdateRangeEvent
	New      AddRangeEvent
}

// MergeRangeEvent occurs whenever a range is merged into another. This Event
// contains two component events: an UpdateRangeEvent for the range which
// subsumed the other, and a RemoveRangeEvent for the range that was subsumed.
type MergeRangeEvent struct {
	StoreID proto.StoreID
	Merged  UpdateRangeEvent
	Removed RemoveRangeEvent
}

// StartStoreEvent occurs whenever a store is initially started.
type StartStoreEvent struct {
	StoreID proto.StoreID
}

// BeginScanRangesEvent occurs when the store is about to scan over all ranges.
// During such a scan, each existing range will be published to the feed as a
// AddRangeEvent. This is used because downstream consumers may be tracking
// statistics via the Deltas in UpdateRangeEvent; this event informs subscribers
// to clear currently cached values.
type BeginScanRangesEvent struct {
	StoreID proto.StoreID
}

// EndScanRangesEvent occurs when the store has finished scanning all ranges.
// Every BeginScanRangeEvent will eventually be followed by an
// EndScanRangeEvent.
type EndScanRangesEvent struct {
	StoreID proto.StoreID
}

// StoreEventFeed is a helper structure which publishes store-specific events to
// a util.Feed. The target feed may be shared by multiple StoreEventFeeds. If
// the target feed is nil, event methods become no-ops.
type StoreEventFeed struct {
	id proto.StoreID
	f  *util.Feed
}

// NewStoreEventFeed creates a new StoreEventFeed which publishes events for a
// specific store to the supplied feed.
func NewStoreEventFeed(id proto.StoreID, feed *util.Feed) StoreEventFeed {
	return StoreEventFeed{
		id: id,
		f:  feed,
	}
}

// addRange publishes a AddRangeEvent to this feed which describes the addition
// of the supplied Range.
func (sef StoreEventFeed) addRange(rng *Range) {
	if sef.f == nil {
		return
	}
	sef.f.Publish(makeAddRangeEvent(sef.id, rng))
}

// updateRange publishes an UpdateRangeEvent to this feed which describes a change
// to the supplied Range.
func (sef StoreEventFeed) updateRange(rng *Range, method proto.Method, delta *proto.MVCCStats) {
	if sef.f == nil {
		return
	}
	sef.f.Publish(makeUpdateRangeEvent(sef.id, rng, method, delta))
}

// removeRange publishes a RemoveRangeEvent to this feed which describes the
// removal of the supplied Range.
func (sef StoreEventFeed) removeRange(rng *Range) {
	if sef.f == nil {
		return
	}
	sef.f.Publish(makeRemoveRangeEvent(sef.id, rng))
}

// splitRange publishes a SplitRangeEvent to this feed which describes a split
// involving the supplied Ranges.
func (sef StoreEventFeed) splitRange(rngOrig, rngNew *Range) {
	if sef.f == nil {
		return
	}
	sef.f.Publish(makeSplitRangeEvent(sef.id, rngOrig, rngNew))
}

// mergeRange publishes a MergeRangeEvent to this feed which describes a merger
// of the supplied Ranges.
func (sef StoreEventFeed) mergeRange(rngMerged, rngRemoved *Range) {
	if sef.f == nil {
		return
	}
	sef.f.Publish(makeMergeRangeEvent(sef.id, rngMerged, rngRemoved))
}

// startStore publishes a StartStoreEvent to this feed.
func (sef StoreEventFeed) startStore() {
	if sef.f == nil {
		return
	}
	sef.f.Publish(&StartStoreEvent{sef.id})
}

// beginScanRanges publishes a BeginScanRangesEvent to this feed.
func (sef StoreEventFeed) beginScanRanges() {
	if sef.f == nil {
		return
	}
	sef.f.Publish(&BeginScanRangesEvent{sef.id})
}

// endScanRanges publishes an EndScanRangesEvent to this feed.
func (sef StoreEventFeed) endScanRanges() {
	if sef.f == nil {
		return
	}
	sef.f.Publish(&EndScanRangesEvent{sef.id})
}

// StoreEventListener is an interface that can be implemented by objects which
// listen for events published by stores.
type StoreEventListener interface {
	OnAddRange(event *AddRangeEvent)
	OnUpdateRange(event *UpdateRangeEvent)
	OnRemoveRange(event *RemoveRangeEvent)
	OnSplitRange(event *SplitRangeEvent)
	OnMergeRange(event *MergeRangeEvent)
	OnStartStore(event *StartStoreEvent)
	OnBeginScanRanges(event *BeginScanRangesEvent)
	OnEndScanRanges(event *EndScanRangesEvent)
}

// ProcessStoreEvents reads store events from the supplied channel and passes
// them to the correct methods of the supplied StoreEventListener. This method
// will run until the Subscription's events channel is closed.
func ProcessStoreEvents(l StoreEventListener, sub *util.Subscription) {
	for event := range sub.Events() {
		// TODO(tamird): https://github.com/barakmich/go-nyet/issues/7
		switch specificEvent := event.(type) {
		case *StartStoreEvent:
			l.OnStartStore(specificEvent)
		case *AddRangeEvent:
			l.OnAddRange(specificEvent)
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
		}
	}
}

func makeAddRangeEvent(id proto.StoreID, rng *Range) *AddRangeEvent {
	return &AddRangeEvent{
		StoreID: id,
		Desc:    rng.Desc(),
		Stats:   rng.stats.GetMVCC(),
	}
}

func makeUpdateRangeEvent(id proto.StoreID, rng *Range, method proto.Method, delta *proto.MVCCStats) *UpdateRangeEvent {
	return &UpdateRangeEvent{
		StoreID: id,
		Desc:    rng.Desc(),
		Stats:   rng.stats.GetMVCC(),
		Method:  method,
		Delta:   *delta,
	}
}

func makeRemoveRangeEvent(id proto.StoreID, rng *Range) *RemoveRangeEvent {
	return &RemoveRangeEvent{
		StoreID: id,
		Desc:    rng.Desc(),
		Stats:   rng.stats.GetMVCC(),
	}
}

func makeSplitRangeEvent(id proto.StoreID, rngOrig, rngNew *Range) *SplitRangeEvent {
	sre := &SplitRangeEvent{
		StoreID: id,
		Original: UpdateRangeEvent{
			Desc:  rngOrig.Desc(),
			Stats: rngOrig.stats.GetMVCC(),
		},
		New: AddRangeEvent{
			Desc:  rngNew.Desc(),
			Stats: rngNew.stats.GetMVCC(),
		},
	}
	// Size delta of original range is the additive inverse of stats for
	// the new range.
	sre.Original.Delta.Subtract(&sre.New.Stats)
	return sre
}

func makeMergeRangeEvent(id proto.StoreID, rngMerged, rngRemoved *Range) *MergeRangeEvent {
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
