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

// NewRangeEvent occurs when a new range is added to a store.  This event
// includes the Range's RangeDescriptor and current MVCCStats.
type NewRangeEvent struct {
	Desc  *proto.RangeDescriptor
	Stats proto.MVCCStats
}

// UpdateRangeEvent occurs whenever a Range is modified. This structure includes
// the same information as NewRangeEvent, but also includes a second set of
// MVCCStats containing the difference from the Range's previous stats. If the
// update did not modify any statistics, this diff may be nil.
type UpdateRangeEvent struct {
	Desc  *proto.RangeDescriptor
	Stats proto.MVCCStats
	Diff  proto.MVCCStats
}

// RemoveRangeEvent occurs whenever a Range is removed from a store. This
// structure includes the Range's RangeDescriptor and the Range's previous
// MVCCStats before it was removed.
type RemoveRangeEvent struct {
	Desc  *proto.RangeDescriptor
	Stats proto.MVCCStats
}

// SplitRangeEvent occurs whenever a range is split in two. This Event actually
// contains two other events: an UpdateRangeEvent for the Range which
// originally existed, and a NewRangeEvent for the range that was created via
// the split.
type SplitRangeEvent struct {
	Original UpdateRangeEvent
	New      NewRangeEvent
}

// MergeRangeEvent occurs whenever a range is merged into another. This Event
// contains two component events: an UpdateRangeEvent for the range which
// subsumed the other, and a RemoveRangeEvent for the range that was subsumed.
type MergeRangeEvent struct {
	Merged  UpdateRangeEvent
	Removed RemoveRangeEvent
}

// BeginScanRangesEvent occurs when the store is about to scan over all ranges.
// During such a scan, each existing range will be published to the feed as a
// NewRangeEvent. This is used because downstream consumers may be tracking
// statistics via the Diffs in UpdateRangeEvent; this event informs subscribers
// to clear currently cached values.
type BeginScanRangesEvent struct{}

// EndScanRangesEvent occurs when the store has finished scanning all ranges.
// Every BeginScanRangeEvent will eventually be followed by an
// EndScanRangeEvent.
type EndScanRangesEvent struct{}

// StoreEventFeed is a feed of events that occur on a Store. Most of these
// events are specific to a single range within the store.
type StoreEventFeed struct {
	f *util.Feed
}

// NewStoreEventFeed creates a new StoreEventFeed. Events can immediately be
// published and Subscribers can immediately subscribe to the feed.
func NewStoreEventFeed() StoreEventFeed {
	return StoreEventFeed{
		f: &util.Feed{},
	}
}

// Subscribe returns a util.Subscription which receives events from this
// StoreEventFeed.
//
// Events are received from the feed as an empty interface and must be cast to
// one of the event types in this package. This is best accomplished with a type
// switch:
//
// 		sub := storeFeed.Subscribe()
//		for event := range sub.Events {
//			switch event := event.(type) {
//			case NewRangeEvent:
//				// Process NewRangeEvent...
//		    case UpdateRangeEvent:
//				// Process UpdateRangeEvent...
//			// ... other interesting types
//			}
//		}
func (sef StoreEventFeed) Subscribe() *util.Subscription {
	return sef.f.Subscribe()
}

// closeFeed closes the underlying events feed.
func (sef StoreEventFeed) closeFeed() {
	sef.f.Close()
}

// newRange publishes a NewRangeEvent to this feed which describes the addition
// of the supplied Range.
func (sef StoreEventFeed) newRange(rng *Range) {
	sef.f.Publish(makeNewRangeEvent(rng))
}

// uewRange publishes an UpdateRangeEvent to this feed which describes a change
// to the supplied Range.
func (sef StoreEventFeed) updateRange(rng *Range, diff *proto.MVCCStats) {
	sef.f.Publish(makeUpdateRangeEvent(rng, diff))
}

// removeRange publishes a RemoveRangeEvent to this feed which describes the
// removal of the supplied Range.
func (sef StoreEventFeed) removeRange(rng *Range) {
	sef.f.Publish(makeRemoveRangeEvent(rng))
}

// splitRange publishes a SplitRangeEvent to this feed which describes a split
// involving the supplied Ranges.
func (sef StoreEventFeed) splitRange(rngOrig, rngNew *Range, diffOrig *proto.MVCCStats) {
	sef.f.Publish(makeSplitRangeEvent(rngOrig, rngNew, diffOrig))
}

// mergeRange publishes a MergeRangeEvent to this feed which describes a merger
// of the supplied Ranges.
func (sef StoreEventFeed) mergeRange(rngMerged, rngRemoved *Range, diffMerged *proto.MVCCStats) {
	sef.f.Publish(makeMergeRangeEvent(rngMerged, rngRemoved, diffMerged))
}

// beginScanRanges publishes a BeginScanRangesEvent to this feed.
func (sef StoreEventFeed) beginScanRanges() {
	sef.f.Publish(&BeginScanRangesEvent{})
}

// endScanRanges publishes an EndScanRangesEvent to this feed.
func (sef StoreEventFeed) endScanRanges() {
	sef.f.Publish(&EndScanRangesEvent{})
}

func makeNewRangeEvent(rng *Range) *NewRangeEvent {
	return &NewRangeEvent{
		Desc:  rng.Desc(),
		Stats: rng.stats.GetMVCC(),
	}
}

func makeUpdateRangeEvent(rng *Range, diff *proto.MVCCStats) *UpdateRangeEvent {
	return &UpdateRangeEvent{
		Desc:  rng.Desc(),
		Stats: rng.stats.GetMVCC(),
		Diff:  *diff,
	}
}

func makeRemoveRangeEvent(rng *Range) *RemoveRangeEvent {
	return &RemoveRangeEvent{
		Desc:  rng.Desc(),
		Stats: rng.stats.GetMVCC(),
	}
}

func makeSplitRangeEvent(rngOrig, rngNew *Range, diffOrig *proto.MVCCStats) *SplitRangeEvent {
	return &SplitRangeEvent{
		Original: UpdateRangeEvent{
			Desc:  rngOrig.Desc(),
			Stats: rngOrig.stats.GetMVCC(),
			Diff:  *diffOrig,
		},
		New: NewRangeEvent{
			Desc:  rngNew.Desc(),
			Stats: rngNew.stats.GetMVCC(),
		},
	}
}

func makeMergeRangeEvent(rngMerged, rngRemoved *Range, diffMerged *proto.MVCCStats) *MergeRangeEvent {
	return &MergeRangeEvent{
		Merged: UpdateRangeEvent{
			Desc:  rngMerged.Desc(),
			Stats: rngMerged.stats.GetMVCC(),
			Diff:  *diffMerged,
		},
		Removed: RemoveRangeEvent{
			Desc:  rngRemoved.Desc(),
			Stats: rngRemoved.stats.GetMVCC(),
		},
	}
}
