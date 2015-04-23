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
	Desc     *proto.RangeDescriptor
	IsLeader bool
	Stats    proto.MVCCStats
}

// UpdateRangeEvent occurs whenever a Range is modified. This structure includes
// the same information as NewRangeEvent, but also includes a second set of
// MVCCStats containing the difference from the Range's previous stats. If the
// update did not modify any statistics, this diff may be nil.
type UpdateRangeEvent struct {
	Desc     *proto.RangeDescriptor
	IsLeader bool
	Stats    proto.MVCCStats
	Diff     proto.MVCCStats
}

// RemoveRangeEvent occurs whenever a Range is removed from a store. This
// structure includes the Range's RangeDescriptor and the Range's previous
// MVCCStats before it was removed.
type RemoveRangeEvent struct {
	Desc     *proto.RangeDescriptor
	IsLeader bool
	Stats    proto.MVCCStats
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
// contains two component event: an UpdateRangeEvent for the range which
// absorbed the other, and a RemoveRangeEvent for the range that was absorbed.
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
	baseStoreEventPublisher
	f *util.Feed
}

// NewStoreEventFeed creates a new StoreEventFeed. Events can immediately be
// published and Subscribers can immediately subscribe to the feed.
func NewStoreEventFeed(stopper *util.Stopper) StoreEventFeed {
	feed := util.StartFeed(stopper)
	return StoreEventFeed{
		f: feed,
		baseStoreEventPublisher: baseStoreEventPublisher{
			publish: feed.Publish,
		},
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
func (sef *StoreEventFeed) Subscribe() *util.Subscription {
	return sef.f.Subscribe()
}

// newBatch returns a new storeEventBatch which accumulates potential store
// events as part of a batch.
func (sef *StoreEventFeed) newBatch() *storeEventBatch {
	seb := &storeEventBatch{
		f:                 sef.f,
		accumulatedEvents: make([]interface{}, 0, 3),
	}
	seb.baseStoreEventPublisher.publish = seb.accumulate
	return seb
}

// storeEventBatch is used to accumulate potential Store events without
// immediately publishing them to a feed.  This is intended for use in
// operations that produce events but may be rolled back by an error.
// Accumulated events will not be published to the actual feed until the
// commit() method is called.
type storeEventBatch struct {
	baseStoreEventPublisher
	f                 *util.Feed
	accumulatedEvents []interface{}
}

// accumulate accepts a single event.
func (seb *storeEventBatch) accumulate(event interface{}) {
	seb.accumulatedEvents = append(seb.accumulatedEvents, event)
}

// commit publishes any accumulated events to the associated feed.
func (seb *storeEventBatch) commit() {
	for _, e := range seb.accumulatedEvents {
		seb.f.Publish(e)
	}
	seb.accumulatedEvents = nil
}

// storeEventPublisher provides a set of methods for publishing specific Store
// events to a feed. It is implemented by both StoreEventFeed and
// storeEventBatch.
type storeEventPublisher interface {
	newRange(rng *Range)
	updateRange(rng *Range, diff *proto.MVCCStats)
	removeRange(rng *Range)
	splitRange(rngOrig, rngNew *Range, diffOrig *proto.MVCCStats)
	mergeRange(rngMerged, rngRemoved *Range, diffMerged *proto.MVCCStats)
	beginScanRanges()
	endScanRanges()
}

// baseStoreEventPublisher is a helper structure which implements the methods of
// storeEventPublisher. Upon creation, these events are passed to the
// baseStoreEventPublisher's 'publish' method. This structure is intended to be
// embedded inside of another structure.
type baseStoreEventPublisher struct {
	publish func(event interface{})
}

// newRange publishes a NewRangeEvent to this feed which describes the addition
// of the supplied Range.
func (sep baseStoreEventPublisher) newRange(rng *Range) {
	sep.publish(makeNewRangeEvent(rng))
}

// uewRange publishes an UpdateRangeEvent to this feed which describes a change
// to the supplied Range.
func (sep baseStoreEventPublisher) updateRange(rng *Range, diff *proto.MVCCStats) {
	sep.publish(makeUpdateRangeEvent(rng, diff))
}

// removeRange publishes a RemoveRangeEvent to this feed which describes the
// removal of the supplied Range.
func (sep baseStoreEventPublisher) removeRange(rng *Range) {
	sep.publish(makeRemoveRangeEvent(rng))
}

// splitRange publishes a SplitRangeEvent to this feed which describes a split
// involving the supplied Ranges.
func (sep baseStoreEventPublisher) splitRange(rngOrig, rngNew *Range, diffOrig *proto.MVCCStats) {
	sep.publish(makeSplitRangeEvent(rngOrig, rngNew, diffOrig))
}

// mergeRange publishes a MergeRangeEvent to this feed which describes a merger
// of the supplied Ranges.
func (sep baseStoreEventPublisher) mergeRange(rngMerged, rngRemoved *Range, diffMerged *proto.MVCCStats) {
	sep.publish(makeMergeRangeEvent(rngMerged, rngRemoved, diffMerged))
}

// beginScanRanges publishes a BeginScanRangesEvent to this feed.
func (sep baseStoreEventPublisher) beginScanRanges() {
	sep.publish(&BeginScanRangesEvent{})
}

// endScanRanges publishes an EndScanRangesEvent to this feed.
func (sep baseStoreEventPublisher) endScanRanges() {
	sep.publish(&EndScanRangesEvent{})
}

func makeNewRangeEvent(rng *Range) *NewRangeEvent {
	return &NewRangeEvent{
		Desc:     rng.Desc(),
		IsLeader: rng.IsLeader(),
		Stats:    rng.stats.GetMVCC(),
	}
}

func makeUpdateRangeEvent(rng *Range, diff *proto.MVCCStats) *UpdateRangeEvent {
	return &UpdateRangeEvent{
		Desc:     rng.Desc(),
		IsLeader: rng.IsLeader(),
		Stats:    rng.stats.GetMVCC(),
		Diff:     *diff,
	}
}

func makeRemoveRangeEvent(rng *Range) *RemoveRangeEvent {
	return &RemoveRangeEvent{
		Desc:     rng.Desc(),
		IsLeader: rng.IsLeader(),
		Stats:    rng.stats.GetMVCC(),
	}
}

func makeSplitRangeEvent(rngOrig, rngNew *Range, diffOrig *proto.MVCCStats) *SplitRangeEvent {
	return &SplitRangeEvent{
		Original: UpdateRangeEvent{
			Desc:     rngOrig.Desc(),
			IsLeader: rngOrig.IsLeader(),
			Stats:    rngOrig.stats.GetMVCC(),
			Diff:     *diffOrig,
		},
		New: NewRangeEvent{
			Desc:     rngNew.Desc(),
			IsLeader: rngNew.IsLeader(),
			Stats:    rngNew.stats.GetMVCC(),
		},
	}
}

func makeMergeRangeEvent(rngMerged, rngRemoved *Range, diffMerged *proto.MVCCStats) *MergeRangeEvent {
	return &MergeRangeEvent{
		Merged: UpdateRangeEvent{
			Desc:     rngMerged.Desc(),
			IsLeader: rngMerged.IsLeader(),
			Stats:    rngMerged.stats.GetMVCC(),
			Diff:     *diffMerged,
		},
		Removed: RemoveRangeEvent{
			Desc:     rngRemoved.Desc(),
			IsLeader: rngRemoved.IsLeader(),
			Stats:    rngRemoved.stats.GetMVCC(),
		},
	}
}
