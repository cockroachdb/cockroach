// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqlwatcher

import (
	"container/heap"

	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// eventBuffer is a helper struct for the SQLWatcher intended to keep track of
// and buffer events resulting from rangefeeds over system.zones and
// system.descriptors. All methods lock internally, so they can be called
// concurrently.
//
// The eventBuffer maintains the notion of a checkpointTS, computed as the
// minimum checkpoint timestamps for the zones and descriptors event channels.
// Internally, it uses a heap to return all events below this checkpointTS.
type eventBuffer struct {
	mu struct {
		syncutil.Mutex

		eventInfoHeap eventInfoHeap

		eventChannelCheckpointTSs [numEventChannels]hlc.Timestamp
	}
}

// eventChannel is used to represent the rangefeed channel from which the
// watcher's event originated from.
type eventChannel int

const (
	zonesEventChannel eventChannel = iota
	descriptorsEventChannel

	// numEventChannels should be listed last.
	numEventChannels
)

// newEventBuffer constructs and returns a new eventBuffer.
func newEventBuffer() *eventBuffer {
	return &eventBuffer{}
}

// recordRangefeedEvent records the given rangefeedEvent in the eventBuffer.
func (b *eventBuffer) recordRangefeedEvent(rangefeedEvent watcherRangefeedEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if rangefeedEvent.isCheckpoint {
		b.mu.eventChannelCheckpointTSs[rangefeedEvent.eventChannel].Forward(rangefeedEvent.timestamp)
		return
	}
	if rangefeedEvent.timestamp.Less(b.mu.eventChannelCheckpointTSs[rangefeedEvent.eventChannel]) {
		// If the rangefeedEvent is at a timestamp below the checkpoint then we
		// don't need to record it.
		return
	}
	eventInfo := &eventInfo{
		event: spanconfig.SQLWatcherEvent{
			ID:             rangefeedEvent.id,
			DescriptorType: rangefeedEvent.descriptorType,
		},
		timestamp: rangefeedEvent.timestamp,
	}
	heap.Push(&b.mu.eventInfoHeap, eventInfo)
}

// flushSQLWatcherEventsBelowCheckpoint returns a list of unique
// spanconfig.SQLWatcher events below the event buffer's checkpoint timestamp.
// The checkpoint timestamp is also returned.
func (b *eventBuffer) flushSQLWatcherEventsBelowCheckpoint() (
	events []spanconfig.SQLWatcherEvent,
	checkpointTS hlc.Timestamp,
) {
	b.mu.Lock()
	defer b.mu.Unlock()
	seenIDs := make(map[descpb.ID]struct{})
	// First we determine the checkpoint timestamp, which is the minimum
	// checkpoint timestamp of all event types.
	checkpointTS = hlc.MaxTimestamp
	for _, ts := range b.mu.eventChannelCheckpointTSs {
		checkpointTS.Backward(ts)
	}

	// Next we accumulate all IDs for which we have received events at timestamps
	// less than than or equal to the checkpoint timestamp.
	for {
		if len(b.mu.eventInfoHeap) == 0 || !b.mu.eventInfoHeap[0].timestamp.LessEq(checkpointTS) {
			break
		}

		eventInfo := heap.Pop(&b.mu.eventInfoHeap).(*eventInfo)
		// De-duplicate IDs from the returned result.
		if _, seen := seenIDs[eventInfo.event.ID]; !seen {
			seenIDs[eventInfo.event.ID] = struct{}{}
			events = append(events, eventInfo.event)
		}
	}

	return events, checkpointTS
}

// eventInfo is a SQLWatcherEvent <-> timestamp pair corresponding to an event
// received by the eventBuffer.
type eventInfo struct {
	event spanconfig.SQLWatcherEvent

	timestamp hlc.Timestamp
}

// eventInfoHeap is a min heap based on timestamp.
type eventInfoHeap []*eventInfo

var _ heap.Interface = (*eventInfoHeap)(nil)

// Len is part of heap.Interface.
func (ih *eventInfoHeap) Len() int {
	return len(*ih)
}

// Less is part of heap.Interface.
func (ih *eventInfoHeap) Less(i, j int) bool {
	return (*ih)[i].timestamp.Less((*ih)[j].timestamp)
}

// Swap is part of heap.Interface.
func (ih *eventInfoHeap) Swap(i, j int) {
	(*ih)[i], (*ih)[j] = (*ih)[j], (*ih)[i]
}

// Push is part of heap.Interface.
func (ih *eventInfoHeap) Push(x interface{}) {
	item := x.(*eventInfo)
	*ih = append(*ih, item)
}

// Pop is part of heap.Interface.
func (ih *eventInfoHeap) Pop() interface{} {
	old := *ih
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*ih = old[0 : n-1]
	return item
}
