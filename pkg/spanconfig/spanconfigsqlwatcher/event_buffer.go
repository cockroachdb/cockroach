// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqlwatcher

import (
	"container/heap"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// eventBuffer is a helper struct for the SQLWatcher intended to keep track of
// and buffer events resulting from rangefeeds over system.zones and
// system.descriptors. It is not intended to be accessed concurrently.
//
// The eventBuffer maintains the notion of a checkpointTS. This is computed as
// the minimum timestamp for {zones,descriptor} checkpoint events. Internally,
// it uses a heap to return all events below this checkpointTS.
type eventBuffer struct {
	idHeap idEventInfoHeap

	eventCheckpointTSs []hlc.Timestamp
}

// eventType represents a watcherEvent type.
type eventType int

const (
	ZonesEventType eventType = iota
	DescriptorsEventType

	// numEvents should be listed last.
	numEvents
)

// newEventBuffer constructs and returns a new eventBuffer.
func newEventBuffer() *eventBuffer {
	return &eventBuffer{
		eventCheckpointTSs: make([]hlc.Timestamp, numEvents),
	}
}

// recordEvent records the given event in the eventBuffer.
func (b *eventBuffer) recordEvent(event watcherEvent) {
	if event.isCheckpoint {
		b.eventCheckpointTSs[event.eventType] = event.timestamp
		return
	}
	idInfo := &idEventInfo{
		id:        event.id,
		timestamp: event.timestamp,
	}
	heap.Push(&b.idHeap, idInfo)
}

// flushIDsBelowCheckpoint returns the IDs in the buffer that are at a timestamp
// below the eventBuffer's checkpoint timestamp. The checkpoint timestamp is
// also returned.
func (b *eventBuffer) flushIDsBelowCheckpoint() (ids descpb.IDs, checkpointTS hlc.Timestamp) {
	// First we determine the checkpoint timestamp, which is the
	checkpointTS = b.eventCheckpointTSs[0]
	for _, ts := range b.eventCheckpointTSs {
		if ts.Less(checkpointTS) {
			checkpointTS = ts
		}
	}

	// Next we accumulate all IDs for which we have received events at timestamps
	// less than than or equal to the checkpoint timestamp.
	for {
		if len(b.idHeap) == 0 || !b.idHeap[0].timestamp.LessEq(checkpointTS) {
			break
		}

		idInfo := heap.Pop(&b.idHeap).(*idEventInfo)
		ids = append(ids, idInfo.id)
	}

	return ids, checkpointTS
}

// idEventInfo is an id <-> timestamp pair corresponding to an event received by
// the eventBuffer.
type idEventInfo struct {
	id descpb.ID

	timestamp hlc.Timestamp
}

// idEventInfoHeap is a min heap based keyed on timestamp.
type idEventInfoHeap []*idEventInfo

var _ heap.Interface = (*idEventInfoHeap)(nil)

// Len is part of heap.Interface.
func (ih *idEventInfoHeap) Len() int {
	return len(*ih)
}

// Less is part of heap.Interface.
func (ih *idEventInfoHeap) Less(i, j int) bool {
	return (*ih)[i].timestamp.Less((*ih)[j].timestamp)
}

// Swap is part of heap.Interface.
func (ih *idEventInfoHeap) Swap(i, j int) {
	(*ih)[i], (*ih)[j] = (*ih)[j], (*ih)[i]
}

// Push is part of heap.Interface.
func (ih *idEventInfoHeap) Push(x interface{}) {
	item := x.(*idEventInfo)
	*ih = append(*ih, item)
}

// Pop is part of heap.Interface.
func (ih *idEventInfoHeap) Pop() interface{} {
	old := *ih
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*ih = old[0 : n-1]
	return item
}
