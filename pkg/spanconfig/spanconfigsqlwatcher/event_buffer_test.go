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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestEventBuffer sends forth descriptor/zones rangefeed events to the event
// buffer and ensures the checkpointing and ID flushing semantics work
// correctly.
func TestEventBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: int64(nanos),
		}
	}

	buffer := newEventBuffer()

	// Sanity check the newly initialized event buffer.
	events, checkpointTS := buffer.flushSQLWatcherEventsBelowCheckpoint()
	require.Equal(t, ts(0), checkpointTS)
	require.True(t, len(events) == 0)

	// Send through some zone and descriptor events that aren't checkpoints.
	// We don't expect anything to be returned by flushSQLWatcherEventsBelowCheckpoint as
	// nothing has been checkpointed.
	buffer.recordRangefeedEvent(watcherRangefeedEvent{
		id:             1,
		descriptorType: catalog.Any,
		eventChannel:   zonesEventChannel,
		timestamp:      ts(10),
		isCheckpoint:   false,
	})
	buffer.recordRangefeedEvent(watcherRangefeedEvent{
		id:             2,
		descriptorType: catalog.Any,
		eventChannel:   descriptorsEventChannel,
		timestamp:      ts(11),
		isCheckpoint:   false,
	})
	events, checkpointTS = buffer.flushSQLWatcherEventsBelowCheckpoint()
	require.Equal(t, ts(0), checkpointTS)
	require.True(t, len(events) == 0)

	// Send forth a zones checkpoint event. We expect flushSQLWatcherEventsBelowCheckpoint to
	// still not  return no results as we haven't sent a descriptors checkpoint
	// yet.
	buffer.recordRangefeedEvent(watcherRangefeedEvent{
		eventChannel: zonesEventChannel,
		timestamp:    ts(11),
		isCheckpoint: true,
	})
	events, checkpointTS = buffer.flushSQLWatcherEventsBelowCheckpoint()
	require.Equal(t, ts(0), checkpointTS)
	require.True(t, len(events) == 0)

	// Send forth a descriptors checkpoint event at a lower timestamp than zones
	// checkpoint timestamp above.
	// flushSQLWatcherEventsBelowCheckpoint should return the lower timestamp as the
	// checkpointTS. Furthermore, we only expect one id to be returned.
	buffer.recordRangefeedEvent(watcherRangefeedEvent{
		eventChannel: descriptorsEventChannel,
		timestamp:    ts(10),
		isCheckpoint: true,
	})
	events, checkpointTS = buffer.flushSQLWatcherEventsBelowCheckpoint()
	require.Equal(t, ts(10), checkpointTS)
	require.Equal(t, []spanconfig.SQLWatcherEvent{{ID: 1, DescriptorType: catalog.Any}}, events)

	// Bump the descriptors checkpoint past the zones checkpoint. This bumps the
	// checkpointTS of the eventBuffer to 11 and we end up returning the last ID
	// in the eventBuffer.
	buffer.recordRangefeedEvent(watcherRangefeedEvent{
		eventChannel: descriptorsEventChannel,
		timestamp:    ts(20),
		isCheckpoint: true,
	})
	events, checkpointTS = buffer.flushSQLWatcherEventsBelowCheckpoint()
	require.Equal(t, ts(11), checkpointTS)
	require.Equal(t, []spanconfig.SQLWatcherEvent{{ID: 2, DescriptorType: catalog.Any}}, events)

	// No events are left in the buffer below the checkpoint (which should be the
	// same as above).
	events, checkpointTS = buffer.flushSQLWatcherEventsBelowCheckpoint()
	require.Equal(t, ts(11), checkpointTS)
	require.True(t, len(events) == 0)

	// Send a checkpoint for zonesEventChannel at a timestamp below what has been
	// checkpointed (11). This should essentially no-op, which we can check by
	// asking for the event buffers checkpoint.
	buffer.recordRangefeedEvent(watcherRangefeedEvent{
		eventChannel: zonesEventChannel,
		timestamp:    ts(5),
		isCheckpoint: true,
	})
	events, checkpointTS = buffer.flushSQLWatcherEventsBelowCheckpoint()
	require.Equal(t, ts(11), checkpointTS)
	require.True(t, len(events) == 0)

	// Send a non-checkpoint event for zonesEventChannel at a timestamp below what
	// has been checkpointed (11). This should no-op as well, and the ID should
	// not be returned when flushing.
	buffer.recordRangefeedEvent(watcherRangefeedEvent{
		id:           1,
		eventChannel: zonesEventChannel,
		timestamp:    ts(5),
	})
	events, checkpointTS = buffer.flushSQLWatcherEventsBelowCheckpoint()
	require.Equal(t, ts(11), checkpointTS)
	require.True(t, len(events) == 0)

	// Lastly, ensure that flushing doesn't return duplicate IDs even when more
	// than one events have been received for a given ID.
	buffer.recordRangefeedEvent(watcherRangefeedEvent{
		id:             1,
		descriptorType: catalog.Any,
		eventChannel:   zonesEventChannel,
		timestamp:      ts(12),
	})
	buffer.recordRangefeedEvent(watcherRangefeedEvent{
		id:             1,
		descriptorType: catalog.Any,
		eventChannel:   zonesEventChannel,
		timestamp:      ts(13),
	})
	buffer.recordRangefeedEvent(watcherRangefeedEvent{
		eventChannel: zonesEventChannel,
		timestamp:    ts(14),
		isCheckpoint: true,
	})
	events, checkpointTS = buffer.flushSQLWatcherEventsBelowCheckpoint()
	require.Equal(t, ts(14), checkpointTS)
	require.Equal(t, []spanconfig.SQLWatcherEvent{{ID: 1, DescriptorType: catalog.Any}}, events)
}
