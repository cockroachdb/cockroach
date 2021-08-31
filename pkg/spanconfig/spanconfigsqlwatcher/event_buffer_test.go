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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestEventBuffer sends forth descriptor/zones events to the event buffer and
// ensures the checkpointing and ID flushing semantics work correctly.
func TestEventBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)
	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: int64(nanos),
		}
	}

	buffer := newEventBuffer()

	// Sanity check the newly initialized event buffer.
	ids, checkpointTS := buffer.flushIDsBelowCheckpoint()
	require.Equal(t, ts(0), checkpointTS)
	require.True(t, len(ids) == 0)

	// Send through some zone and descriptor events that aren't checkpoints.
	// We don't expect anything to be returned by flushIDsBelowCheckpoint as
	//// .
	buffer.recordEvent(watcherEvent{
		id:           1,
		eventType:    ZonesEventType,
		timestamp:    ts(10),
		isCheckpoint: false,
	})
	buffer.recordEvent(watcherEvent{
		id:           2,
		eventType:    DescriptorsEventType,
		timestamp:    ts(11),
		isCheckpoint: false,
	})
	ids, checkpointTS = buffer.flushIDsBelowCheckpoint()
	require.Equal(t, ts(0), checkpointTS)
	require.True(t, len(ids) == 0)

	// Send forth a zones checkpoint event. We expect flushIDsBelowCheckpoint to
	// still not  return no results as we haven't sent a descriptors checkpoint
	// yet.
	buffer.recordEvent(watcherEvent{
		eventType:    ZonesEventType,
		timestamp:    ts(11),
		isCheckpoint: true,
	})
	ids, checkpointTS = buffer.flushIDsBelowCheckpoint()
	require.Equal(t, ts(0), checkpointTS)
	require.True(t, len(ids) == 0)

	// Send forth a descriptors checkpoint event at a lower timestamp than zones
	// checkpoint timestamp above.
	// flushIDsBelowCheckpoint should return the lower timestamp as the
	// checkpointTS. Furthermore, we only expect one id to be returned.
	buffer.recordEvent(watcherEvent{
		eventType:    DescriptorsEventType,
		timestamp:    ts(10),
		isCheckpoint: true,
	})
	ids, checkpointTS = buffer.flushIDsBelowCheckpoint()
	require.Equal(t, ts(10), checkpointTS)
	require.Equal(t, descpb.IDs{1}, ids)

	// Bump the descriptors checkpoint past the zones checkpoint. This bumps the
	// checkpointTS of the eventBuffer to 11 and we end up returning the last ID
	// in the eventBuffer.
	buffer.recordEvent(watcherEvent{
		eventType:    DescriptorsEventType,
		timestamp:    ts(20),
		isCheckpoint: true,
	})
	ids, checkpointTS = buffer.flushIDsBelowCheckpoint()
	require.Equal(t, ts(11), checkpointTS)
	require.Equal(t, descpb.IDs{2}, ids)

	// No ids are left in the buffer below the checkpoint (which should be the
	// same as above).
	ids, checkpointTS = buffer.flushIDsBelowCheckpoint()
	require.Equal(t, ts(11), checkpointTS)
	require.True(t, len(ids) == 0)
}
