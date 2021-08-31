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
	"context"
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

	ctx := context.Background()
	buffer := newBuffer()

	// Sanity check the newly initialized event buffer.
	events, combinedFrontierTS := buffer.flush(ctx)
	require.Equal(t, ts(0), combinedFrontierTS)
	require.True(t, len(events) == 0)

	// Add a few events without advancing any of the frontiers. We don't expect
	// anything to be returned by the call to flush yet.
	err := buffer.add(event{
		update: spanconfig.SQLWatcherUpdate{
			ID:             1,
			DescriptorType: catalog.Any,
		},
		timestamp: ts(10),
	})
	require.NoError(t, err)

	err = buffer.add(event{
		update: spanconfig.SQLWatcherUpdate{
			ID:             2,
			DescriptorType: catalog.Any,
		},
		timestamp: ts(11),
	})
	require.NoError(t, err)
	events, combinedFrontierTS = buffer.flush(ctx)
	require.Equal(t, ts(0), combinedFrontierTS)
	require.True(t, len(events) == 0)

	// Advance the zones frontier. We expect flush to still not return any results
	// as the descriptors frontier hasn't been advanced yet.
	buffer.advance(zonesRangefeed, ts(11))
	events, combinedFrontierTS = buffer.flush(ctx)
	require.Equal(t, ts(0), combinedFrontierTS)
	require.True(t, len(events) == 0)

	// Advance the descriptors frontier to a lower timestamp than the zones
	// frontier above. Flush should now return the lower timestamp as the
	// combinedFrontierTS. Furthermore, we only expect one id to be returned.
	buffer.advance(descriptorsRangefeed, ts(10))
	require.NoError(t, err)
	events, combinedFrontierTS = buffer.flush(ctx)
	require.Equal(t, ts(10), combinedFrontierTS)
	require.Equal(t, []spanconfig.SQLWatcherUpdate{{ID: 1, DescriptorType: catalog.Any}}, events)

	// Bump the descriptors frontier past the zones frontier. This should bump the
	// combinedFrontierTS of the buffer to 11, resulting in flush returning the
	// last ID in the buffer.
	buffer.advance(descriptorsRangefeed, ts(20))
	events, combinedFrontierTS = buffer.flush(ctx)
	require.Equal(t, ts(11), combinedFrontierTS)
	require.Equal(t, []spanconfig.SQLWatcherUpdate{{ID: 2, DescriptorType: catalog.Any}}, events)

	// No events are left in the buffer below the combined frontier (which hasn't
	// changed from above).
	events, combinedFrontierTS = buffer.flush(ctx)
	require.Equal(t, ts(11), combinedFrontierTS)
	require.True(t, len(events) == 0)

	// Try regressing the zones frontier by advancing it to a timestamp below what
	// it was previously advanced to (11). This should essentially no-op, which we
	// can check by looking at the combinedFrontierTS from the call to flush.
	buffer.advance(zonesRangefeed, ts(5))
	events, combinedFrontierTS = buffer.flush(ctx)
	require.Equal(t, ts(11), combinedFrontierTS)
	require.True(t, len(events) == 0)

	// Try adding an event at a TS below the combinedFrontierTS. This should no-op
	// as well and the ID should not be returned when flushing.
	err = buffer.add(event{
		update: spanconfig.SQLWatcherUpdate{
			ID:             1,
			DescriptorType: catalog.Any,
		},
		timestamp: ts(5),
	})
	require.NoError(t, err)
	events, combinedFrontierTS = buffer.flush(ctx)
	require.Equal(t, ts(11), combinedFrontierTS)
	require.True(t, len(events) == 0)

	// Lastly, ensure that flushing doesn't return duplicate IDs even when more
	// than one events have been added for a given ID.
	err = buffer.add(event{
		update: spanconfig.SQLWatcherUpdate{
			ID:             1,
			DescriptorType: catalog.Any,
		},
		timestamp: ts(12),
	})
	require.NoError(t, err)
	err = buffer.add(event{
		update: spanconfig.SQLWatcherUpdate{
			ID:             1,
			DescriptorType: catalog.Any,
		},
		timestamp: ts(13),
	})
	require.NoError(t, err)
	buffer.advance(zonesRangefeed, ts(14))
	events, combinedFrontierTS = buffer.flush(ctx)
	require.Equal(t, ts(14), combinedFrontierTS)
	require.Equal(t, []spanconfig.SQLWatcherUpdate{{ID: 1, DescriptorType: catalog.Any}}, events)
}
