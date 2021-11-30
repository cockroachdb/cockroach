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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestBuffer adds DescriptorUpdate events to the buffer and ensures that
// checkpointing and ID flushing semantics work correctly.
func TestBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: int64(nanos),
		}
	}
	makeEvent := func(descID int, timestamp hlc.Timestamp) event {
		return event{
			update: spanconfig.DescriptorUpdate{
				ID:             descpb.ID(descID),
				DescriptorType: catalog.Any,
			},
			timestamp: timestamp,
		}
	}
	makeUpdates := func(descIDs ...int) []spanconfig.DescriptorUpdate {
		updates := make([]spanconfig.DescriptorUpdate, 0, len(descIDs))
		for _, descID := range descIDs {
			updates = append(updates, spanconfig.DescriptorUpdate{
				ID:             descpb.ID(descID),
				DescriptorType: catalog.Any,
			})
		}
		return updates
	}

	ctx := context.Background()
	buffer := newBuffer(10 /* limit */, ts(1))

	// Sanity check the newly initialized event buffer.
	updates, combinedFrontierTS, err := buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(1), combinedFrontierTS)
	require.True(t, len(updates) == 0)

	// Add a few events without advancing any of the frontiers. We don't expect
	// anything to be returned by the call to flush yet.
	err = buffer.add(makeEvent(1, ts(10)))
	require.NoError(t, err)

	err = buffer.add(makeEvent(2, ts(11)))
	require.NoError(t, err)
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(1), combinedFrontierTS)
	require.True(t, len(updates) == 0)

	// Advance the zones frontier. We expect flush to still not return any results
	// as the descriptors frontier hasn't been advanced yet.
	buffer.advance(zonesRangefeed, ts(11))
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(1), combinedFrontierTS)
	require.True(t, len(updates) == 0)

	// Advance the descriptors frontier to a lower timestamp than the zones
	// frontier above. Flush should now return the lower timestamp as the
	// combinedFrontierTS. Furthermore, we only expect one id to be returned.
	buffer.advance(descriptorsRangefeed, ts(10))
	require.NoError(t, err)
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(10), combinedFrontierTS)
	require.Equal(t, makeUpdates(1), updates)

	// Bump the descriptors frontier past the zones frontier. This should bump the
	// combinedFrontierTS of the buffer to 11, resulting in flush returning the
	// last ID in the buffer.
	buffer.advance(descriptorsRangefeed, ts(20))
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(11), combinedFrontierTS)
	require.Equal(t, makeUpdates(2), updates)

	// No updates are left in the buffer below the combined frontier (which hasn't
	// changed from above).
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(11), combinedFrontierTS)
	require.True(t, len(updates) == 0)

	// Try regressing the zones frontier by advancing it to a timestamp below what
	// it was previously advanced to (11). This should essentially no-op, which we
	// can check by looking at the combinedFrontierTS from the call to flush.
	buffer.advance(zonesRangefeed, ts(5))
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(11), combinedFrontierTS)
	require.True(t, len(updates) == 0)

	// Try adding an event at a TS below the combinedFrontierTS. This should no-op
	// as well and the ID should not be returned when flushing.
	err = buffer.add(makeEvent(1, ts(5)))
	require.NoError(t, err)
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(11), combinedFrontierTS)
	require.True(t, len(updates) == 0)

	// Lastly, ensure that flushing doesn't return duplicate IDs even when more
	// than one updates have been added for a given ID.
	err = buffer.add(makeEvent(1, ts(12)))
	require.NoError(t, err)
	err = buffer.add(makeEvent(1, ts(13)))
	require.NoError(t, err)
	err = buffer.add(makeEvent(2, ts(14)))
	require.NoError(t, err)
	buffer.advance(zonesRangefeed, ts(14))
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(14), combinedFrontierTS)
	require.Equal(t, makeUpdates(1, 2), updates)
}

// TestBufferCombinesDescriptorTypes ensures that the descriptor type of the
// same ID is combined correctly when flushing. It also ensures that the
// semantics around flushing are sound, in particular:
// - Type spanconfig.Any can combine with any concrete descriptor type
// (and itself).
// - A concrete descriptor type can combine with itself.
// - A concrete descriptor type cannot combine with a different concrete
// descriptor type.
func TestBufferCombinesDescriptorTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: int64(nanos),
		}
	}
	buffer := newBuffer(10 /* limit */, ts(0))

	makeEvent := func(descID int, descType catalog.DescriptorType, timestamp hlc.Timestamp) event {
		return event{
			update: spanconfig.DescriptorUpdate{
				ID:             descpb.ID(descID),
				DescriptorType: descType,
			},
			timestamp: timestamp,
		}
	}
	advanceRangefeeds := func(timestamp hlc.Timestamp) {
		buffer.advance(descriptorsRangefeed, timestamp)
		buffer.advance(zonesRangefeed, timestamp)
	}
	makeUpdates := func(descID int, descType catalog.DescriptorType) []spanconfig.DescriptorUpdate {
		return []spanconfig.DescriptorUpdate{
			{
				ID:             descpb.ID(descID),
				DescriptorType: descType,
			},
		}
	}

	concreteDescriptorTypes := []catalog.DescriptorType{
		catalog.Table,
		catalog.Database,
		catalog.Schema,
		catalog.Type,
	}

	nanos := 1
	// Test 3 events on the same ID, with a concrete descriptor event
	// (eg. catalog.Table) flanked by catalog.Any on either side. This
	// should all combine to the concrete descriptor type.
	{
		for _, descType := range concreteDescriptorTypes {
			err := buffer.add(makeEvent(1, catalog.Any, ts(nanos)))
			require.NoError(t, err)
			nanos++
			err = buffer.add(makeEvent(1, descType, ts(nanos)))
			require.NoError(t, err)
			nanos++
			err = buffer.add(makeEvent(1, catalog.Any, ts(nanos)))
			require.NoError(t, err)
			nanos++
			advanceRangefeeds(ts(nanos))

			updates, combinedFrontierTS, err := buffer.flush(ctx)
			require.NoError(t, err)
			require.Equal(t, ts(nanos), combinedFrontierTS)
			require.Equal(t, updates, makeUpdates(1, descType))
		}
	}

	// Test all descriptor types (including spanconfig.Any) combine with each
	// other.
	{
		for _, descType := range append(concreteDescriptorTypes, catalog.Any) {
			err := buffer.add(makeEvent(1, descType, ts(nanos)))
			require.NoError(t, err)
			nanos++
			err = buffer.add(makeEvent(1, descType, ts(nanos)))
			require.NoError(t, err)
			nanos++
			err = buffer.add(makeEvent(1, catalog.Any, ts(nanos)))
			require.NoError(t, err)
			nanos++
			advanceRangefeeds(ts(nanos))

			updates, combinedFrontierTS, err := buffer.flush(ctx)
			require.NoError(t, err)
			require.Equal(t, ts(nanos), combinedFrontierTS)
			require.Equal(t, updates, makeUpdates(1, descType))
		}
	}
}
