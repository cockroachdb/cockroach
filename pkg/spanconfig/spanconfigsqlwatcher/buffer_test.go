// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigsqlwatcher

import (
	"context"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	makePTSEvent := func(isClusterTarget bool, tenantID roachpb.TenantID, timestamp hlc.Timestamp) event {
		if isClusterTarget {
			return event{update: spanconfig.MakeClusterProtectedTimestampSQLUpdate(), timestamp: timestamp}
		}
		return event{update: spanconfig.MakeTenantProtectedTimestampSQLUpdate(tenantID), timestamp: timestamp}
	}
	makePTSUpdates := func(targets ...ptpb.Target) []spanconfig.SQLUpdate {
		updates := make([]spanconfig.SQLUpdate, 0, len(targets))
		for _, target := range targets {
			switch t := target.GetUnion().(type) {
			case *ptpb.Target_Cluster:
				updates = append(updates, spanconfig.MakeClusterProtectedTimestampSQLUpdate())
			case *ptpb.Target_Tenants:
				for _, tenID := range t.Tenants.IDs {
					updates = append(updates, spanconfig.MakeTenantProtectedTimestampSQLUpdate(tenID))
				}
			}
		}
		return updates
	}
	makeDescriptorEvent := func(descID int, timestamp hlc.Timestamp) event {
		return event{
			update:    spanconfig.MakeDescriptorSQLUpdate(descpb.ID(descID), catalog.Any),
			timestamp: timestamp,
		}
	}
	makeDescriptorUpdates := func(descIDs ...int) []spanconfig.SQLUpdate {
		updates := make([]spanconfig.SQLUpdate, 0, len(descIDs))
		for _, descID := range descIDs {
			updates = append(updates, spanconfig.MakeDescriptorSQLUpdate(descpb.ID(descID), catalog.Any))
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
	require.NoError(t, buffer.add(makeDescriptorEvent(1, ts(10))))
	require.NoError(t, err)

	require.NoError(t, buffer.add(makeDescriptorEvent(2, ts(11))))
	require.NoError(t, err)

	err = buffer.add(makePTSEvent(true, roachpb.TenantID{}, ts(12)))
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

	// Advance the descriptors frontier to a higher timestamp than the zones
	// frontier above. We expect flush to still not return any results as the pts
	// frontier hasn't been advanced yet.
	buffer.advance(descriptorsRangefeed, ts(13))
	require.NoError(t, err)
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(1), combinedFrontierTS)
	require.True(t, len(updates) == 0)

	// Advance the pts frontier to a lower timestamp than the zones frontier
	// above. Flush should now return the lower timestamp as the
	// combinedFrontierTS. Furthermore, we only expect one id to be returned.
	buffer.advance(protectedTimestampRangefeed, ts(10))
	require.NoError(t, err)
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(10), combinedFrontierTS)
	require.Equal(t, makeDescriptorUpdates(1), updates)

	// Bump the descriptors and protected timestamp frontier past the zones
	// frontier. This should bump the combinedFrontierTS of the buffer to 11,
	// resulting in flush returning the last descriptor SQLUpdate in the buffer.
	buffer.advance(descriptorsRangefeed, ts(20))
	buffer.advance(protectedTimestampRangefeed, ts(21))
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(11), combinedFrontierTS)
	require.Equal(t, makeDescriptorUpdates(2), updates)

	// Bump the zones frontier to a timestamp past the last SQLUpdate. This should
	// bump the combinedFrontierTS of the buffer to 13, resulting in flush
	// returning the last, and only PTS SQLUpdate in the buffer.
	buffer.advance(zonesRangefeed, ts(13))
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(13), combinedFrontierTS)
	require.Equal(t, makePTSUpdates(*ptpb.MakeClusterTarget()), updates)

	// No updates are left in the buffer below the combined frontier (which hasn't
	// changed from above).
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(13), combinedFrontierTS)
	require.True(t, len(updates) == 0)

	// Try regressing the zones frontier by advancing it to a timestamp below what
	// it was previously advanced to (11). This should essentially no-op, which we
	// can check by looking at the combinedFrontierTS from the call to flush.
	buffer.advance(zonesRangefeed, ts(5))
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(13), combinedFrontierTS)
	require.True(t, len(updates) == 0)

	// Try adding events at a TS below the combinedFrontierTS. This should no-op
	// as well and the ID should not be returned when flushing.
	require.NoError(t, buffer.add(makeDescriptorEvent(1, ts(5))))
	require.NoError(t, buffer.add(makePTSEvent(true, roachpb.TenantID{}, ts(5))))
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(13), combinedFrontierTS)
	require.True(t, len(updates) == 0)

	// Ensure that flushing doesn't return duplicate IDs even when more than one
	// descriptor SQLUpdates have been added for a given ID.
	require.NoError(t, buffer.add(makeDescriptorEvent(1, ts(14))))
	require.NoError(t, buffer.add(makeDescriptorEvent(1, ts(15))))

	// Interleave some PTS SQLUpdates to make sure they are de-duped correctly,
	// and interact as expected with descriptor SQLUpdates.
	require.NoError(t, buffer.add(makePTSEvent(true, roachpb.TenantID{}, ts(16))))
	require.NoError(t, buffer.add(
		makePTSEvent(false, roachpb.MustMakeTenantID(1), ts(17))))
	require.NoError(t, buffer.add(
		makePTSEvent(false, roachpb.MustMakeTenantID(1), ts(18))))
	require.NoError(t, buffer.add(makePTSEvent(true, roachpb.TenantID{}, ts(19))))

	require.NoError(t, buffer.add(makeDescriptorEvent(2, ts(20))))

	buffer.advance(zonesRangefeed, ts(20))
	updates, combinedFrontierTS, err = buffer.flush(ctx)
	require.NoError(t, err)
	require.Equal(t, ts(20), combinedFrontierTS)
	expectedUpdates := makeDescriptorUpdates(1, 2)
	expectedUpdates = append(expectedUpdates,
		makePTSUpdates(
			*ptpb.MakeTenantsTarget([]roachpb.TenantID{roachpb.MustMakeTenantID(1)}),
			*ptpb.MakeClusterTarget())...,
	)
	require.Equal(t, expectedUpdates, updates)
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

	makeDescriptorEvent := func(descID int, descType catalog.DescriptorType,
		timestamp hlc.Timestamp) event {
		return event{
			update:    spanconfig.MakeDescriptorSQLUpdate(descpb.ID(descID), descType),
			timestamp: timestamp,
		}
	}
	advanceRangefeeds := func(timestamp hlc.Timestamp) {
		buffer.advance(descriptorsRangefeed, timestamp)
		buffer.advance(zonesRangefeed, timestamp)
		buffer.advance(protectedTimestampRangefeed, timestamp)
	}
	makeDescriptorSQLUpdate := func(descID int,
		descType catalog.DescriptorType) spanconfig.SQLUpdate {
		return spanconfig.MakeDescriptorSQLUpdate(descpb.ID(descID), descType)
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
			err := buffer.add(makeDescriptorEvent(1, catalog.Any, ts(nanos)))
			require.NoError(t, err)
			nanos++
			err = buffer.add(makeDescriptorEvent(1, descType, ts(nanos)))
			require.NoError(t, err)
			nanos++
			err = buffer.add(makeDescriptorEvent(1, catalog.Any, ts(nanos)))
			require.NoError(t, err)
			nanos++
			advanceRangefeeds(ts(nanos))

			updates, combinedFrontierTS, err := buffer.flush(ctx)
			require.NoError(t, err)
			require.Equal(t, ts(nanos), combinedFrontierTS)
			require.Equal(t, []spanconfig.SQLUpdate{makeDescriptorSQLUpdate(1, descType)}, updates)
		}
	}

	// Test all descriptor types (including spanconfig.Any) combine with each
	// other.
	{
		for _, descType := range append(concreteDescriptorTypes, catalog.Any) {
			err := buffer.add(makeDescriptorEvent(1, descType, ts(nanos)))
			require.NoError(t, err)
			nanos++
			err = buffer.add(makeDescriptorEvent(1, descType, ts(nanos)))
			require.NoError(t, err)
			nanos++
			err = buffer.add(makeDescriptorEvent(1, catalog.Any, ts(nanos)))
			require.NoError(t, err)
			nanos++
			advanceRangefeeds(ts(nanos))

			updates, combinedFrontierTS, err := buffer.flush(ctx)
			require.NoError(t, err)
			require.Equal(t, ts(nanos), combinedFrontierTS)
			require.Equal(t, []spanconfig.SQLUpdate{makeDescriptorSQLUpdate(1, descType)}, updates)
		}
	}
}

func TestBufferedEventsSort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	makePTSEvent := func(isClusterTarget bool, tenantID roachpb.TenantID, timestamp hlc.Timestamp) event {
		if isClusterTarget {
			return event{
				update:    spanconfig.MakeClusterProtectedTimestampSQLUpdate(),
				timestamp: timestamp,
			}
		}
		return event{
			update:    spanconfig.MakeTenantProtectedTimestampSQLUpdate(tenantID),
			timestamp: timestamp,
		}
	}
	makeDescriptorEvent := func(descID int, timestamp hlc.Timestamp) event {
		return event{
			update:    spanconfig.MakeDescriptorSQLUpdate(descpb.ID(descID), catalog.Any),
			timestamp: timestamp,
		}
	}
	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: int64(nanos),
		}
	}

	evs := make(events, 0)

	// Insert a few descriptor updates.
	evs = append(evs, makeDescriptorEvent(1, ts(10)))
	evs = append(evs, makeDescriptorEvent(2, ts(5)))
	evs = append(evs, makeDescriptorEvent(2, ts(6)))

	// Insert a few cluster updates.
	evs = append(evs, makePTSEvent(true, roachpb.TenantID{}, ts(3)))
	evs = append(evs, makePTSEvent(true, roachpb.TenantID{}, ts(2)))

	// Insert a few tenant updates that should sort before the cluster updates.
	evs = append(evs, makePTSEvent(false, roachpb.MustMakeTenantID(2), ts(3)))
	evs = append(evs, makePTSEvent(false, roachpb.MustMakeTenantID(1), ts(7)))
	evs = append(evs, makePTSEvent(false, roachpb.MustMakeTenantID(2), ts(2)))

	// Insert a few more descriptor updates to interleave the updates.
	evs = append(evs, makeDescriptorEvent(4, ts(12)))
	evs = append(evs, makeDescriptorEvent(5, ts(11)))

	// Shuffle the slice for fun.
	rand.Shuffle(len(evs), func(i, j int) {
		evs[i], evs[j] = evs[j], evs[i]
	})

	sort.Sort(evs)

	require.Equal(t, events{
		makeDescriptorEvent(1, ts(10)),
		makeDescriptorEvent(2, ts(5)),
		makeDescriptorEvent(2, ts(6)),
		makeDescriptorEvent(4, ts(12)),
		makeDescriptorEvent(5, ts(11)),
		makePTSEvent(false, roachpb.MustMakeTenantID(1), ts(7)),
		makePTSEvent(false, roachpb.MustMakeTenantID(2), ts(2)),
		makePTSEvent(false, roachpb.MustMakeTenantID(2), ts(3)),
		makePTSEvent(true, roachpb.TenantID{}, ts(2)),
		makePTSEvent(true, roachpb.TenantID{}, ts(3)),
	}, evs)

}
