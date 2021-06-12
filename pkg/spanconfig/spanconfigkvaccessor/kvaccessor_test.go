// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvaccessor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TODO(irfansharif): This interface would benefit from a datadriven test.

func TestKVAccessor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	span := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
	}
	conf := func(maxBytes int64) roachpb.SpanConfig {
		c := zonepb.DefaultZoneConfigRef().AsSpanConfig()
		c.RangeMaxBytes = maxBytes
		return c
	}
	toEntries := func(spans []roachpb.Span) []roachpb.SpanConfigEntry {
		var entries []roachpb.SpanConfigEntry
		for i, sp := range spans {
			entries = append(entries, roachpb.SpanConfigEntry{
				Span:   sp,
				Config: conf(2000 + int64(i)),
			})
		}
		return entries
	}
	mergeLastTwo := func(spans []roachpb.Span) ([]roachpb.Span, roachpb.Span) {
		prevLastSpan := spans[len(spans)-1]
		spans = spans[:len(spans)-1]
		spans[len(spans)-1].EndKey = prevLastSpan.EndKey
		return spans, prevLastSpan
	}

	testSpans := []roachpb.Span{
		span("a", "b"),
		span("b", "c"),
		span("c", "d"),
		span("d", "e"),
	}
	everythingSpan := span("a", "e")
	testEntries := toEntries(testSpans)

	const dummySpanConfigurationsFQN = "defaultdb.public.dummy_span_configurations"
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummySpanConfigurationsFQN))
	accessor := spanconfigkvaccessor.New(
		tc.Server(0).DB(),
		tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor),
		tc.Server(0).ClusterSettings(),
		dummySpanConfigurationsFQN,
	)

	{ // With an empty slate.
		entries, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Empty(t, entries)

		err = accessor.UpdateSpanConfigEntries(ctx, []roachpb.Span{everythingSpan}, nil)
		require.Truef(t, testutils.IsError(err, "expected to delete 1 row"), err.Error())
	}

	{ // Verify that writing and reading a single entry behaves as expected.
		testEntry := testEntries[0]
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, nil, []roachpb.SpanConfigEntry{testEntry}))
		entries, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entries, 1)
		require.True(t, entries[0].Equal(testEntry))

		require.Nil(t, accessor.UpdateSpanConfigEntries(ctx, []roachpb.Span{testEntry.Span}, nil))
		entries, err = accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entries, 0)
	}

	{ // Verify that adding all entries does in fact add all entries.
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, nil, testEntries))
		entries, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entries, len(testSpans))
		for i, got := range entries {
			require.True(t, got.Equal(testEntries[i]))
		}
	}

	{ // Verify that updating entries (including noops) show up as such.
		for i := range testEntries {
			if i%2 == 0 {
				continue
			}
			testEntries[i].Config.RangeMaxBytes += 100
		}
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, nil, testEntries))
		entries, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entries, len(testSpans))
		for i, got := range entries {
			require.True(t, got.Equal(testEntries[i]))
		}
	}

	{ // Verify that fetching entries for multiple spans behaves as expected.
		entries, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{testSpans[1], testSpans[2]})
		require.Nil(t, err)
		require.Len(t, entries, 2)
		require.True(t, entries[0].Equal(testEntries[1]))
		require.True(t, entries[1].Equal(testEntries[2]))
	}

	{ // Verify that deleting entries actually removes them.
		const toDelete = 2
		require.Nil(t, accessor.UpdateSpanConfigEntries(ctx, testSpans[:toDelete], nil))
		entries, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entries, len(testSpans)-toDelete)
		for i, got := range entries {
			require.True(t, got.Equal(testEntries[toDelete+i]))
		}

		// Attempts to delete non existent spans should error out.
		require.NotNil(t, accessor.UpdateSpanConfigEntries(ctx, testSpans[:toDelete], nil))

		// Attempts to re-write previously deleted spans should go through.
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, nil, testEntries[:toDelete]))
		entries, err = accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entries, len(testSpans))
		for i, got := range entries {
			require.True(t, got.Equal(testEntries[i]))
		}
	}

	{ // Verify that we're able to re-partition span configs correctly.
		newTestSpans, prevLast := mergeLastTwo(testSpans)
		newTestEntries := toEntries(newTestSpans)
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, []roachpb.Span{prevLast}, newTestEntries))
		entries, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entries, len(newTestSpans))
		for i, got := range entries {
			require.True(t, got.Equal(newTestEntries[i]))
		}

		// We should also be able to "unmerge" correctly.
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, newTestSpans, testEntries))
		entries, err = accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entries, len(testSpans))
		for i, got := range entries {
			require.True(t, got.Equal(testEntries[i]))
		}
	}
}
