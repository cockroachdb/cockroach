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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestKVAccessor(t *testing.T) {
	defer leaktest.AfterTest(t)

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
	everythingSpan := roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey}

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
		require.Len(t, entries, 1)
		require.Empty(t, entries[0])

		err = accessor.UpdateSpanConfigEntries(ctx, []roachpb.Span{everythingSpan}, nil)
		require.Truef(t, testutils.IsError(err, "expected to delete 1 row"), err.Error())
	}

	// Write and delete a batch of entries.
	spans := []roachpb.Span{
		span("a", "b"),
		span("b", "c"),
		span("c", "d"),
		span("d", "e"),
	}
	entries := toEntries(spans)

	{ // Verify that writing and reading a single entry behaves as expected.
		entry := entries[0]
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, nil, []roachpb.SpanConfigEntry{entry}))
		entriesList, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entriesList, 1)
		require.Len(t, entriesList[0], 1)
		got := entriesList[0][0]
		require.True(t, got.Equal(entry))

		require.Nil(t, accessor.UpdateSpanConfigEntries(ctx, []roachpb.Span{entry.Span}, nil))
		entriesList, err = accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entriesList, 1)
		require.Empty(t, entriesList[0])
	}

	{ // Verify that adding all entries does in fact add all entries.
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, nil, entries))
		entriesList, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entriesList, 1)
		require.Len(t, entriesList[0], len(spans))
		for i, got := range entriesList[0] {
			require.True(t, got.Equal(entries[i]))
		}
	}

	{ // Verify that updating entries (including noops) show up as such.
		for i := range entries {
			if i%2 == 0 {
				continue
			}
			entries[i].Config.RangeMaxBytes += 100
		}
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, nil, entries))
		entriesList, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entriesList, 1)
		require.Len(t, entriesList[0], len(spans))
		for i, got := range entriesList[0] {
			require.True(t, got.Equal(entries[i]))
		}
	}

	{ // Verify that fetching entries for multiple spans behaves as expected.
		entriesList, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan, spans[1], spans[2]})
		require.Nil(t, err)
		require.Len(t, entriesList, 3)
		require.Len(t, entriesList[0], len(spans))
		for i, got := range entriesList[0] {
			require.True(t, got.Equal(entries[i]))
		}

		require.Len(t, entriesList[1], 1)
		require.True(t, entriesList[1][0].Equal(entries[1]))

		require.Len(t, entriesList[2], 1)
		require.True(t, entriesList[2][0].Equal(entries[2]))
	}

	{ // Verify that deleting entries actually removes them.
		const toDelete = 2
		require.Nil(t, accessor.UpdateSpanConfigEntries(ctx, spans[:toDelete], nil))
		entriesList, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entriesList, 1)
		require.Len(t, entriesList[0], len(spans)-toDelete)
		for i, got := range entriesList[0] {
			require.True(t, got.Equal(entries[toDelete+i]))
		}

		// Attempts to delete non existent spans should error out.
		require.NotNil(t, accessor.UpdateSpanConfigEntries(ctx, spans[:toDelete], nil))
		// Attempts to re-write previously deleted spans should go through.
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, nil, entries[:toDelete]))
	}

	{ // Verify that we're able to re-partition span configs correctly.
		spans, prevLast := mergeLastTwo(spans)
		entries := toEntries(spans)
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, []roachpb.Span{prevLast}, entries))
		entriesList, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entriesList, 1)
		require.Len(t, entriesList[0], len(spans))
		for i, got := range entriesList[0] {
			require.True(t, got.Equal(entries[i]))
		}
	}
}
