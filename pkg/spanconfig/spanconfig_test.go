// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestKVAccessor(t *testing.T) {
	defer leaktest.AfterTest(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SpanConfig: &spanconfig.TestingKnobs{
				// Disable the reconciliation process; this test wants sole
				// access to the server's SpanConfigAccessor.
				ManagerDisableJobCreation: true,
			},
		},
	})
	defer s.Stopper().Stop(context.Background())

	span := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
	}
	conf := func(maxBytes int64) roachpb.SpanConfig {
		c := kvserver.TestingDefaultSpanConfig()
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

	ts := s.(*server.TestServer)
	everythingSpan := roachpb.Span{Key: keys.MinKey, EndKey: keys.MaxKey}
	accessor := ts.SpanConfigAccessor().(spanconfig.KVAccessor)

	{ // With an empty slate.
		entries, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entries, 1)
		require.Empty(t, entries[0])

		require.True(t, testutils.IsError(
			accessor.UpdateSpanConfigEntries(ctx, nil, []roachpb.Span{everythingSpan} /* delete */),
			"expected to delete single row"))
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
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, []roachpb.SpanConfigEntry{entry}, nil))
		entriesList, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entriesList, 1)
		require.Len(t, entriesList[0], 1)
		got := entriesList[0][0]
		require.True(t, got.Equal(entry))

		require.Nil(t, accessor.UpdateSpanConfigEntries(ctx, nil, []roachpb.Span{entry.Span} /* delete */))
		entriesList, err = accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entriesList, 1)
		require.Empty(t, entriesList[0])
	}

	{ // Verify that adding all entries does in fact add all entries.
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, entries, nil))
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
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, entries, nil))
		entriesList, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entriesList, 1)
		require.Len(t, entriesList[0], len(spans))
		for i, got := range entriesList[0] {
			require.True(t, got.Equal(entries[i]))
		}
	}

	{ // Verify that deleting entries actually removes them.
		const toDelete = 2
		require.Nil(t, accessor.UpdateSpanConfigEntries(ctx, nil, spans[:toDelete] /* delete */))
		entriesList, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entriesList, 1)
		require.Len(t, entriesList[0], len(spans)-toDelete)
		for i, got := range entriesList[0] {
			require.True(t, got.Equal(entries[toDelete+i]))
		}

		// Attempts to delete non existent spans should error out.
		require.NotNil(t, accessor.UpdateSpanConfigEntries(ctx, nil, spans[:toDelete] /* delete */))
		// Attempts to re-write previously deleted spans should go through.
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, entries[:toDelete], nil))
	}

	{ // Verify that we're able to re-partition span configs correctly.
		spans, prevLast := mergeLastTwo(spans)
		entries := toEntries(spans)
		require.NoError(t, accessor.UpdateSpanConfigEntries(ctx, entries, []roachpb.Span{prevLast}))
		entriesList, err := accessor.GetSpanConfigEntriesFor(ctx, []roachpb.Span{everythingSpan})
		require.Nil(t, err)
		require.Len(t, entriesList, 1)
		require.Len(t, entriesList[0], len(spans))
		for i, got := range entriesList[0] {
			require.True(t, got.Equal(entries[i]))
		}
	}
}
