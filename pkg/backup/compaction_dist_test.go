// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCreateCompactionCorePlacements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	sp := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
	}
	entries := func(spans ...roachpb.Span) []execinfrapb.RestoreSpanEntry {
		var entries []execinfrapb.RestoreSpanEntry
		for _, s := range spans {
			entries = append(entries, execinfrapb.RestoreSpanEntry{Span: s})
		}
		return entries
	}

	testcases := []struct {
		name                 string
		entries              []execinfrapb.RestoreSpanEntry
		numNodes             int
		expectedDistribution [][]roachpb.Span
	}{
		{
			name:     "single node, single span entry",
			entries:  entries(sp("a", "b")),
			numNodes: 1,
			expectedDistribution: [][]roachpb.Span{
				{sp("a", "b")},
			},
		},
		{
			name:     "single node, multiple span entries",
			entries:  entries(sp("a", "b"), sp("c", "d"), sp("e", "f")),
			numNodes: 1,
			expectedDistribution: [][]roachpb.Span{
				{sp("a", "b"), sp("c", "d"), sp("e", "f")},
			},
		},
		{
			name:     "single node, multiple overlapping span entries",
			entries:  entries(sp("a", "b"), sp("b", "d"), sp("e", "f")),
			numNodes: 1,
			expectedDistribution: [][]roachpb.Span{
				{sp("a", "d"), sp("e", "f")},
			},
		},
		{
			name:     "multiple nodes, single span entry",
			entries:  entries(sp("a", "b")),
			numNodes: 3,
			expectedDistribution: [][]roachpb.Span{
				{sp("a", "b")},
				nil,
				nil,
			},
		},
		{
			name: "multiple nodes, exact multiple of span entries",
			entries: entries(
				sp("a", "b"), sp("c", "d"),
				sp("e", "f"), sp("g", "h"),
				sp("i", "j"), sp("k", "l"),
			),
			numNodes: 3,
			expectedDistribution: [][]roachpb.Span{
				{sp("a", "b"), sp("c", "d")},
				{sp("e", "f"), sp("g", "h")},
				{sp("i", "j"), sp("k", "l")},
			},
		},
		{
			name: "multiple nodes, exact multiple of overlapping span entries",
			entries: entries(
				sp("a", "b"), sp("b", "d"), sp("d", "f"),
				sp("f", "h"), sp("i", "j"), sp("j", "l"),
			),
			numNodes: 2,
			expectedDistribution: [][]roachpb.Span{
				{sp("a", "f")},
				{sp("f", "h"), sp("i", "l")},
			},
		},
		{
			name: "multiple nodes, not exact multiple of span entries",
			entries: entries(
				sp("a", "b"), sp("c", "d"),
				sp("e", "f"), sp("f", "h"),
				sp("i", "j"),
			),
			numNodes: 3,
			expectedDistribution: [][]roachpb.Span{
				{sp("a", "b"), sp("c", "d")},
				{sp("e", "h")},
				{sp("i", "j")},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			genSpan := func(_ context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error {
				for _, entry := range tc.entries {
					spanCh <- entry
				}
				return nil
			}
			sqlInstanceIDs := make([]base.SQLInstanceID, tc.numNodes)
			for i := range tc.numNodes {
				sqlInstanceIDs[i] = base.SQLInstanceID(i)
			}
			placements, err := createCompactionCorePlacements(
				ctx,
				0, /* jobID */
				username.RootUserName(),
				jobspb.BackupDetails{},
				execinfrapb.ElidePrefix_None,
				genSpan,
				nil, /* spansToCompact */
				sqlInstanceIDs,
				0, /* targetSize */
				0, /* maxFiles */
				len(tc.entries),
			)
			require.NoError(t, err)
			require.Equal(t, tc.numNodes, len(placements))
			for i, expected := range tc.expectedDistribution {
				core := placements[i].Core.CompactBackups
				require.NotNil(t, core)
				require.Equal(t, expected, core.AssignedSpans)
			}
		})
	}
}
