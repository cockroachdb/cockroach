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
	type Key = roachpb.Key
	testcases := []struct {
		name                 string
		entries              []execinfrapb.RestoreSpanEntry
		numNodes             int
		expectedDistribution [][]roachpb.Span
	}{
		{
			name: "single node, single span entry",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: Key("a"), EndKey: Key("b")}},
			},
			numNodes: 1,
			expectedDistribution: [][]roachpb.Span{
				{{Key: Key("a"), EndKey: Key("b")}},
			},
		},
		{
			name: "single node, multiple span entries",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: Key("a"), EndKey: Key("b")}},
				{Span: roachpb.Span{Key: Key("c"), EndKey: Key("d")}},
				{Span: roachpb.Span{Key: Key("e"), EndKey: Key("f")}},
			},
			numNodes: 1,
			expectedDistribution: [][]roachpb.Span{
				{
					{Key: Key("a"), EndKey: Key("b")},
					{Key: Key("c"), EndKey: Key("d")},
					{Key: Key("e"), EndKey: Key("f")},
				},
			},
		},
		{
			name: "single node, multiple overlapping span entries",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: Key("a"), EndKey: Key("b")}},
				{Span: roachpb.Span{Key: Key("b"), EndKey: Key("d")}},
				{Span: roachpb.Span{Key: Key("e"), EndKey: Key("f")}},
			},
			numNodes: 1,
			expectedDistribution: [][]roachpb.Span{
				{{Key: Key("a"), EndKey: Key("d")}, {Key: Key("e"), EndKey: Key("f")}},
			},
		},
		{
			name: "multiple nodes, single span entry",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: Key("a"), EndKey: Key("b")}},
			},
			numNodes: 3,
			expectedDistribution: [][]roachpb.Span{
				{{Key: Key("a"), EndKey: Key("b")}},
				nil,
				nil,
			},
		},
		{
			name: "multiple nodes, exact multiple of span entries",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: Key("a"), EndKey: Key("b")}},
				{Span: roachpb.Span{Key: Key("c"), EndKey: Key("d")}},
				{Span: roachpb.Span{Key: Key("e"), EndKey: Key("f")}},
				{Span: roachpb.Span{Key: Key("g"), EndKey: Key("h")}},
				{Span: roachpb.Span{Key: Key("i"), EndKey: Key("j")}},
				{Span: roachpb.Span{Key: Key("k"), EndKey: Key("l")}},
			},
			numNodes: 3,
			expectedDistribution: [][]roachpb.Span{
				{{Key: Key("a"), EndKey: Key("b")}, {Key: Key("c"), EndKey: Key("d")}},
				{{Key: Key("e"), EndKey: Key("f")}, {Key: Key("g"), EndKey: Key("h")}},
				{{Key: Key("i"), EndKey: Key("j")}, {Key: Key("k"), EndKey: Key("l")}},
			},
		},
		{
			name: "multiple nodes, exact multiple of overlapping span entries",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: Key("a"), EndKey: Key("b")}},
				{Span: roachpb.Span{Key: Key("b"), EndKey: Key("d")}},
				{Span: roachpb.Span{Key: Key("e"), EndKey: Key("f")}},
				{Span: roachpb.Span{Key: Key("f"), EndKey: Key("h")}},
				{Span: roachpb.Span{Key: Key("i"), EndKey: Key("j")}},
				{Span: roachpb.Span{Key: Key("j"), EndKey: Key("l")}},
			},
			numNodes: 3,
			expectedDistribution: [][]roachpb.Span{
				{{Key: Key("a"), EndKey: Key("d")}},
				{{Key: Key("e"), EndKey: Key("h")}},
				{{Key: Key("i"), EndKey: Key("l")}},
			},
		},
		{
			name: "multiple nodes, not exact multiple of span entries",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: Key("a"), EndKey: Key("b")}},
				{Span: roachpb.Span{Key: Key("c"), EndKey: Key("d")}},
				{Span: roachpb.Span{Key: Key("e"), EndKey: Key("f")}},
				{Span: roachpb.Span{Key: Key("g"), EndKey: Key("h")}},
				{Span: roachpb.Span{Key: Key("i"), EndKey: Key("j")}},
				{Span: roachpb.Span{Key: Key("k"), EndKey: Key("l")}},
				{Span: roachpb.Span{Key: Key("m"), EndKey: Key("n")}},
				{Span: roachpb.Span{Key: Key("o"), EndKey: Key("p")}},
			},
			numNodes: 3,
			expectedDistribution: [][]roachpb.Span{
				{
					{Key: Key("a"), EndKey: Key("b")},
					{Key: Key("c"), EndKey: Key("d")},
					{Key: Key("e"), EndKey: Key("f")},
				},
				{
					{Key: Key("g"), EndKey: Key("h")},
					{Key: Key("i"), EndKey: Key("j")},
					{Key: Key("k"), EndKey: Key("l")},
				},
				{
					{Key: Key("m"), EndKey: Key("n")}, {Key: Key("o"), EndKey: Key("p")},
				},
			},
		},
		{
			name: "multiple nodes, not exact multiple of overlapping span entries",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: Key("a"), EndKey: Key("b")}},
				{Span: roachpb.Span{Key: Key("b"), EndKey: Key("d")}},
				{Span: roachpb.Span{Key: Key("d"), EndKey: Key("f")}},
				{Span: roachpb.Span{Key: Key("f"), EndKey: Key("h")}},
				{Span: roachpb.Span{Key: Key("i"), EndKey: Key("j")}},
				{Span: roachpb.Span{Key: Key("j"), EndKey: Key("l")}},
				{Span: roachpb.Span{Key: Key("m"), EndKey: Key("n")}},
				{Span: roachpb.Span{Key: Key("n"), EndKey: Key("p")}},
			},
			numNodes: 3,
			expectedDistribution: [][]roachpb.Span{
				{
					{Key: Key("a"), EndKey: Key("f")},
				},
				{
					{Key: Key("f"), EndKey: Key("h")},
					{Key: Key("i"), EndKey: Key("l")},
				},
				{
					{Key: Key("m"), EndKey: Key("p")},
				},
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
