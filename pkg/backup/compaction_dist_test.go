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
	testcases := []struct {
		name                 string
		entries              []execinfrapb.RestoreSpanEntry
		numNodes             int
		expectedDistribution []int // expected number of entries per node
	}{
		{
			name: "single node, single span entry",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
			},
			numNodes:             1,
			expectedDistribution: []int{1},
		},
		{
			name: "single node, multiple span entries",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
				{Span: roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}},
			},
			numNodes:             1,
			expectedDistribution: []int{3},
		},
		{
			name: "multiple nodes, single span entry",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
			},
			numNodes:             3,
			expectedDistribution: []int{1, 0, 0},
		},
		{
			name: "multiple nodes, exact multiple of span entries",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
				{Span: roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}},
				{Span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}},
				{Span: roachpb.Span{Key: roachpb.Key("i"), EndKey: roachpb.Key("j")}},
				{Span: roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("l")}},
			},
			numNodes:             3,
			expectedDistribution: []int{2, 2, 2},
		},
		{
			name: "multiple nodes, not exact multiple of span entries",
			entries: []execinfrapb.RestoreSpanEntry{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
				{Span: roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key("f")}},
				{Span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}},
				{Span: roachpb.Span{Key: roachpb.Key("i"), EndKey: roachpb.Key("j")}},
				{Span: roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("l")}},
				{Span: roachpb.Span{Key: roachpb.Key("m"), EndKey: roachpb.Key("n")}},
				{Span: roachpb.Span{Key: roachpb.Key("o"), EndKey: roachpb.Key("p")}},
			},
			numNodes:             3,
			expectedDistribution: []int{3, 3, 2},
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
				require.Equal(t, expected, len(core.AssignedSpans))
			}
		})
	}
}
