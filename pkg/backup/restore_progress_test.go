// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	gogotypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

// TestProgressTracker tests the lifecycle of the checkpoint frontier by testing the following
// over a sequence of updates on a mock set of required spans:
// - loading the persisted frontier into memory
// - updating to the in memory frontier
// - persisting the in memory frontier
func TestProgressTracker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	c := makeCoverUtils(ctx, t, &execCfg)

	requiredSpans := []roachpb.Span{c.sp("a", "e"), c.sp("f", "i")}

	mockUpdate := func(sp roachpb.Span, completeUpTo hlc.Timestamp) *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress {
		restoreProgress := backuppb.RestoreProgress{DataSpan: sp, CompleteUpTo: completeUpTo}
		details, err := gogotypes.MarshalAny(&restoreProgress)
		require.NoError(t, err)
		return &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{ProgressDetails: *details}
	}

	// testStep characterizes an update to the span frontier and the expected
	// persisted spans after the update.
	type testStep struct {
		update            roachpb.Span
		expectedPersisted []jobspb.RestoreProgress_FrontierEntry
		completeUpTo      hlc.Timestamp
	}

	// Each test step builds on the persistedSpan.
	persistedSpans := make([]jobspb.RestoreProgress_FrontierEntry, 0)
	for i, step := range []testStep{
		{
			update:            c.sp("a", "c"),
			expectedPersisted: pSp(c.sp("a", "c")),
		},
		{
			// Last update in first required span should extend the persisted end key
			// to the start key of the second required span.
			update:            c.sp("c", "e"),
			expectedPersisted: pSp(c.sp("a", "f")),
		},
		{
			update:            c.sp("h", "i"),
			expectedPersisted: pSp(c.sp("a", "f")),
			completeUpTo:      hlc.Timestamp{Logical: 1},
		},
		{
			update:            c.sp("h", "i"),
			expectedPersisted: pSp(c.sp("a", "f"), c.sp("h", "i")),
		},
		{
			// After both required spans completed, only one persisted span should exist.
			update:            c.sp("f", "h"),
			expectedPersisted: pSp(c.sp("a", "i")),
		},
	} {
		restoreTime := hlc.Timestamp{}
		pt, err := makeProgressTracker(requiredSpans, persistedSpans, 0, restoreTime)
		require.NoError(t, err, "step %d", i)

		done, err := pt.ingestUpdate(ctx, mockUpdate(step.update, step.completeUpTo))
		require.NoError(t, err)
		lastInSpan := step.completeUpTo == restoreTime
		require.Equal(t, lastInSpan, done, "step %d", i)

		persistedSpans = persistFrontier(pt.mu.checkpointFrontier, 0)
		require.Equal(t, step.expectedPersisted, persistedSpans, "step %d", i)
	}
}

func pSp(spans ...roachpb.Span) []jobspb.RestoreProgress_FrontierEntry {
	pSpans := make([]jobspb.RestoreProgress_FrontierEntry, 0)
	for _, sp := range spans {
		pSpans = append(pSpans, jobspb.RestoreProgress_FrontierEntry{Span: sp, Timestamp: completedSpanTime})
	}
	return pSpans
}

func TestPersistedFrontierEqual(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		ps1   []jobspb.RestoreProgress_FrontierEntry
		ps2   []jobspb.RestoreProgress_FrontierEntry
		equal bool
	}

	sp := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
	}

	for _, tc := range []testCase{
		{
			ps1:   pSp(sp("a", "c"), sp("d", "e")),
			ps2:   pSp(sp("a", "c"), sp("d", "e")),
			equal: true,
		},
		{
			ps1:   pSp(sp("a", "c")),
			ps2:   pSp(sp("a", "c"), sp("d", "e")),
			equal: false,
		},
		{
			ps1:   pSp(sp("a", "c")),
			equal: false,
		},
		{
			ps1:   pSp(sp("a", "c"), sp("d", "e")),
			ps2:   pSp(sp("a", "c"), sp("d", "f")),
			equal: false,
		},
		{
			ps1:   pSp(sp("a", "c"), sp("d", "e")),
			ps2:   pSp(sp("a", "e")),
			equal: false,
		},
	} {
		var (
			ps1Entries jobspb.RestoreFrontierEntries
			ps2Entries jobspb.RestoreFrontierEntries
		)
		ps1Entries = tc.ps1
		ps2Entries = tc.ps2
		require.Equal(t, tc.equal, ps1Entries.Equal(ps2Entries))
		require.Equal(t, tc.equal, ps2Entries.Equal(ps1Entries))

	}
}
