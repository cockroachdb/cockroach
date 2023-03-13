// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	c := makeCoverUtils(ctx, t, &execCfg)

	requiredSpans := []roachpb.Span{c.sp("a", "e"), c.sp("f", "i")}

	mockUpdate := func(sp roachpb.Span) *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress {
		restoreProgress := backuppb.RestoreProgress{DataSpan: sp}
		details, err := gogotypes.MarshalAny(&restoreProgress)
		require.NoError(t, err)
		return &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{ProgressDetails: *details}
	}

	pSp := func(spans ...roachpb.Span) []jobspb.RestoreProgress_FrontierEntry {
		pSpans := make([]jobspb.RestoreProgress_FrontierEntry, 0)
		for _, sp := range spans {
			pSpans = append(pSpans, jobspb.RestoreProgress_FrontierEntry{Span: sp, Timestamp: completedSpanTime})
		}
		return pSpans
	}

	// testStep characterizes an update to the span frontier and the expected
	// persisted spans after the update.
	type testStep struct {
		update            roachpb.Span
		expectedPersisted []jobspb.RestoreProgress_FrontierEntry
	}

	// Each test step builds on the persistedSpan.
	persistedSpans := make([]jobspb.RestoreProgress_FrontierEntry, 0)
	for _, step := range []testStep{
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
			expectedPersisted: pSp(c.sp("a", "f"), c.sp("h", "i")),
		},
		{
			// After both required spans completed, only one persisted span should exist.
			update:            c.sp("f", "h"),
			expectedPersisted: pSp(c.sp("a", "i")),
		},
	} {
		pt, err := makeProgressTracker(requiredSpans, persistedSpans, true, 0)
		require.NoError(t, err)

		require.NoError(t, pt.ingestUpdate(ctx, mockUpdate(step.update)))

		persistedSpans = persistFrontier(pt.mu.checkpointFrontier, 0)
		require.Equal(t, step.expectedPersisted, persistedSpans)
	}
}
