// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// NOTE: This test is for functions in ttljob.go. We already have
// ttljob_test.go, but that is part of the ttljob_test package. This test is
// specifically part of the ttljob package to access non-exported functions and
// structs. Hence, the name '_internal_' in the file to signify that it accesses
// internal functions.

func makeFakeSpans(n int) []roachpb.Span {
	spans := make([]roachpb.Span, n)
	for i := 0; i < n; i++ {
		start := roachpb.Key(fmt.Sprintf("k%03d", i))
		end := roachpb.Key(fmt.Sprintf("k%03d", i+1))
		spans[i] = roachpb.Span{Key: start, EndKey: end}
	}
	return spans
}

func TestTTLProgressLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	infra := &physicalplan.PhysicalInfrastructure{
		Processors: []physicalplan.Processor{
			{
				SQLInstanceID: base.SQLInstanceID(11),
				Spec: execinfrapb.ProcessorSpec{
					ProcessorID: 1,
					Core: execinfrapb.ProcessorCoreUnion{
						Ttl: &execinfrapb.TTLSpec{
							Spans: makeFakeSpans(100),
						},
					},
				},
			},
			{
				SQLInstanceID: base.SQLInstanceID(12),
				Spec: execinfrapb.ProcessorSpec{
					ProcessorID: 2,
					Core: execinfrapb.ProcessorCoreUnion{
						Ttl: &execinfrapb.TTLSpec{
							Spans: makeFakeSpans(100),
						},
					},
				},
			},
		},
	}
	physPlan := physicalplan.PhysicalPlan{
		PhysicalInfrastructure: infra,
	}
	sqlPlan := sql.PhysicalPlan{
		PhysicalPlan: physPlan,
	}
	resumer := rowLevelTTLResumer{
		physicalPlan: &sqlPlan,
	}

	// Create two processors to match the IDs used in the physical plan
	proc1 := mockProcessor(1, roachpb.NodeID(11), 100)
	proc2 := mockProcessor(2, roachpb.NodeID(12), 100)
	mockRowReceiver := metadataCache{}

	// Step 1: initProgress
	progress, err := resumer.initProgress(200)
	require.NoError(t, err)
	require.NotNil(t, progress)
	require.Equal(t, float32(0), progress.GetFractionCompleted())
	ttlProgress := progress.GetRowLevelTTL()
	require.Equal(t, int64(200), ttlProgress.JobTotalSpanCount)
	require.Zero(t, ttlProgress.JobProcessedSpanCount)
	require.Zero(t, ttlProgress.JobDeletedRowCount)
	require.Len(t, ttlProgress.ProcessorProgresses, 0)
	md := jobs.JobMetadata{
		Progress: progress,
	}

	// First refresh (processor 1 partial)
	proc1.progressUpdater.OnSpanProcessed(50, 100)
	err = proc1.progressUpdater.UpdateProgress(ctx, &mockRowReceiver)
	require.NoError(t, err)
	progress, err = resumer.refreshProgress(ctx, &md, mockRowReceiver.GetLatest())
	require.NoError(t, err)
	require.NotNil(t, progress)
	md.Progress = progress

	require.NoError(t, err)
	require.InEpsilon(t, 0.25, progress.GetFractionCompleted(), 0.001)
	ttlProgress = progress.GetRowLevelTTL()
	require.Equal(t, int64(200), ttlProgress.JobTotalSpanCount)
	require.Equal(t, int64(50), ttlProgress.JobProcessedSpanCount)
	require.Equal(t, int64(100), ttlProgress.JobDeletedRowCount)
	require.Len(t, ttlProgress.ProcessorProgresses, 1)

	// Second refresh (processor 2 full)
	proc2.progressUpdater.OnSpanProcessed(100, 400)
	err = proc2.progressUpdater.UpdateProgress(ctx, &mockRowReceiver)
	require.NoError(t, err)
	progress, err = resumer.refreshProgress(ctx, &md, mockRowReceiver.GetLatest())
	require.NoError(t, err)
	require.NotNil(t, progress)
	md.Progress = progress

	require.NoError(t, err)
	require.InEpsilon(t, 0.75, progress.GetFractionCompleted(), 0.001)
	ttlProgress = progress.GetRowLevelTTL()
	require.Equal(t, int64(200), ttlProgress.JobTotalSpanCount)
	require.Equal(t, int64(150), ttlProgress.JobProcessedSpanCount)
	require.Equal(t, int64(500), ttlProgress.JobDeletedRowCount)
	require.Len(t, ttlProgress.ProcessorProgresses, 2)

	// No update refresh (processor 1 empty)
	err = proc1.progressUpdater.UpdateProgress(ctx, &mockRowReceiver) // No call to OnSpanProcessed
	require.NoError(t, err)
	progress, err = resumer.refreshProgress(ctx, &md, mockRowReceiver.GetLatest())
	require.NoError(t, err)
	require.Nil(t, progress)

	// Final refresh (processor 1 remaining)
	proc1.progressUpdater.OnSpanProcessed(50, 500)
	err = proc1.progressUpdater.UpdateProgress(ctx, &mockRowReceiver)
	require.NoError(t, err)
	progress, err = resumer.refreshProgress(ctx, &md, mockRowReceiver.GetLatest())
	require.NoError(t, err)
	require.NotNil(t, progress)

	require.NoError(t, err)
	require.InEpsilon(t, 1, progress.GetFractionCompleted(), 0.001)
	ttlProgress = progress.GetRowLevelTTL()
	require.Equal(t, int64(200), ttlProgress.JobTotalSpanCount)
	require.Equal(t, int64(200), ttlProgress.JobProcessedSpanCount)
	require.Equal(t, int64(1000), ttlProgress.JobDeletedRowCount)
	require.Len(t, ttlProgress.ProcessorProgresses, 2)
}
