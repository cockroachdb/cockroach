// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// IndexBackfillerMergePlanner holds dependencies for the merge step of the
// index backfiller.
type IndexBackfillerMergePlanner struct {
	execCfg   *ExecutorConfig
	ieFactory sqlutil.SessionBoundInternalExecutorFactory
}

// NewIndexBackfillerMergePlanner creates a new IndexBackfillerMergePlanner.
func NewIndexBackfillerMergePlanner(
	execCfg *ExecutorConfig, ieFactory sqlutil.SessionBoundInternalExecutorFactory,
) *IndexBackfillerMergePlanner {
	return &IndexBackfillerMergePlanner{execCfg: execCfg, ieFactory: ieFactory}
}

func (im *IndexBackfillerMergePlanner) plan(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	todoSpanList [][]roachpb.Span,
	addedIndexes, temporaryIndexes []descpb.IndexID,
	metaFn func(_ context.Context, meta *execinfrapb.ProducerMetadata) error,
) (func(context.Context) error, error) {
	var p *PhysicalPlan
	var evalCtx extendedEvalContext
	var planCtx *PlanningCtx

	if err := DescsTxn(ctx, im.execCfg, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		evalCtx = createSchemaChangeEvalCtx(ctx, im.execCfg, txn.ReadTimestamp(), descriptors)
		planCtx = im.execCfg.DistSQLPlanner.NewPlanningCtx(ctx, &evalCtx, nil /* planner */, txn,
			true /* distribute */)
		chunkSize := indexBackfillBatchSize.Get(&im.execCfg.Settings.SV)

		spec, err := initIndexBackfillMergerSpec(*tableDesc.TableDesc(), chunkSize, addedIndexes, temporaryIndexes)
		if err != nil {
			return err
		}
		p, err = im.execCfg.DistSQLPlanner.createIndexBackfillerMergePhysicalPlan(planCtx, spec, todoSpanList)
		return err
	}); err != nil {
		return nil, err
	}

	return func(ctx context.Context) error {
		cbw := MetadataCallbackWriter{rowResultWriter: &errOnlyResultWriter{}, fn: metaFn}
		recv := MakeDistSQLReceiver(
			ctx,
			&cbw,
			tree.Rows, /* stmtType - doesn't matter here since no result are produced */
			im.execCfg.RangeDescriptorCache,
			nil, /* txn - the flow does not run wholly in a txn */
			im.execCfg.Clock,
			evalCtx.Tracing,
			im.execCfg.ContentionRegistry,
			nil, /* testingPushCallback */
		)
		defer recv.Release()
		evalCtxCopy := evalCtx
		im.execCfg.DistSQLPlanner.Run(
			planCtx,
			nil, /* txn - the processors manage their own transactions */
			p, recv, &evalCtxCopy,
			nil, /* finishedSetupFn */
		)()
		return cbw.Err()
	}, nil
}

// MergeProgress tracks the progress for an index backfill merge.
type MergeProgress struct {
	// TodoSpans contains the all the spans for all the temporary
	// indexes that still need to be merged.
	TodoSpans [][]roachpb.Span

	// MutationIdx contains the indexes of the mutations for the
	// temporary indexes in the list of mutations.
	MutationIdx []int

	// AddedIndexes and TemporaryIndexes contain the index IDs for
	// all newly added indexes and their corresponding temporary
	// index.
	AddedIndexes, TemporaryIndexes []descpb.IndexID
}

// IndexMergeTracker abstracts the infrastructure to read and write merge
// progress to job state.
type IndexMergeTracker struct {
	job *jobs.Job

	mu struct {
		syncutil.Mutex
		progress *MergeProgress
	}
}

var _ scexec.BackfillProgressFlusher = (*IndexMergeTracker)(nil)

// NewIndexMergeTracker creates a new IndexMergeTracker
func NewIndexMergeTracker(progress *MergeProgress, job *jobs.Job) *IndexMergeTracker {
	imt := IndexMergeTracker{}
	imt.mu.progress = progress
	imt.job = job
	return &imt
}

// FlushCheckpoint writes out a checkpoint containing any data which has been
// previously set via SetMergeProgress
func (imt *IndexMergeTracker) FlushCheckpoint(ctx context.Context) error {
	progress := imt.GetMergeProgress()

	if progress.TodoSpans == nil {
		return nil
	}

	details, ok := imt.job.Details().(jobspb.SchemaChangeDetails)
	if !ok {
		return errors.Errorf("expected SchemaChangeDetails job type, got %T", imt.job.Details())
	}

	for idx := range progress.TodoSpans {
		details.ResumeSpanList[progress.MutationIdx[idx]].ResumeSpans = progress.TodoSpans[idx]
	}

	return imt.job.SetDetails(ctx, nil, details)
}

// FlushFractionCompleted writes out the fraction completed.
func (imt *IndexMergeTracker) FlushFractionCompleted(ctx context.Context) error {
	// TODO(rui): The backfiller currently doesn't have a good way to report the
	// total progress of mutations that occur in multiple stages that
	// independently report progress. So fraction tracking of the merge will be
	// unimplemented for now and the progress fraction will report only the
	// progress of the backfilling stage.
	return nil
}

// SetMergeProgress sets the progress for all index merges. Setting the progress
// does not make that progress durable as the tracker may invoke FlushCheckpoint
// later.
func (imt *IndexMergeTracker) SetMergeProgress(ctx context.Context, progress *MergeProgress) {
	imt.mu.Lock()
	imt.mu.progress = progress
	imt.mu.Unlock()
}

// GetMergeProgress reads the current merge progress.
func (imt *IndexMergeTracker) GetMergeProgress() *MergeProgress {
	imt.mu.Lock()
	defer imt.mu.Unlock()
	return imt.mu.progress
}

func newPeriodicProgressFlusher(settings *cluster.Settings) scexec.PeriodicProgressFlusher {
	return scdeps.NewPeriodicProgressFlusher(
		func() time.Duration {
			return backfill.IndexBackfillCheckpointInterval.Get(&settings.SV)
		},
		func() time.Duration {
			// fractionInterval is copied from the logic in existing backfill code.
			const fractionInterval = 10 * time.Second
			return fractionInterval
		},
	)
}
