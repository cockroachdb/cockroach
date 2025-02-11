// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/backfiller"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// IndexBackfillerMergePlanner holds dependencies for the merge step of the
// index backfiller.
type IndexBackfillerMergePlanner struct {
	execCfg *ExecutorConfig
}

// NewIndexBackfillerMergePlanner creates a new IndexBackfillerMergePlanner.
func NewIndexBackfillerMergePlanner(execCfg *ExecutorConfig) *IndexBackfillerMergePlanner {
	return &IndexBackfillerMergePlanner{execCfg: execCfg}
}

// MergeIndexes is part of the scexec.Merger interface.
func (im *IndexBackfillerMergePlanner) MergeIndexes(
	ctx context.Context,
	job *jobs.Job,
	progress scexec.MergeProgress,
	tracker scexec.BackfillerProgressWriter,
	descriptor catalog.TableDescriptor,
) (err error) {
	spansToDo := make([][]roachpb.Span, len(progress.SourceIndexIDs))
	if len(progress.CompletedSpans) != len(progress.SourceIndexIDs) {
		return errors.AssertionFailedf("invalid MergeProgress, CompletedSpans should " +
			"be parallel to SourceIndexIDs")
	}
	var hasToDo bool
	for i, sourceID := range progress.SourceIndexIDs {
		sourceIndexSpan := descriptor.IndexSpan(im.execCfg.Codec, sourceID)
		var g roachpb.SpanGroup
		g.Add(sourceIndexSpan)
		g.Sub(progress.CompletedSpans[i]...)
		spansToDo[i] = g.Slice()
		if len(spansToDo) > 0 {
			hasToDo = true
		}
	}
	if !hasToDo { // already done
		return nil
	}
	completed := struct {
		syncutil.Mutex
		g []roachpb.SpanGroup
	}{
		g: make([]roachpb.SpanGroup, len(progress.SourceIndexIDs)),
	}
	addCompleted := func(idxs []int32, spans []roachpb.Span) (ret [][]roachpb.Span) {
		completed.Lock()
		defer completed.Unlock()
		for i, idx := range idxs {
			completed.g[idx].Add(spans[i])
		}
		for _, g := range completed.g {
			ret = append(ret, g.Slice())
		}
		return ret
	}
	updateFunc := func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress == nil {
			return nil
		}
		progress.CompletedSpans = addCompleted(
			meta.BulkProcessorProgress.CompletedSpanIdx,
			meta.BulkProcessorProgress.CompletedSpans,
		)
		return tracker.SetMergeProgress(ctx, progress)
	}
	mergeTimeStamp := getMergeTimestamp(im.execCfg.Clock)
	protectedTimestampCleaner := im.execCfg.ProtectedTimestampManager.TryToProtectBeforeGC(ctx, job, descriptor, mergeTimeStamp)
	defer func() {
		cleanupError := protectedTimestampCleaner(ctx)
		if cleanupError != nil {
			err = errors.CombineErrors(cleanupError, err)
		}
	}()

	run, err := im.plan(
		ctx,
		descriptor,
		spansToDo,
		progress.DestIndexIDs,
		progress.SourceIndexIDs,
		updateFunc,
		mergeTimeStamp,
	)
	if err != nil {
		return err
	}
	return run(ctx)
}

func (im *IndexBackfillerMergePlanner) plan(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	todoSpanList [][]roachpb.Span,
	addedIndexes, temporaryIndexes []descpb.IndexID,
	metaFn func(_ context.Context, meta *execinfrapb.ProducerMetadata) error,
	mergeTimestamp hlc.Timestamp,
) (func(context.Context) error, error) {
	var p *PhysicalPlan
	var evalCtx extendedEvalContext
	var planCtx *PlanningCtx

	if err := DescsTxn(ctx, im.execCfg, func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) error {
		sd := NewInternalSessionData(ctx, im.execCfg.Settings, "plan-index-backfill-merge")
		evalCtx = createSchemaChangeEvalCtx(ctx, im.execCfg, sd, txn.KV().ReadTimestamp(), descriptors)
		planCtx = im.execCfg.DistSQLPlanner.NewPlanningCtx(
			ctx, &evalCtx, nil /* planner */, txn.KV(), FullDistribution,
		)

		spec, err := initIndexBackfillMergerSpec(*tableDesc.TableDesc(), addedIndexes, temporaryIndexes, mergeTimestamp)
		if err != nil {
			return err
		}
		p, err = im.execCfg.DistSQLPlanner.createIndexBackfillerMergePhysicalPlan(ctx, planCtx, spec, todoSpanList)
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
		)
		defer recv.Release()
		evalCtxCopy := evalCtx
		im.execCfg.DistSQLPlanner.Run(
			ctx,
			planCtx,
			nil, /* txn - the processors manage their own transactions */
			p, recv, &evalCtxCopy,
			nil, /* finishedSetupFn */
		)
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

// Copy returns a copy of this MergeProcess. Note that roachpb.Span's
// aren't deep copied.
func (mp *MergeProgress) Copy() *MergeProgress {
	newp := &MergeProgress{
		TodoSpans:        make([][]roachpb.Span, len(mp.TodoSpans)),
		MutationIdx:      make([]int, len(mp.MutationIdx)),
		AddedIndexes:     make([]descpb.IndexID, len(mp.AddedIndexes)),
		TemporaryIndexes: make([]descpb.IndexID, len(mp.TemporaryIndexes)),
	}
	copy(newp.MutationIdx, mp.MutationIdx)
	copy(newp.AddedIndexes, mp.AddedIndexes)
	copy(newp.TemporaryIndexes, mp.TemporaryIndexes)
	for i, spanSlice := range mp.TodoSpans {
		newSpanSlice := make([]roachpb.Span, len(spanSlice))
		copy(newSpanSlice, spanSlice)
		newp.TodoSpans[i] = newSpanSlice
	}
	return newp
}

// FlatSpans returns all of the TodoSpans being tracked by this merger
// as a flat slice.
func (mp *MergeProgress) FlatSpans() []roachpb.Span {
	spans := []roachpb.Span{}
	for _, s := range mp.TodoSpans {
		spans = append(spans, s...)
	}
	return spans
}

// IndexMergeTracker abstracts the infrastructure to read and write merge
// progress to job state.
type IndexMergeTracker struct {
	mu struct {
		syncutil.Mutex
		progress *MergeProgress

		hasOrigNRanges bool
		origNRanges    int
	}

	jobMu struct {
		syncutil.Mutex
		job *jobs.Job
	}

	rangeCounter   rangeCounter
	fractionScaler *multiStageFractionScaler
}

var _ scexec.BackfillerProgressFlusher = (*IndexMergeTracker)(nil)

type rangeCounter func(ctx context.Context, spans []roachpb.Span) (int, error)

// NewIndexMergeTracker creates a new IndexMergeTracker
func NewIndexMergeTracker(
	progress *MergeProgress,
	job *jobs.Job,
	rangeCounter rangeCounter,
	scaler *multiStageFractionScaler,
) *IndexMergeTracker {
	imt := IndexMergeTracker{
		rangeCounter:   rangeCounter,
		fractionScaler: scaler,
	}
	imt.mu.hasOrigNRanges = false
	imt.mu.progress = progress.Copy()
	imt.jobMu.job = job
	return &imt
}

// FlushCheckpoint writes out a checkpoint containing any data which
// has been previously updated via UpdateMergeProgress.
func (imt *IndexMergeTracker) FlushCheckpoint(ctx context.Context) error {
	imt.jobMu.Lock()
	defer imt.jobMu.Unlock()

	progress := func() *MergeProgress {
		imt.mu.Lock()
		defer imt.mu.Unlock()
		if imt.mu.progress.TodoSpans == nil {
			return nil
		}
		return imt.mu.progress.Copy()
	}()
	if progress == nil {
		return nil
	}

	details, ok := imt.jobMu.job.Details().(jobspb.SchemaChangeDetails)
	if !ok {
		return errors.Errorf("expected SchemaChangeDetails job type, got %T", imt.jobMu.job.Details())
	}

	for idx := range progress.TodoSpans {
		details.ResumeSpanList[progress.MutationIdx[idx]].ResumeSpans = progress.TodoSpans[idx]
	}

	return imt.jobMu.job.NoTxn().SetDetails(ctx, details)
}

// FlushFractionCompleted writes out the fraction completed based on the number of total
// ranges completed.
func (imt *IndexMergeTracker) FlushFractionCompleted(ctx context.Context) error {
	spans := func() []roachpb.Span {
		imt.mu.Lock()
		defer imt.mu.Unlock()
		return imt.mu.progress.FlatSpans()
	}()

	rangeCount, err := imt.rangeCounter(ctx, spans)
	if err != nil {
		return err
	}

	orig := imt.maybeSetOrigNRanges(rangeCount)
	if orig >= rangeCount && orig != 0 {
		fractionRangesFinished := float32(orig-rangeCount) / float32(orig)
		frac, err := imt.fractionScaler.fractionCompleteFromStageFraction(stageMerge, fractionRangesFinished)
		if err != nil {
			return err
		}

		imt.jobMu.Lock()
		defer imt.jobMu.Unlock()
		if err := imt.jobMu.job.NoTxn().FractionProgressed(
			ctx, jobs.FractionUpdater(frac),
		); err != nil {
			return jobs.SimplifyInvalidStateError(err)
		}
	}
	return nil
}

// maybeSetOrigNRanges sets the initial range count, if it wasn't
// previously set. The updated value of the range count is returned.
func (imt *IndexMergeTracker) maybeSetOrigNRanges(count int) int {
	imt.mu.Lock()
	defer imt.mu.Unlock()
	if !imt.mu.hasOrigNRanges {
		imt.mu.hasOrigNRanges = true
		imt.mu.origNRanges = count
	}
	return imt.mu.origNRanges
}

// UpdateMergeProgress allow the caller to modify the current progress with updateFn.
func (imt *IndexMergeTracker) UpdateMergeProgress(
	ctx context.Context, updateFn func(ctx context.Context, progress *MergeProgress),
) {
	imt.mu.Lock()
	defer imt.mu.Unlock()
	updateFn(ctx, imt.mu.progress)
}

func newPeriodicProgressFlusher(settings *cluster.Settings) scexec.PeriodicProgressFlusher {
	return backfiller.NewPeriodicProgressFlusher(
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
