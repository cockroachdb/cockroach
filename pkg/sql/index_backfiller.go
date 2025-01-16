// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// IndexBackfillPlanner holds dependencies for an index backfiller
// for use in the declarative schema changer.
type IndexBackfillPlanner struct {
	execCfg *ExecutorConfig
}

// NewIndexBackfiller creates a new IndexBackfillPlanner.
func NewIndexBackfiller(execCfg *ExecutorConfig) *IndexBackfillPlanner {
	return &IndexBackfillPlanner{execCfg: execCfg}
}

// MaybePrepareDestIndexesForBackfill is part of the scexec.Backfiller interface.
func (ib *IndexBackfillPlanner) MaybePrepareDestIndexesForBackfill(
	ctx context.Context, current scexec.BackfillProgress, td catalog.TableDescriptor,
) (scexec.BackfillProgress, error) {
	if !current.MinimumWriteTimestamp.IsEmpty() {
		return current, nil
	}
	// Pick an arbitrary read timestamp for the reads of the backfill.
	// It's safe to use any timestamp to read even if we've partially backfilled
	// at an earlier timestamp because other writing transactions have been
	// writing at the appropriate timestamps in-between.
	backfillReadTimestamp := ib.execCfg.Clock.Now()
	targetSpans := make([]roachpb.Span, len(current.DestIndexIDs))
	for i, idxID := range current.DestIndexIDs {
		targetSpans[i] = td.IndexSpan(ib.execCfg.Codec, idxID)
	}
	if err := scanTargetSpansToPushTimestampCache(
		ctx, ib.execCfg.DB, backfillReadTimestamp, targetSpans,
	); err != nil {
		return scexec.BackfillProgress{}, err
	}
	return scexec.BackfillProgress{
		Backfill:              current.Backfill,
		MinimumWriteTimestamp: backfillReadTimestamp,
	}, nil
}

// BackfillIndexes is part of the scexec.Backfiller interface.
func (ib *IndexBackfillPlanner) BackfillIndexes(
	ctx context.Context,
	progress scexec.BackfillProgress,
	tracker scexec.BackfillerProgressWriter,
	job *jobs.Job,
	descriptor catalog.TableDescriptor,
) (retErr error) {
	// Potentially install a protected timestamp before the GC interval is hit,
	// which can help avoid transaction retry errors, with shorter GC intervals.
	protectedTimestampCleaner := ib.execCfg.ProtectedTimestampManager.TryToProtectBeforeGC(ctx,
		job,
		descriptor,
		progress.MinimumWriteTimestamp)
	defer func() {
		cleanupError := protectedTimestampCleaner(ctx)
		if cleanupError != nil {
			retErr = errors.CombineErrors(retErr, cleanupError)
		}
	}()

	var completed = struct {
		syncutil.Mutex
		g roachpb.SpanGroup
	}{}
	addCompleted := func(c ...roachpb.Span) []roachpb.Span {
		completed.Lock()
		defer completed.Unlock()
		completed.g.Add(c...)
		return completed.g.Slice()
	}
	updateFunc := func(
		ctx context.Context, meta *execinfrapb.ProducerMetadata,
	) error {
		if meta.BulkProcessorProgress == nil {
			return nil
		}
		progress.CompletedSpans = addCompleted(
			meta.BulkProcessorProgress.CompletedSpans...)
		return tracker.SetBackfillProgress(ctx, progress)
	}
	var spansToDo []roachpb.Span
	{
		sourceIndexSpan := descriptor.IndexSpan(ib.execCfg.Codec, progress.SourceIndexID)
		var g roachpb.SpanGroup
		g.Add(sourceIndexSpan)
		g.Sub(progress.CompletedSpans...)
		spansToDo = g.Slice()
	}
	if len(spansToDo) == 0 { // already done
		return nil
	}
	now := ib.execCfg.DB.Clock().Now()
	run, retErr := ib.plan(
		ctx,
		descriptor,
		now,
		progress.MinimumWriteTimestamp,
		progress.MinimumWriteTimestamp,
		spansToDo,
		progress.DestIndexIDs,
		updateFunc,
	)
	if retErr != nil {
		return retErr
	}
	return run(ctx)
}

// Index backfilling ingests SSTs that don't play nicely with running txns
// since they just add their keys blindly. Running a Scan of the target
// spans at the time the SSTs' keys will be written will calcify history up
// to then since the scan will resolve intents and populate tscache to keep
// anything else from sneaking under us. Since these are new indexes, these
// spans should be essentially empty, so this should be a pretty quick and
// cheap scan.
func scanTargetSpansToPushTimestampCache(
	ctx context.Context, db *kv.DB, backfillTimestamp hlc.Timestamp, targetSpans []roachpb.Span,
) error {
	const pageSize = 10000
	return db.TxnWithAdmissionControl(
		ctx, kvpb.AdmissionHeader_FROM_SQL, admissionpb.BulkNormalPri,
		kv.SteppingDisabled,
		func(
			ctx context.Context, txn *kv.Txn,
		) error {
			if err := txn.SetFixedTimestamp(ctx, backfillTimestamp); err != nil {
				return err
			}
			for _, span := range targetSpans {
				// TODO(dt): a Count() request would be nice here if the target isn't
				// empty, since we don't need to drag all the results back just to
				// then ignore them -- we just need the iteration on the far end.
				if err := txn.Iterate(ctx, span.Key, span.EndKey, pageSize, iterateNoop); err != nil {
					return err
				}
			}
			return nil
		})
}

func iterateNoop(_ []kv.KeyValue) error { return nil }

var _ scexec.Backfiller = (*IndexBackfillPlanner)(nil)

func (ib *IndexBackfillPlanner) plan(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	nowTimestamp, writeAsOf, readAsOf hlc.Timestamp,
	sourceSpans []roachpb.Span,
	indexesToBackfill []descpb.IndexID,
	callback func(_ context.Context, meta *execinfrapb.ProducerMetadata) error,
) (runFunc func(context.Context) error, _ error) {

	var p *PhysicalPlan
	var evalCtx extendedEvalContext
	var planCtx *PlanningCtx
	td := tabledesc.NewBuilder(tableDesc.TableDesc()).BuildExistingMutableTable()
	if err := DescsTxn(ctx, ib.execCfg, func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) error {
		sd := NewInternalSessionData(ctx, ib.execCfg.Settings, "plan-index-backfill")
		evalCtx = createSchemaChangeEvalCtx(ctx, ib.execCfg, sd, nowTimestamp, descriptors)
		planCtx = ib.execCfg.DistSQLPlanner.NewPlanningCtx(
			ctx, &evalCtx, nil /* planner */, txn.KV(), FullDistribution,
		)
		// TODO(ajwerner): Adopt metamorphic.ConstantWithTestRange for the
		// batch size. Also plumb in a testing knob.
		chunkSize := indexBackfillBatchSize.Get(&ib.execCfg.Settings.SV)
		const writeAtRequestTimestamp = true
		spec, err := initIndexBackfillerSpec(
			*td.TableDesc(), writeAsOf, readAsOf, writeAtRequestTimestamp, chunkSize,
			indexesToBackfill,
		)
		if err != nil {
			return err
		}
		p, err = ib.execCfg.DistSQLPlanner.createBackfillerPhysicalPlan(ctx, planCtx, spec, sourceSpans)
		return err
	}); err != nil {
		return nil, err
	}

	return func(ctx context.Context) error {
		cbw := MetadataCallbackWriter{rowResultWriter: &errOnlyResultWriter{}, fn: callback}
		recv := MakeDistSQLReceiver(
			ctx,
			&cbw,
			tree.Rows, /* stmtType - doesn't matter here since no result are produced */
			ib.execCfg.RangeDescriptorCache,
			nil, /* txn - the flow does not run wholly in a txn */
			ib.execCfg.Clock,
			evalCtx.Tracing,
		)
		defer recv.Release()
		evalCtxCopy := evalCtx
		ib.execCfg.DistSQLPlanner.Run(ctx, planCtx, nil, p, recv, &evalCtxCopy, nil)
		return cbw.Err()
	}, nil
}
