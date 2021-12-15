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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// IndexBackfillPlanner holds dependencies for an index backfiller
// for use in the declarative schema changer.
type IndexBackfillPlanner struct {
	execCfg   *ExecutorConfig
	ieFactory sqlutil.SessionBoundInternalExecutorFactory
}

// NewIndexBackfiller creates a new IndexBackfillPlanner.
func NewIndexBackfiller(
	execCfg *ExecutorConfig, ieFactory sqlutil.SessionBoundInternalExecutorFactory,
) *IndexBackfillPlanner {
	return &IndexBackfillPlanner{execCfg: execCfg, ieFactory: ieFactory}
}

// MaybePrepareDestIndexesForBackfill is part of the scexec.Backfiller interface.
func (ib *IndexBackfillPlanner) MaybePrepareDestIndexesForBackfill(
	ctx context.Context, progress scexec.BackfillProgress, td catalog.TableDescriptor,
) (scexec.BackfillProgress, error) {
	bf := progress
	if err := scanDestIndexesBeforeBackfill(
		ctx, ib.execCfg.Codec, ib.execCfg.DB, td, &bf,
	); err != nil {
		return scexec.BackfillProgress{}, err
	}
	return bf, nil
}

// BackfillIndex is part of the scexec.Backfiller interface.
func (ib *IndexBackfillPlanner) BackfillIndex(
	ctx context.Context,
	progress scexec.BackfillProgress,
	tracker scexec.BackfillProgressWriter,
	descriptor catalog.TableDescriptor,
) error {
	updateFunc := func(
		ctx context.Context, meta *execinfrapb.ProducerMetadata,
	) error {
		if meta.BulkProcessorProgress == nil {
			return nil
		}
		progress.SpansToDo = roachpb.SubtractSpans(progress.SpansToDo,
			meta.BulkProcessorProgress.CompletedSpans)
		return tracker.SetBackfillProgress(ctx, progress)
	}
	now := ib.execCfg.DB.Clock().Now()
	run, err := ib.plan(
		ctx,
		descriptor,
		progress.MinimumWriteTimestamp,
		now,
		progress.MinimumWriteTimestamp,
		progress.SpansToDo,
		progress.DestIndexIDs,
		updateFunc,
	)
	if err != nil {
		return err
	}
	return run(ctx)
}

func scanDestIndexesBeforeBackfill(
	ctx context.Context,
	codec keys.SQLCodec,
	db *kv.DB,
	descriptor catalog.TableDescriptor,
	backfillProgress *scexec.BackfillProgress,
) error {
	if !backfillProgress.MinimumWriteTimestamp.IsEmpty() {
		return nil
	}
	// Pick an arbitrary read timestamp for the reads of the backfill.
	// It's safe to use any timestamp to read even if we've partially backfilled
	// at an earlier timestamp because other writing transactions have been
	// writing at the appropriate timestamps in-between.
	backfillReadTimestamp := db.Clock().Now()
	targetSpans := make([]roachpb.Span, len(backfillProgress.DestIndexIDs))
	for i, idxID := range backfillProgress.DestIndexIDs {
		targetSpans[i] = descriptor.IndexSpan(codec, idxID)
	}
	if err := scanTargetSpansToPushTimestampCache(
		ctx, db, backfillReadTimestamp, targetSpans,
	); err != nil {
		return err
	}

	backfillProgress.MinimumWriteTimestamp = backfillReadTimestamp
	backfillProgress.SpansToDo = []roachpb.Span{
		descriptor.IndexSpan(codec, descriptor.GetPrimaryIndexID()),
	}
	return nil
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
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
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
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		evalCtx = createSchemaChangeEvalCtx(ctx, ib.execCfg, nowTimestamp, ib.ieFactory, descriptors)
		planCtx = ib.execCfg.DistSQLPlanner.NewPlanningCtx(ctx, &evalCtx, nil /* planner */, txn,
			true /* distribute */)
		// TODO(ajwerner): Adopt util.ConstantWithMetamorphicTestRange for the
		// batch size. Also plumb in a testing knob.
		chunkSize := indexBackfillBatchSize.Get(&ib.execCfg.Settings.SV)
		spec, err := initIndexBackfillerSpec(*td.TableDesc(), writeAsOf, readAsOf, chunkSize, indexesToBackfill)
		if err != nil {
			return err
		}
		p, err = ib.execCfg.DistSQLPlanner.createBackfillerPhysicalPlan(planCtx, spec, sourceSpans)
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
			ib.execCfg.ContentionRegistry,
			nil, /* testingPushCallback */
		)
		defer recv.Release()
		evalCtxCopy := evalCtx
		ib.execCfg.DistSQLPlanner.Run(planCtx, nil, p, recv, &evalCtxCopy, nil)()
		return cbw.Err()
	}, nil
}
