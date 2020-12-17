package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/executor"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type IndexBackfillPlanner struct {
	execCfg   *ExecutorConfig
	ieFactory sqlutil.SessionBoundInternalExecutorFactory
}

func NewIndexBackfiller(
	execCfg *ExecutorConfig, ieFactory sqlutil.SessionBoundInternalExecutorFactory,
) *IndexBackfillPlanner {
	return &IndexBackfillPlanner{execCfg: execCfg, ieFactory: ieFactory}
}

// BackfillIndex will backfill the specified index on the passed table.
//
// TODO(ajwerner): allow backfilling multiple indexes.
func (ib *IndexBackfillPlanner) BackfillIndex(
	ctx context.Context,
	tracker executor.JobProgressTracker,
	descriptor catalog.TableDescriptor,
	source descpb.IndexID,
	toBackfill ...descpb.IndexID,
) error {

	// Pick an arbitrary read timestamp for the reads of the backfill.
	// It's safe to use any timestamp to read even if we've partially backfilled
	// at an earlier timestamp because other writing transactions have been
	// writing at the appropriate timestamps in-between.
	backfillReadTimestamp := ib.execCfg.DB.Clock().Now()
	targetSpans := make([]roachpb.Span, len(toBackfill))
	for i, idxID := range toBackfill {
		targetSpans[i] = descriptor.IndexSpan(ib.execCfg.Codec, idxID)
	}
	if err := ib.scanTargetSpansToPushTimestampCache(
		ctx, backfillReadTimestamp, targetSpans,
	); err != nil {
		return err
	}

	resumeSpans, err := tracker.GetResumeSpans(ctx, descriptor.GetID(), source)
	if err != nil {
		return err
	}
	run, err := ib.plan(ctx, descriptor, backfillReadTimestamp, backfillReadTimestamp, resumeSpans, toBackfill, func(
		ctx context.Context, meta *execinfrapb.ProducerMetadata,
	) error {
		// TODO(ajwerner): Hook up the jobs tracking stuff.
		log.Infof(ctx, "got update: %v", meta)
		return nil
	})
	if err != nil {
		return err
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
func (i *IndexBackfillPlanner) scanTargetSpansToPushTimestampCache(
	ctx context.Context, backfillTimestamp hlc.Timestamp, targetSpans []roachpb.Span,
) error {
	const pageSize = 10000
	return i.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetFixedTimestamp(ctx, backfillTimestamp)
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

var _ executor.IndexBackfiller = (*IndexBackfillPlanner)(nil)

func (ip *IndexBackfillPlanner) plan(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	nowTimestamp, readAsOf hlc.Timestamp,
	spans []roachpb.Span,
	indexesToBackfill []descpb.IndexID,
	callback func(_ context.Context, meta *execinfrapb.ProducerMetadata) error,
) (runFunc func(context.Context) error, _ error) {

	var p *PhysicalPlan
	var evalCtx extendedEvalContext
	var planCtx *PlanningCtx
	if err := ip.execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		evalCtx = createSchemaChangeEvalCtx(ctx, ip.execCfg, nowTimestamp, ip.ieFactory)
		planCtx = ip.execCfg.DistSQLPlanner.NewPlanningCtx(ctx, &evalCtx, nil /* planner */, txn,
			true /* distribute */)
		// TODO(ajwerner): Adopt util.ConstantWithMetamorphicTestRange for the
		// batch size. Also plumb in a testing knob.
		spec, err := initIndexBackfillerSpec(
			*tableDesc.TableDesc(), readAsOf, indexBackfillBatchSize, indexesToBackfill)
		if err != nil {
			return err
		}
		p, err = ip.execCfg.DistSQLPlanner.createBackfillerPhysicalPlan(planCtx, spec, spans)
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
			ip.execCfg.RangeDescriptorCache,
			nil, /* txn - the flow does not run wholly in a txn */
			ip.execCfg.Clock,
			evalCtx.Tracing,
		)
		defer recv.Release()
		evalCtxCopy := evalCtx
		ip.execCfg.DistSQLPlanner.Run(planCtx, nil, p, recv, &evalCtxCopy, nil)()
		return cbw.Err()
	}, nil
}
