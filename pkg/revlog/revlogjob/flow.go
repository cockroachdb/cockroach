// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Run plans and executes the revlog DistSQL flow. The (current) span
// set returned by resolveSpans is partitioned across SQL instances
// via dsp.PartitionSpans (the same primitive backup uses), and one
// producer processor is planned on each instance that received any
// spans, watching only its assigned subset. Per-flush metadata
// streams back to the gateway, where this function decodes each
// entry and routes it to a TickManager that aggregates checkpoints
// across the union of all producer subsets and writes manifests as
// each tick's frontier crosses its end.
//
// resolveSpans is the seam for backup-side scope logic: Run calls it
// once at startup (with changedDescIDs=nil) to learn the initial
// span set, and a future descriptor-rangefeed-driven coordinator
// will re-call it on each schema change. See SpanResolver.
//
// ptsTarget describes the keyspace covered by the writer's
// self-managed protected timestamp record (see pts.go). It is the
// caller's responsibility to construct a target that matches the
// resolveSpans coverage; revlogjob does not derive one because the
// codec/tenant context lives outside this package.
//
// v1 simplifications:
//
//   - resolveSpans is invoked exactly once at startup; mid-job
//     coverage changes are TODO.
//   - startHLC, dest, and tickWidth come from the caller. A real
//     backup-job resumer would derive these from BackupDetails
//     (TODO).
//   - No persistence / resumption (TODO).
func Run(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	resolveSpans DescSpanResolver,
	startHLC hlc.Timestamp,
	dest string,
	tickWidth time.Duration,
	ptsTarget *ptpb.Target,
) error {
	if resolveSpans == nil {
		return errors.AssertionFailedf("revlogjob.Run: resolveSpans must be non-nil")
	}
	spans, err := resolveSpans(ctx, startHLC, nil /* changedDescIDs */)
	if err != nil {
		return errors.Wrap(err, "resolving initial span set")
	}
	if len(spans) == 0 {
		return errors.AssertionFailedf("revlogjob.Run: resolveSpans returned no spans")
	}

	// The gateway-side TickManager writes manifests as the
	// flushed-frontier reported by producers crosses each tick
	// boundary.
	parsedDest, err := cloud.ExternalStorageConfFromURI(dest, username.RootUserName())
	if err != nil {
		return errors.Wrap(err, "parsing destination URI")
	}
	es, err := execCtx.ExecCfg().DistSQLSrv.ExternalStorage(ctx, parsedDest)
	if err != nil {
		return errors.Wrap(err, "opening destination storage")
	}
	defer es.Close()

	manager, err := NewTickManager(es, spans, startHLC, tickWidth)
	if err != nil {
		return err
	}

	// Load the job record so the progress-updater goroutine (started
	// below) can call Job.Update to publish HighWater. A failure to
	// load is fatal — without a job row to update, the writer would
	// run blind to operators and resume.
	job, err := execCtx.ExecCfg().JobRegistry.LoadJob(ctx, jobID)
	if err != nil {
		return errors.Wrapf(err, "loading revlog job %d", jobID)
	}

	// Install the v1 self-managed PTS record before any writer-side
	// work begins, so we never run with the rangefeed open and
	// nothing protecting the data we're about to read. The record's
	// UUID is persisted onto the sibling job's BackupDetails by
	// install — the existing BACKUP OnFailOrCancel uses that field
	// to release the record on teardown, so we deliberately do not
	// add a release call in this package. See pts.go.
	pts := newPTSManager(
		job, execCtx.ExecCfg().ProtectedTimestampProvider,
		execCtx.ExecCfg().InternalDB, ptsTarget, startHLC,
	)
	if err := pts.install(ctx); err != nil {
		return errors.Wrap(err, "installing protected timestamp")
	}
	manager.SetAfterFrontierAdvance(pts.advance)

	// Start the periodic progress-updater. It runs for the duration
	// of the DistSQL flow and exits when progressCtx is cancelled
	// either by the deferred cleanup below or by parent ctx
	// cancellation (job pause / cancel / fail). It only reads from
	// manager (via LastClosed); it never mutates flow state, so
	// it's safe to run concurrently with the DistSQL flow.
	progressCtx, cancelProgress := context.WithCancel(ctx)
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		if err := runProgressUpdater(progressCtx, job, manager); err != nil {
			log.Dev.Warningf(ctx, "revlogjob: progress updater exited with error: %v", err)
		}
	}()
	defer func() {
		cancelProgress()
		<-progressDone
	}()

	dsp := execCtx.DistSQLPlanner()
	planCtx, _, err := dsp.SetupAllNodesPlanning(
		ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg(),
	)
	if err != nil {
		return err
	}

	// Partition the span set across SQL instances so each producer
	// only opens a rangefeed over (and writes files for) its assigned
	// subset. PartitionSpans returns one entry per instance that
	// received any spans, so instances with no leaseholders for any
	// of these spans get no producer.
	partitions, err := dsp.PartitionSpans(ctx, planCtx, spans, sql.PartitionSpansBoundDefault)
	if err != nil {
		return errors.Wrap(err, "partitioning spans across producers")
	}
	if len(partitions) == 0 {
		return errors.AssertionFailedf(
			"revlogjob.Run: span partitioning yielded no producers for %d spans", len(spans))
	}

	plan := planCtx.NewPhysicalPlan()
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(partitions))
	for i, part := range partitions {
		corePlacement[i].SQLInstanceID = part.SQLInstanceID
		corePlacement[i].Core.Revlog = &execinfrapb.RevlogSpec{
			JobID:          jobID,
			Spans:          part.Spans,
			StartHLC:       startHLC,
			Dest:           dest,
			TickWidthNanos: int64(tickWidth),
		}
	}
	plan.AddNoInputStage(
		corePlacement, execinfrapb.PostProcessSpec{}, []*types.T{},
		execinfrapb.Ordering{}, nil, /* finalizeLastStageCb */
	)
	sql.FinalizePlan(ctx, planCtx, plan)

	res := sql.NewMetadataOnlyMetadataCallbackWriter(
		func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
			return handleProducerMetadata(ctx, manager, meta)
		},
	)
	recv := sql.MakeDistSQLReceiver(
		ctx, res, tree.Ack,
		nil, /* rangeCache */
		nil, /* txn */
		nil, /* clockUpdater */
		execCtx.ExtendedEvalContext().Tracing,
	)
	defer recv.Release()

	evalCtxCopy := execCtx.ExtendedEvalContext().Context.Copy()
	dsp.Run(ctx, planCtx, nil /* txn */, plan, recv, evalCtxCopy, nil /* finishedSetupFn */)
	return res.Err()
}

// handleProducerMetadata routes one ProducerMetadata into the
// manager. Non-progress metadata (e.g. tracing) is ignored; the
// flow's standard metadata handling has already done what's
// appropriate with it.
func handleProducerMetadata(
	ctx context.Context, manager *TickManager, meta *execinfrapb.ProducerMetadata,
) error {
	if meta == nil || meta.BulkProcessorProgress == nil {
		return nil
	}
	flush, err := DecodeFlush(meta.BulkProcessorProgress.ProgressDetails)
	if err != nil {
		return err
	}
	return manager.Flush(ctx, flush)
}
