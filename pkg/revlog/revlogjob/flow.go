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
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

// Run plans and executes the revlog DistSQL flow.
//
// One descriptor rangefeed runs across the full job lifetime,
// driving schema-delta and coverage writes and signalling scope
// transitions. Inside that, an outer loop runs one DistSQL flow
// per scope-span generation: each iteration partitions the
// current spans across SQL instances (one producer per instance),
// runs to completion, and on a widening signal cancels and
// re-plans with the new spans. The TickManager spans iterations,
// so the resume protocol (per-tick flushorder bumped above the
// prior incarnation's max) keeps per-key ordering intact across
// restarts. ErrScopeTerminated from the descfeed ends the loop
// successfully.
//
// ptsTarget describes the keyspace covered by the writer's
// self-managed protected timestamp record (see pts.go); it's the
// caller's responsibility to construct one matching the scope.
func Run(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	scope Scope,
	startHLC hlc.Timestamp,
	dest string,
	tickWidth time.Duration,
	ptsTarget *ptpb.Target,
) error {
	if scope == nil {
		return errors.AssertionFailedf("revlogjob.Run: scope must be non-nil")
	}
	spans, err := scope.Spans(ctx, startHLC)
	if err != nil {
		return errors.Wrap(err, "resolving initial span set")
	}
	if len(spans) == 0 {
		return errors.AssertionFailedf("revlogjob.Run: scope.Spans returned no spans")
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

	// One coverage entry effective at startHLC so readers can
	// resolve "what was watched at T?" for any T >= startHLC even
	// before any descriptor change writes an incremental entry.
	if err := writeInitialCoverage(ctx, es, scope, spans, startHLC); err != nil {
		return errors.Wrap(err, "writing initial coverage manifest")
	}

	// Load the job record so the PTS manager (below) can persist
	// the PTS record's UUID onto BackupDetails. Job state itself
	// (frontier, open-tick file lists, high-water) goes through
	// jobPersister, not Job.Update.
	job, err := execCtx.ExecCfg().JobRegistry.LoadJob(ctx, jobID)
	if err != nil {
		return errors.Wrapf(err, "loading revlog job %d", jobID)
	}

	persister := newJobPersister(jobID, execCtx.ExecCfg().InternalDB)
	loaded, found, err := persister.Load(ctx)
	if err != nil {
		return errors.Wrap(err, "loading revlogjob checkpoint")
	}
	if found {
		log.Dev.Infof(ctx,
			"revlogjob: resuming from checkpoint (high-water %s, %d open ticks)",
			loaded.HighWater, len(loaded.OpenTicks))
		if err := manager.Rehydrate(loaded); err != nil {
			return errors.Wrap(err, "rehydrating revlogjob state")
		}
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

	// Start the periodic checkpoint loop. It runs for the duration
	// of the DistSQL flow and exits when checkpointCtx is cancelled
	// either by the deferred cleanup below or by parent ctx
	// cancellation (job pause / cancel / fail). It only reads from
	// manager (via Snapshot); it never mutates flow state, so it's
	// safe to run concurrently with the DistSQL flow.
	checkpointCtx, cancelCheckpoint := context.WithCancel(ctx)
	checkpointDone := make(chan struct{})
	go func() {
		defer close(checkpointDone)
		if err := runCheckpointer(checkpointCtx, persister, manager); err != nil {
			log.Dev.Warningf(ctx, "revlogjob: checkpointer exited with error: %v", err)
		}
	}()
	defer func() {
		cancelCheckpoint()
		<-checkpointDone
	}()

	// descfeed runs across the entire job lifetime; it spans the
	// inner-flow restarts that scope widening triggers. termCh
	// closes on clean termination; sigs.replan delivers the new
	// span set on widening.
	descCtx, cancelDescFeed := context.WithCancel(ctx)
	defer cancelDescFeed()
	sigs := newDescFeedSignals()
	termCh := make(chan struct{})
	// descErrCh signals an unexpected descfeed exit to the outer
	// loop. Buffered so the descfeed goroutine never blocks on it
	// even if the outer loop has already returned.
	descErrCh := make(chan error, 1)
	descDone := make(chan struct{})
	go func() {
		defer close(descDone)
		descSource := newFactoryDescRangefeedSource(
			execCtx.ExecCfg().RangeFeedFactory, execCtx.ExecCfg().Codec,
		)
		err := runDescFeed(
			descCtx, descSource, execCtx.ExecCfg().Codec,
			scope, manager, es, startHLC, spans, sigs,
		)
		switch {
		case err == nil, errors.Is(err, context.Canceled):
		case errors.Is(err, ErrScopeTerminated):
			log.Dev.Infof(ctx, "revlogjob: scope terminated, exiting cleanly")
			close(termCh)
		default:
			// Surface to the outer loop so the job fails (and is
			// retried by the jobs system) instead of silently
			// hanging: descFrontier would stall, blocking all
			// future tick closes.
			log.Dev.Warningf(ctx, "revlogjob: descfeed exited with error: %v", err)
			descErrCh <- err
		}
	}()
	defer func() { <-descDone }()

	// Outer flow loop. Each iteration plans and runs a DistSQL
	// flow over the current span set with a chosen start ts.
	// Termination ends the loop; a widening signal cancels the
	// running flow, picks up the new spans + start ts, and
	// re-plans. The TickManager and persister are shared across
	// iterations so per-key ordering survives via the resume
	// protocol (ResumeStateForPartition).
	currentSpans := spans
	currentStartTS := startHLC
	for {
		flowCtx, cancelFlow := context.WithCancel(ctx)
		flowDone := make(chan error, 1)
		go func() {
			flowDone <- runOneFlow(
				flowCtx, execCtx, jobID, manager, currentSpans, currentStartTS, dest, tickWidth,
			)
		}()

		select {
		case <-termCh:
			cancelFlow()
			<-flowDone
			return nil
		case err := <-descErrCh:
			cancelFlow()
			<-flowDone
			return errors.Wrap(err, "descfeed failed")
		case sig := <-sigs.replan:
			cancelFlow()
			<-flowDone
			log.Dev.Infof(ctx,
				"revlogjob: replanning flow at %s with %d spans (was %d, %d new)",
				sig.StartTS, len(sig.Spans), len(currentSpans), len(sig.NewSpans))
			currentSpans = sig.Spans
			currentStartTS = sig.StartTS
			// continue outer loop
		case err := <-flowDone:
			cancelFlow()
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			// flow exited cleanly without a termination or
			// replan signal; treat as parent-ctx cancellation.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return nil
		case <-ctx.Done():
			cancelFlow()
			<-flowDone
			return ctx.Err()
		}
	}
}

// runOneFlow plans and runs one DistSQL flow over spans. Returns
// when ctx is cancelled or the flow exits on its own. flowStartTS
// is the rangefeed subscription start time the producers will use,
// which on widening replans is min(currentDataFrontier,
// earliestWideningTs) — strictly less than or equal to the job's
// original startHLC for an unchanged-scope flow.
func runOneFlow(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	manager *TickManager,
	spans []roachpb.Span,
	flowStartTS hlc.Timestamp,
	dest string,
	tickWidth time.Duration,
) error {
	dsp := execCtx.DistSQLPlanner()
	planCtx, _, err := dsp.SetupAllNodesPlanning(
		ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg(),
	)
	if err != nil {
		return err
	}

	partitions, err := dsp.PartitionSpans(ctx, planCtx, spans, sql.PartitionSpansBoundDefault)
	if err != nil {
		return errors.Wrap(err, "partitioning spans across producers")
	}
	if len(partitions) == 0 {
		return errors.AssertionFailedf(
			"revlogjob.runOneFlow: span partitioning yielded no producers for %d spans", len(spans))
	}

	// Snapshot the manager once so every producer in this iteration
	// sees a consistent ResumeState — and so each iteration after a
	// widening starts new producers' per-tick flushorder above any
	// prior incarnation's contributions to the still-open tick.
	resumeBase, err := manager.Snapshot()
	if err != nil {
		return errors.Wrap(err, "snapshotting manager for producer resume")
	}

	plan := planCtx.NewPhysicalPlan()
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(partitions))
	for i, part := range partitions {
		spec := &execinfrapb.RevlogSpec{
			JobID:          jobID,
			Spans:          part.Spans,
			StartHLC:       flowStartTS,
			Dest:           dest,
			TickWidthNanos: int64(tickWidth),
		}
		resumeToSpec(spec, ResumeStateForPartition(resumeBase, part.Spans))
		corePlacement[i].SQLInstanceID = part.SQLInstanceID
		corePlacement[i].Core.Revlog = spec
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

// writeInitialCoverage writes one log/coverage/<startHLC> entry
// describing the scope's resolved span set at job startup. On
// resume the same-HLC entry is overwritten with the same content
// (or rejected by WORM-strict storage; either way the persisted
// state stays correct).
func writeInitialCoverage(
	ctx context.Context,
	es cloud.ExternalStorage,
	scope Scope,
	spans []roachpb.Span,
	startHLC hlc.Timestamp,
) error {
	return revlog.WriteCoverage(ctx, es, revlogpb.Coverage{
		EffectiveFrom: startHLC,
		Scope:         scope.String(),
		Spans:         spans,
	})
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
