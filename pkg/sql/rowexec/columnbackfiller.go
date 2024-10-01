// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// columnBackfiller is a processor for backfilling columns.
type columnBackfiller struct {
	backfill.ColumnBackfiller

	desc catalog.TableDescriptor

	// mutationFilter returns true if the mutation should be processed by the
	// chunkBackfiller.
	filter backfill.MutationFilter

	spec        execinfrapb.BackfillerSpec
	flowCtx     *execinfra.FlowCtx
	processorID int32

	// commitWaitFns contains a set of functions, each of which was returned
	// from a call to (*kv.Txn).DeferCommitWait when backfilling a single chunk
	// of rows. The functions must be called to ensure consistency with any
	// causally dependent readers.
	commitWaitFns []func(context.Context) error
}

var _ execinfra.Processor = &columnBackfiller{}

func newColumnBackfiller(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.BackfillerSpec,
) (*columnBackfiller, error) {
	columnBackfillerMon := execinfra.NewMonitor(
		ctx, flowCtx.Cfg.BackfillerMonitor, "column-backfill-mon",
	)
	cb := &columnBackfiller{
		desc:        flowCtx.TableDescriptor(ctx, &spec.Table),
		filter:      backfill.ColumnMutationFilter,
		flowCtx:     flowCtx,
		processorID: processorID,
		spec:        spec,
	}
	if err := cb.ColumnBackfiller.InitForDistributedUse(
		ctx, flowCtx, cb.desc, columnBackfillerMon,
	); err != nil {
		return nil, err
	}
	return cb, nil
}

// OutputTypes is part of the execinfra.Processor interface.
func (*columnBackfiller) OutputTypes() []*types.T {
	// No output types.
	return nil
}

// MustBeStreaming is part of the execinfra.Processor interface.
func (*columnBackfiller) MustBeStreaming() bool {
	return false
}

// Run is part of the execinfra.Processor interface.
func (cb *columnBackfiller) Run(ctx context.Context, output execinfra.RowReceiver) {
	opName := "column backfiller"
	ctx = logtags.AddTag(ctx, opName, int(cb.spec.Table.ID))
	ctx, span := execinfra.ProcessorSpan(ctx, cb.flowCtx, opName, cb.processorID)
	defer span.Finish()
	meta := cb.doRun(ctx)
	execinfra.SendTraceData(ctx, cb.flowCtx, output)
	output.Push(nil /* row */, meta)
	output.ProducerDone()
}

// Resume is part of the execinfra.Processor interface.
func (*columnBackfiller) Resume(output execinfra.RowReceiver) {
	panic("not implemented")
}

// Close is part of the execinfra.Processor interface.
func (*columnBackfiller) Close(context.Context) {}

func (cb *columnBackfiller) doRun(ctx context.Context) *execinfrapb.ProducerMetadata {
	finishedSpans, err := cb.mainLoop(ctx)
	if err != nil {
		return &execinfrapb.ProducerMetadata{Err: err}
	}
	var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	prog.CompletedSpans = append(prog.CompletedSpans, finishedSpans...)
	return &execinfrapb.ProducerMetadata{BulkProcessorProgress: &prog}
}

// mainLoop invokes runChunk on chunks of rows.
// It does not close the output.
func (cb *columnBackfiller) mainLoop(ctx context.Context) (roachpb.Spans, error) {
	defer cb.ColumnBackfiller.Close(ctx)

	// As we approach the end of the configured duration, we may want to actually
	// opportunistically wrap up a bit early. Specifically, if doing so can avoid
	// starting a new fresh buffer that would need to then be flushed shortly
	// thereafter with very little in it, resulting in many small SSTs that are
	// almost as expensive to for their recipients but don't actually add much
	// data. Instead, if our buffer is full enough that it is likely to flush soon
	// and we're near the end of the alloted time, go ahead and stop there, flush
	// and return.
	opportunisticCheckpointAfter := (cb.spec.Duration * 4) / 5
	chunkSize := rowinfra.RowLimit(cb.spec.ChunkSize)
	updateChunkSizeThresholdBytes := rowinfra.BytesLimit(cb.spec.UpdateChunkSizeThresholdBytes)
	start := timeutil.Now()
	totalChunks := 0
	totalSpans := 0
	var finishedSpans roachpb.Spans

	for i := range cb.spec.Spans {
		log.VEventf(ctx, 2, "column backfiller starting span %d of %d: %s",
			i+1, len(cb.spec.Spans), cb.spec.Spans[i])
		chunks := 0
		todo := cb.spec.Spans[i]
		for todo.Key != nil {
			log.VEventf(ctx, 3, "column backfiller starting chunk %d: %s", chunks, todo)
			var err error
			todo.Key, err = cb.runChunk(ctx, todo, chunkSize, updateChunkSizeThresholdBytes, cb.spec.ReadAsOf)
			if err != nil {
				return nil, err
			}
			chunks++
			running := timeutil.Since(start)
			if running > opportunisticCheckpointAfter || running > cb.spec.Duration {
				break
			}
		}
		totalChunks += chunks

		// If we exited the loop with a non-nil resume key, we ran out of time.
		if todo.Key != nil {
			log.VEventf(ctx, 2,
				"column backfiller ran out of time on span %d of %d, will resume it at %s next time",
				i+1, len(cb.spec.Spans), todo)
			finishedSpans = append(finishedSpans, roachpb.Span{Key: cb.spec.Spans[i].Key, EndKey: todo.Key})
			break
		}
		log.VEventf(ctx, 2, "column backfiller finished span %d of %d: %s",
			i+1, len(cb.spec.Spans), cb.spec.Spans[i])
		totalSpans++
		finishedSpans = append(finishedSpans, cb.spec.Spans[i])
	}

	log.VEvent(ctx, 3, "column backfiller flushing...")
	if err := cb.runCommitWait(ctx); err != nil {
		return nil, err
	}
	log.VEventf(ctx, 2, "column backfiller finished %d spans in %d chunks in %s",
		totalSpans, totalChunks, timeutil.Since(start))

	return finishedSpans, nil
}

// GetResumeSpans returns a ResumeSpanList from a job.
func GetResumeSpans(
	ctx context.Context,
	jobsRegistry *jobs.Registry,
	txn isql.Txn,
	codec keys.SQLCodec,
	col *descs.Collection,
	tableID descpb.ID,
	mutationID descpb.MutationID,
	filter backfill.MutationFilter,
) ([]roachpb.Span, *jobs.Job, int, error) {
	tableDesc, err := col.ByIDWithoutLeased(txn.KV()).Get().Table(ctx, tableID)
	if err != nil {
		return nil, nil, 0, err
	}

	// Find the index of the first mutation that is being worked on.
	const noIndex = -1
	mutationIdx := noIndex
	for i, m := range tableDesc.AllMutations() {
		if m.MutationID() != mutationID {
			break
		}
		if mutationIdx == noIndex && filter(m) {
			mutationIdx = i
		}
	}

	if mutationIdx == noIndex {
		return nil, nil, 0, errors.AssertionFailedf(
			"mutation %d has completed", errors.Safe(mutationID))
	}

	// Find the job.
	var jobID jobspb.JobID
	if len(tableDesc.GetMutationJobs()) > 0 {
		// TODO (lucy): We need to get rid of MutationJobs. This is the only place
		// where we need to get the job where it's not completely straightforward to
		// remove the use of MutationJobs, since the backfiller doesn't otherwise
		// know which job it's associated with.
		for _, job := range tableDesc.GetMutationJobs() {
			if job.MutationID == mutationID {
				jobID = job.JobID
				break
			}
		}
	}

	if jobID == 0 {
		log.Errorf(ctx, "mutation with no job: %d, table desc: %+v", mutationID, tableDesc)
		return nil, nil, 0, errors.AssertionFailedf(
			"no job found for mutation %d", errors.Safe(mutationID))
	}

	job, err := jobsRegistry.LoadJobWithTxn(ctx, jobID, txn)
	if err != nil {
		return nil, nil, 0, errors.Wrapf(err, "can't find job %d", errors.Safe(jobID))
	}
	details, ok := job.Details().(jobspb.SchemaChangeDetails)
	if !ok {
		return nil, nil, 0, errors.AssertionFailedf(
			"expected SchemaChangeDetails job type, got %T", job.Details())
	}

	spanList := details.ResumeSpanList[mutationIdx].ResumeSpans
	prefix := codec.TenantPrefix()
	for i := range spanList {
		spanList[i], err = keys.RewriteSpanToTenantPrefix(spanList[i], prefix)
		if err != nil {
			return nil, nil, 0, err
		}
	}
	// Return the resume spans from the job using the mutation idx.
	return spanList, job, mutationIdx, nil
}

// SetResumeSpansInJob adds a list of resume spans into a job details field.
func SetResumeSpansInJob(
	ctx context.Context, spans []roachpb.Span, mutationIdx int, txn isql.Txn, job *jobs.Job,
) error {
	details, ok := job.Details().(jobspb.SchemaChangeDetails)
	if !ok {
		return errors.Errorf("expected SchemaChangeDetails job type, got %T", job.Details())
	}
	details.ResumeSpanList[mutationIdx].ResumeSpans = spans
	return job.WithTxn(txn).SetDetails(ctx, details)
}

// maxCommitWaitFns is the maximum number of commit-wait functions that the
// columnBackfiller will accumulate before consuming them to reclaim memory.
// Each function retains a reference to its corresponding TxnCoordSender, so we
// need to be careful not to accumulate an unbounded number of these functions.
var backfillerMaxCommitWaitFns = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"schemachanger.backfiller.max_commit_wait_fns",
	"the maximum number of commit-wait functions that the columnBackfiller will accumulate before consuming them to reclaim memory",
	128,
	settings.PositiveInt,
)

func (cb *columnBackfiller) runChunk(
	ctx context.Context,
	sp roachpb.Span,
	chunkSize rowinfra.RowLimit,
	updateChunkSizeThresholdBytes rowinfra.BytesLimit,
	_ hlc.Timestamp,
) (roachpb.Key, error) {
	var key roachpb.Key
	var commitWaitFn func(context.Context) error
	err := cb.flowCtx.Cfg.DB.Txn(
		ctx, func(ctx context.Context, txn isql.Txn) error {
			if cb.flowCtx.Cfg.TestingKnobs.RunBeforeBackfillChunk != nil {
				if err := cb.flowCtx.Cfg.TestingKnobs.RunBeforeBackfillChunk(sp); err != nil {
					return err
				}
			}
			if cb.flowCtx.Cfg.TestingKnobs.RunAfterBackfillChunk != nil {
				defer cb.flowCtx.Cfg.TestingKnobs.RunAfterBackfillChunk()
			}

			// Defer the commit-wait operation so that we can coalesce this wait
			// across all batches. This dramatically reduces the total time we spend
			// waiting for consistency when backfilling a column on GLOBAL tables.
			commitWaitFn = txn.KV().DeferCommitWait(ctx)

			var err error
			key, err = cb.RunColumnBackfillChunk(
				ctx,
				txn.KV(),
				cb.desc,
				sp,
				chunkSize,
				updateChunkSizeThresholdBytes,
				true, /*alsoCommit*/
				cb.flowCtx.TraceKV,
			)
			return err
		}, isql.WithPriority(admissionpb.BulkNormalPri))
	if err == nil {
		cb.commitWaitFns = append(cb.commitWaitFns, commitWaitFn)
		maxCommitWaitFns := int(backfillerMaxCommitWaitFns.Get(&cb.flowCtx.Cfg.Settings.SV))
		if len(cb.commitWaitFns) >= maxCommitWaitFns {
			if err := cb.runCommitWait(ctx); err != nil {
				return nil, err
			}
		}
	}
	return key, err
}

// runCommitWait consumes the commit-wait functions that the columnBackfiller
// has accumulated across the chunks that it has backfilled. It calls each
// commit-wait function to ensure that any dependent reads on the rows we just
// backfilled observe the new column.
func (cb *columnBackfiller) runCommitWait(ctx context.Context) error {
	for i, fn := range cb.commitWaitFns {
		if err := fn(ctx); err != nil {
			return err
		}
		cb.commitWaitFns[i] = nil
	}
	cb.commitWaitFns = cb.commitWaitFns[:0]
	return nil
}
