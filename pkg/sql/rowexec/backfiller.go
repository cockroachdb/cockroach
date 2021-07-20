// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

type chunkBackfiller interface {
	// prepare must be called before runChunk.
	prepare(ctx context.Context) error

	// close should always be called to close a backfiller if prepare() was called.
	close(ctx context.Context)

	// runChunk returns the next-key and an error. next-key is nil
	// once the backfill is complete.
	runChunk(
		ctx context.Context,
		span roachpb.Span,
		chunkSize int64,
		readAsOf hlc.Timestamp,
	) (roachpb.Key, error)

	// CurrentBufferFill returns how fractionally full the configured buffer is.
	CurrentBufferFill() float32

	// flush must be called after the last chunk to finish buffered work.
	flush(ctx context.Context) error
}

// backfiller is a processor that implements a distributed backfill of
// an entity, like indexes or columns, during a schema change.
type backfiller struct {
	chunks chunkBackfiller
	// name is the name of the kind of entity this backfiller processes.
	name string
	// mutationFilter returns true if the mutation should be processed by the
	// chunkBackfiller.
	filter backfill.MutationFilter

	spec        execinfrapb.BackfillerSpec
	output      execinfra.RowReceiver
	out         execinfra.ProcOutputHelper
	flowCtx     *execinfra.FlowCtx
	processorID int32
}

// OutputTypes is part of the execinfra.Processor interface.
func (*backfiller) OutputTypes() []*types.T {
	// No output types.
	return nil
}

// MustBeStreaming is part of the execinfra.Processor interface.
func (*backfiller) MustBeStreaming() bool {
	return false
}

// Run is part of the execinfra.Processor interface.
func (b *backfiller) Run(ctx context.Context) {
	opName := fmt.Sprintf("%sBackfiller", b.name)
	ctx = logtags.AddTag(ctx, opName, int(b.spec.Table.ID))
	ctx, span := execinfra.ProcessorSpan(ctx, opName)
	defer span.Finish()
	meta := b.doRun(ctx)
	execinfra.SendTraceData(ctx, b.output)
	if emitHelper(ctx, b.output, &b.out, nil /* row */, meta, func(ctx context.Context) {}) {
		b.output.ProducerDone()
	}
}

func (b *backfiller) doRun(ctx context.Context) *execinfrapb.ProducerMetadata {
	semaCtx := tree.MakeSemaContext()
	if err := b.out.Init(&execinfrapb.PostProcessSpec{}, nil, &semaCtx, b.flowCtx.NewEvalCtx()); err != nil {
		return &execinfrapb.ProducerMetadata{Err: err}
	}
	finishedSpans, err := b.mainLoop(ctx)
	if err != nil {
		return &execinfrapb.ProducerMetadata{Err: err}
	}
	var prog execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	prog.CompletedSpans = append(prog.CompletedSpans, finishedSpans...)
	return &execinfrapb.ProducerMetadata{BulkProcessorProgress: &prog}
}

// mainLoop invokes runChunk on chunks of rows.
// It does not close the output.
func (b *backfiller) mainLoop(ctx context.Context) (roachpb.Spans, error) {
	if err := b.chunks.prepare(ctx); err != nil {
		return nil, err
	}
	defer b.chunks.close(ctx)

	// As we approach the end of the configured duration, we may want to actually
	// opportunistically wrap up a bit early. Specifically, if doing so can avoid
	// starting a new fresh buffer that would need to then be flushed shortly
	// thereafter with very little in it, resulting in many small SSTs that are
	// almost as expensive to for their recipients but don't actually add much
	// data. Instead, if our buffer is full enough that it is likely to flush soon
	// and we're near the end of the alloted time, go ahead and stop there, flush
	// and return.
	opportunisticCheckpointAfter := (b.spec.Duration * 4) / 5
	// opportunisticFillThreshold is the buffer fill fraction above which we'll
	// conclude that running another chunk risks starting *but not really filling*
	// a new buffer. This can be set pretty high -- if a single chunk is likely to
	// fill more than this amount and cause a flush, then it likely also fills
	// a non-trivial part of the next buffer.
	const opportunisticCheckpointThreshold = 0.8
	start := timeutil.Now()
	totalChunks := 0
	totalSpans := 0
	var finishedSpans roachpb.Spans

	for i := range b.spec.Spans {
		log.VEventf(ctx, 2, "%s backfiller starting span %d of %d: %s",
			b.name, i+1, len(b.spec.Spans), b.spec.Spans[i].Span)
		chunks := 0
		todo := b.spec.Spans[i].Span
		for todo.Key != nil {
			log.VEventf(ctx, 3, "%s backfiller starting chunk %d: %s", b.name, chunks, todo)
			var err error
			todo.Key, err = b.chunks.runChunk(ctx, todo, b.spec.ChunkSize, b.spec.ReadAsOf)
			if err != nil {
				return nil, err
			}
			chunks++
			running := timeutil.Since(start)
			if running > opportunisticCheckpointAfter && b.chunks.CurrentBufferFill() > opportunisticCheckpointThreshold {
				break
			}
			if running > b.spec.Duration {
				break
			}
		}
		totalChunks += chunks

		// If we exited the loop with a non-nil resume key, we ran out of time.
		if todo.Key != nil {
			log.VEventf(ctx, 2,
				"%s backfiller ran out of time on span %d of %d, will resume it at %s next time",
				b.name, i+1, len(b.spec.Spans), todo)
			finishedSpans = append(finishedSpans, roachpb.Span{Key: b.spec.Spans[i].Span.Key, EndKey: todo.Key})
			break
		}
		log.VEventf(ctx, 2, "%s backfiller finished span %d of %d: %s",
			b.name, i+1, len(b.spec.Spans), b.spec.Spans[i].Span)
		totalSpans++
		finishedSpans = append(finishedSpans, b.spec.Spans[i].Span)
	}

	log.VEventf(ctx, 3, "%s backfiller flushing...", b.name)
	if err := b.chunks.flush(ctx); err != nil {
		return nil, err
	}
	log.VEventf(ctx, 2, "%s backfiller finished %d spans in %d chunks in %s",
		b.name, totalSpans, totalChunks, timeutil.Since(start))

	return finishedSpans, nil
}

// GetResumeSpans returns a ResumeSpanList from a job.
func GetResumeSpans(
	ctx context.Context,
	jobsRegistry *jobs.Registry,
	txn *kv.Txn,
	codec keys.SQLCodec,
	tableID descpb.ID,
	mutationID descpb.MutationID,
	filter backfill.MutationFilter,
) ([]roachpb.Span, *jobs.Job, int, error) {
	tableDesc, err := catalogkv.MustGetTableDescByID(ctx, txn, codec, tableID)
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
				jobID = jobspb.JobID(job.JobID)
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
	// Return the resume spans from the job using the mutation idx.
	return details.ResumeSpanList[mutationIdx].ResumeSpans, job, mutationIdx, nil
}

// SetResumeSpansInJob adds a list of resume spans into a job details field.
func SetResumeSpansInJob(
	ctx context.Context, spans []roachpb.Span, mutationIdx int, txn *kv.Txn, job *jobs.Job,
) error {
	details, ok := job.Details().(jobspb.SchemaChangeDetails)
	if !ok {
		return errors.Errorf("expected SchemaChangeDetails job type, got %T", job.Details())
	}
	details.ResumeSpanList[mutationIdx].ResumeSpans = spans
	return job.SetDetails(ctx, txn, details)
}
