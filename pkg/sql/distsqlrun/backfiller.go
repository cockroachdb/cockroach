// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
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
		mutations []sqlbase.DescriptorMutation,
		span roachpb.Span,
		chunkSize int64,
		readAsOf hlc.Timestamp,
	) (roachpb.Key, error)

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

	spec        distsqlpb.BackfillerSpec
	output      RowReceiver
	flowCtx     *FlowCtx
	processorID int32
}

// OutputTypes is part of the processor interface.
func (*backfiller) OutputTypes() []sqlbase.ColumnType {
	// No output types.
	return nil
}

// Run is part of the Processor interface.
func (b *backfiller) Run(ctx context.Context) {
	opName := fmt.Sprintf("%sBackfiller", b.name)
	ctx = logtags.AddTag(ctx, opName, int(b.spec.Table.ID))
	ctx, span := processorSpan(ctx, opName)
	defer tracing.FinishSpan(span)

	if err := b.mainLoop(ctx); err != nil {
		b.output.Push(nil /* row */, &ProducerMetadata{Err: err})
	}
	sendTraceData(ctx, b.output)
	b.output.ProducerDone()
}

// mainLoop invokes runChunk on chunks of rows.
// It does not close the output.
func (b *backfiller) mainLoop(ctx context.Context) error {
	var mutations []sqlbase.DescriptorMutation
	desc := b.spec.Table
	if len(desc.Mutations) == 0 {
		return errors.Errorf("no schema changes for table ID=%d", desc.ID)
	}
	const noNewIndex = -1
	// The first index of a mutation in the mutation list that will be
	// processed.
	firstMutationIdx := noNewIndex
	mutationID := desc.Mutations[0].MutationID
	for i, m := range desc.Mutations {
		if m.MutationID != mutationID {
			break
		}
		if b.filter(m) {
			mutations = append(mutations, m)
			if firstMutationIdx == noNewIndex {
				firstMutationIdx = i
			}
		}
	}

	if firstMutationIdx == noNewIndex ||
		len(b.spec.Spans) == 0 {
		return errors.Errorf("completed processing all spans for %s backfill (%d, %d)", b.name, desc.ID, mutationID)
	}

	// Backfill the mutations for all the rows.
	chunkSize := b.spec.ChunkSize

	if err := b.chunks.prepare(ctx); err != nil {
		return err
	}
	defer b.chunks.close(ctx)

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
			todo.Key, err = b.chunks.runChunk(ctx, mutations, todo, chunkSize, b.spec.ReadAsOf)
			if err != nil {
				return err
			}
			chunks++
			if timeutil.Since(start) > b.spec.Duration {
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
		return err
	}
	log.VEventf(ctx, 2, "%s backfiller finished %d spans in %d chunks in %s",
		b.name, totalSpans, totalChunks, timeutil.Since(start))

	return WriteResumeSpan(ctx,
		b.flowCtx.ClientDB,
		b.spec.Table.ID,
		mutationID,
		b.filter,
		finishedSpans,
		b.flowCtx.JobRegistry,
	)
}

// GetResumeSpans returns a ResumeSpanList from a job.
func GetResumeSpans(
	ctx context.Context,
	jobsRegistry *jobs.Registry,
	txn *client.Txn,
	tableID sqlbase.ID,
	mutationID sqlbase.MutationID,
	filter backfill.MutationFilter,
) ([]roachpb.Span, *jobs.Job, int, error) {
	tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, tableID)
	if err != nil {
		return nil, nil, 0, err
	}

	// Find the index of the first mutation that is being worked on.
	const noIndex = -1
	mutationIdx := noIndex
	if len(tableDesc.Mutations) > 0 {
		for i, m := range tableDesc.Mutations {
			if m.MutationID != mutationID {
				break
			}
			if mutationIdx == noIndex && filter(m) {
				mutationIdx = i
			}
		}
	}

	if mutationIdx == noIndex {
		return nil, nil, 0, pgerror.NewAssertionErrorf(
			"mutation %d has completed", log.Safe(mutationID))
	}

	// Find the job.
	var jobID int64
	if len(tableDesc.MutationJobs) > 0 {
		for _, job := range tableDesc.MutationJobs {
			if job.MutationID == mutationID {
				jobID = job.JobID
				break
			}
		}
	}

	if jobID == 0 {
		return nil, nil, 0, pgerror.NewAssertionErrorf(
			"no job found for mutation %d", log.Safe(mutationID))
	}

	job, err := jobsRegistry.LoadJobWithTxn(ctx, jobID, txn)
	if err != nil {
		return nil, nil, 0, pgerror.Wrapf(err, pgerror.CodeDataExceptionError,
			"can't find job %d", log.Safe(jobID))
	}
	details, ok := job.Details().(jobspb.SchemaChangeDetails)
	if !ok {
		return nil, nil, 0, pgerror.NewAssertionErrorf(
			"expected SchemaChangeDetails job type, got %T", job.Details())
	}
	// Return the resume spans from the job using the mutation idx.
	return details.ResumeSpanList[mutationIdx].ResumeSpans, job, mutationIdx, nil
}

// SetResumeSpansInJob addeds a list of resume spans into a job details field.
func SetResumeSpansInJob(
	ctx context.Context, spans []roachpb.Span, mutationIdx int, txn *client.Txn, job *jobs.Job,
) error {
	details, ok := job.Details().(jobspb.SchemaChangeDetails)
	if !ok {
		return errors.Errorf("expected SchemaChangeDetails job type, got %T", job.Details())
	}
	details.ResumeSpanList[mutationIdx].ResumeSpans = spans
	return job.WithTxn(txn).SetDetails(ctx, details)
}

// WriteResumeSpan writes a checkpoint for the backfill work on origSpan.
// origSpan is the span of keys that were assigned to be backfilled,
// resume is the left over work from origSpan.
func WriteResumeSpan(
	ctx context.Context,
	db *client.DB,
	id sqlbase.ID,
	mutationID sqlbase.MutationID,
	filter backfill.MutationFilter,
	finished roachpb.Spans,
	jobsRegistry *jobs.Registry,
) error {
	ctx, traceSpan := tracing.ChildSpan(ctx, "checkpoint")
	defer tracing.FinishSpan(traceSpan)

	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		resumeSpans, job, mutationIdx, error := GetResumeSpans(ctx, jobsRegistry, txn, id, mutationID, filter)
		if error != nil {
			return error
		}

		resumeSpans = roachpb.SubtractSpans(resumeSpans, finished)
		return SetResumeSpansInJob(ctx, resumeSpans, mutationIdx, txn, job)
	})
}
