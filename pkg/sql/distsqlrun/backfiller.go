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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

type chunkBackfiller interface {
	// runChunk returns the next-key and an error. next-key is nil
	// once the backfill is complete.
	runChunk(
		ctx context.Context,
		mutations []sqlbase.DescriptorMutation,
		span roachpb.Span,
		chunkSize int64,
		readAsOf hlc.Timestamp,
	) (roachpb.Key, error)
}

// backfiller is a processor that implements a distributed backfill of
// an entity, like indexes or columns, during a schema change.
type backfiller struct {
	chunkBackfiller
	// name is the name of the kind of entity this backfiller processes.
	name string
	// mutationFilter returns true if the mutation should be processed by the
	// chunkBackfiller.
	filter backfill.MutationFilter

	spec        BackfillerSpec
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
func (b *backfiller) Run(ctx context.Context, wg *sync.WaitGroup) {
	opName := fmt.Sprintf("%sBackfiller", b.name)
	ctx = logtags.AddTag(ctx, opName, int(b.spec.Table.ID))
	ctx, span := processorSpan(ctx, opName)
	defer tracing.FinishSpan(span)

	if wg != nil {
		defer wg.Done()
	}

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
	work := b.spec.Spans[0].Span

	// Backfill the mutations for all the rows.
	chunkSize := b.spec.ChunkSize
	start := timeutil.Now()
	var resume roachpb.Span
	sp := work
	var nChunks, row = 0, int64(0)
	for ; sp.Key != nil; nChunks, row = nChunks+1, row+chunkSize {
		if log.V(2) {
			log.Infof(ctx, "%s backfill (%d, %d) at row: %d, span: %s",
				b.name, desc.ID, mutationID, row, sp)
		}
		var err error
		sp.Key, err = b.runChunk(ctx, mutations, sp, chunkSize, b.spec.ReadAsOf)
		if err != nil {
			return err
		}
		if timeutil.Since(start) > b.spec.Duration && sp.Key != nil {
			resume = sp
			break
		}
	}
	log.VEventf(ctx, 2, "processed %d rows in %d chunks", row, nChunks)
	return WriteResumeSpan(ctx,
		b.flowCtx.ClientDB,
		b.spec.Table.ID,
		mutationID,
		b.filter,
		work,
		resume,
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
		return nil, nil, 0, errors.Errorf("mutation %d has completed", mutationID)
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
		return nil, nil, 0, errors.Errorf("no job found for mutation %d", mutationID)
	}

	job, err := jobsRegistry.LoadJobWithTxn(ctx, jobID, txn)
	if err != nil {
		return nil, nil, 0, errors.Wrapf(err, "can't find job %d", jobID)
	}
	details, ok := job.Details().(jobspb.SchemaChangeDetails)
	if !ok {
		return nil, nil, 0, errors.Errorf("expected SchemaChangeDetails job type, got %T", job.Details())
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
	origSpan roachpb.Span,
	resume roachpb.Span,
	jobsRegistry *jobs.Registry,
) error {
	ctx, traceSpan := tracing.ChildSpan(ctx, "checkpoint")
	defer tracing.FinishSpan(traceSpan)
	if resume.Key != nil && !resume.EndKey.Equal(origSpan.EndKey) {
		panic("resume must end on the same key as origSpan")
	}

	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		resumeSpans, job, mutationIdx, error := GetResumeSpans(ctx, jobsRegistry, txn, id, mutationID, filter)
		if error != nil {
			return error
		}

		// This loop is finding a span in the checkpoint that fits
		// origSpan. It then carves a spot for origSpan in the
		// checkpoint, and replaces origSpan in the checkpoint with
		// resume.
		for i, sp := range resumeSpans {
			if sp.Key.Compare(origSpan.Key) <= 0 &&
				sp.EndKey.Compare(origSpan.EndKey) >= 0 {
				// origSpan is in sp; split sp if needed to accommodate
				// origSpan and replace origSpan with resume.
				before := resumeSpans[:i]
				after := append([]roachpb.Span{}, resumeSpans[i+1:]...)

				// add span to before, but merge it with the last span
				// if possible.
				addSpan := func(begin, end roachpb.Key) {
					if begin.Equal(end) {
						return
					}
					if len(before) > 0 && before[len(before)-1].EndKey.Equal(begin) {
						before[len(before)-1].EndKey = end
					} else {
						before = append(before, roachpb.Span{Key: begin, EndKey: end})
					}
				}

				// The work done = [origSpan.Key...resume.Key]
				addSpan(sp.Key, origSpan.Key)
				if resume.Key != nil {
					addSpan(resume.Key, resume.EndKey)
				} else {
					log.VEventf(ctx, 2, "completed processing of span: %+v", origSpan)
				}
				addSpan(origSpan.EndKey, sp.EndKey)
				resumeSpans = append(before, after...)

				log.VEventf(ctx, 2, "ckpt %+v", resumeSpans)

				return SetResumeSpansInJob(ctx, resumeSpans, mutationIdx, txn, job)
			}
		}
		// Unable to find a span containing origSpan.
		return errors.Errorf(
			"span %+v not found among %+v", origSpan, resumeSpans,
		)
	})
}
