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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

// MutationFilter is the type of a simple predicate on a mutation.
type MutationFilter func(sqlbase.DescriptorMutation) bool

// backfiller is a processor that implements a distributed backfill of
// an entity, like indexes or columns, during a schema change.
type backfiller struct {
	chunkBackfiller
	// name is the name of the kind of entity this backfiller processes.
	name string
	// mutationFilter returns true if the mutation should be processed by the
	// chunkBackfiller.
	filter MutationFilter

	spec    BackfillerSpec
	output  RowReceiver
	flowCtx *FlowCtx
	fetcher sqlbase.RowFetcher
	alloc   sqlbase.DatumAlloc
}

// OutputTypes is part of the processor interface.
func (*backfiller) OutputTypes() []sqlbase.ColumnType {
	// No output types.
	return nil
}

// Run is part of the processor interface.
func (b *backfiller) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	opName := fmt.Sprintf("%sBackfiller", b.name)
	ctx := log.WithLogTagInt(b.flowCtx.Ctx, opName, int(b.spec.Table.ID))
	ctx, span := processorSpan(ctx, opName)
	defer tracing.FinishSpan(span)

	log.VEventf(ctx, 1, "starting")
	if log.V(1) {
		defer log.Infof(ctx, "exiting")
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
	const noNewIndex = -1
	addedIndexMutationIdx := noNewIndex
	desc := b.spec.Table
	if len(desc.Mutations) == 0 {
		return errors.Errorf("no schema changes for table ID=%d", desc.ID)
	}
	mutationID := desc.Mutations[0].MutationID
	for i, m := range desc.Mutations {
		if m.MutationID != mutationID {
			break
		}
		if b.filter(m) {
			mutations = append(mutations, m)
			if addedIndexMutationIdx == noNewIndex {
				addedIndexMutationIdx = i
			}
		}
	}

	if addedIndexMutationIdx == noNewIndex ||
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
		b.flowCtx.clientDB,
		b.spec.Table.ID,
		work,
		resume,
		addedIndexMutationIdx,
		b.flowCtx.JobRegistry,
	)
}

// GetResumeSpansFromJob returns a ResumeSpanList from a job given a job id and index.
func GetResumeSpansFromJob(
	ctx context.Context, jobsRegistry *jobs.Registry, txn *client.Txn, jobID int64, mutationIdx int,
) ([]roachpb.Span, error) {
	job, err := jobsRegistry.LoadJobWithTxn(ctx, jobID, txn)
	if err != nil {
		return nil, errors.Wrapf(err, "can't find job %d", jobID)
	}
	details, ok := job.Record.Details.(jobs.SchemaChangeDetails)
	if !ok {
		return nil, errors.Errorf("expected SchemaChangeDetails job type, got %T", job.Record.Details)
	}
	return details.ResumeSpanList[mutationIdx].ResumeSpans, nil
}

// GetResumeSpanIndexofMutationID returns the index of a resume span within a job
// that corresponds to the given mutation index and table descriptor.
func GetResumeSpanIndexofMutationID(tableDesc *sqlbase.TableDescriptor, mutationIdx int) int {
	if len(tableDesc.MutationJobs) > 0 {
		mutationID := tableDesc.Mutations[mutationIdx].MutationID
		for i, job := range tableDesc.Mutations {
			if job.MutationID == mutationID {
				return mutationIdx - i
			}
		}
	}
	return -1
}

// SetResumeSpansInJob addeds a list of resume spans into a job details field.
func SetResumeSpansInJob(
	ctx context.Context,
	spans []roachpb.Span,
	jobsRegistry *jobs.Registry,
	mutationIdx int,
	txn *client.Txn,
	jobID int64,
) error {

	job, err := jobsRegistry.LoadJobWithTxn(ctx, jobID, txn)
	if err != nil {
		return errors.Wrapf(err, "can't find job %d", jobID)
	}

	details, ok := job.Record.Details.(jobs.SchemaChangeDetails)
	if !ok {
		return errors.Errorf("expected SchemaChangeDetails job type, got %T", job.Record.Details)
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
	origSpan roachpb.Span,
	resume roachpb.Span,
	mutationIdx int,
	jobsRegistry *jobs.Registry,
) error {
	ctx, traceSpan := tracing.ChildSpan(ctx, "checkpoint")
	defer tracing.FinishSpan(traceSpan)
	if resume.Key != nil && !resume.EndKey.Equal(origSpan.EndKey) {
		panic("resume must end on the same key as origSpan")
	}
	cnt := 0
	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		cnt++
		tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, id)
		if err != nil {
			return err
		}
		if cnt > 1 {
			log.Infof(ctx, "retrying adding checkpoint %s to table %s", resume, tableDesc.Name)
		}

		mutationID := tableDesc.Mutations[mutationIdx].MutationID
		var jobID int64

		if len(tableDesc.MutationJobs) > 0 {
			for _, job := range tableDesc.MutationJobs {
				if job.MutationID == mutationID {
					jobID = job.JobID
					break
				}
			}
		}

		resumeSpanIndex := GetResumeSpanIndexofMutationID(tableDesc, mutationIdx)
		resumeSpans, error := GetResumeSpansFromJob(ctx, jobsRegistry, txn, jobID, resumeSpanIndex)
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

				return SetResumeSpansInJob(ctx, resumeSpans, jobsRegistry, mutationIdx, txn, jobID)
			}
		}
		// Unable to find a span containing origSpan.
		return errors.Errorf(
			"span %+v not found among %+v", origSpan, resumeSpans,
		)
	})
}

// ConvertBackfillError returns a cleaner SQL error for a failed Batch.
func ConvertBackfillError(
	ctx context.Context, tableDesc *sqlbase.TableDescriptor, b *client.Batch,
) error {
	// A backfill on a new schema element has failed and the batch contains
	// information useful in printing a sensible error. However
	// ConvertBatchError() will only work correctly if the schema elements
	// are "live" in the tableDesc.
	desc := protoutil.Clone(tableDesc).(*sqlbase.TableDescriptor)
	mutationID := desc.Mutations[0].MutationID
	for _, mutation := range desc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		desc.MakeMutationComplete(mutation)
	}
	return sqlbase.ConvertBatchError(ctx, desc, b)
}
