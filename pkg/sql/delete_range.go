// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// deleteRangeNode implements DELETE on a primary index satisfying certain
// conditions that permit the direct use of the DeleteRange kv operation,
// instead of many point deletes.
//
// Note: deleteRangeNode can't autocommit in the general case, because it has to
// delete in batches, and it won't know whether or not there is more work to do
// until after a batch is returned. This property precludes using auto commit.
// However, if the optimizer can prove that only a small number of rows will
// be deleted, it'll enable autoCommit for delete range.
type deleteRangeNode struct {
	zeroInputPlanNode
	rowsAffectedOutputHelper
	// spans are the spans to delete.
	spans roachpb.Spans
	// desc is the table descriptor the delete is operating on.
	desc catalog.TableDescriptor
	// fetcher is around to decode the returned keys from the DeleteRange, so that
	// we can count the number of rows deleted.
	fetcher row.Fetcher

	// autoCommitEnabled is set to true if the optimizer proved that we can safely
	// use autocommit - so that the number of possible returned keys from this
	// operation is low. If this is true, we won't attempt to run the delete in
	// batches and will just send one big delete with a commit statement attached.
	autoCommitEnabled bool
}

// deleteRangeRun contains the execution logic for deleteRangeNode.
type deleteRangeRun struct {
	node *deleteRangeNode
}

func (r *deleteRangeRun) executeDeleteRange(ctx context.Context, flowCtx *execinfra.FlowCtx) error {
	// Configure the fetcher, which is only used to decode the returned keys
	// from the Del and the DelRange operations, and is never used to actually
	// fetch kvs.
	var spec fetchpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(
		&spec, flowCtx.Codec(), r.node.desc, r.node.desc.GetPrimaryIndex(), nil, /* columnIDs */
	); err != nil {
		return err
	}
	if err := r.node.fetcher.Init(
		ctx,
		row.FetcherInitArgs{
			WillUseKVProvider: true,
			Alloc:             &tree.DatumAlloc{},
			Spec:              &spec,
		},
	); err != nil {
		return err
	}

	txn := flowCtx.Txn
	sessionData := flowCtx.EvalCtx.SessionData()

	log.VEvent(ctx, 2, "fast delete: skipping scan")
	spans := make([]roachpb.Span, len(r.node.spans))
	copy(spans, r.node.spans)
	if !r.node.autoCommitEnabled {
		// Without autocommit, we're going to run each batch one by one, respecting
		// a max span request keys size. We use spans as a queue of spans to delete.
		// It'll be edited if there are any resume spans encountered (if any request
		// hits the key limit).
		for len(spans) != 0 {
			b := txn.NewBatch()
			b.Header.MaxSpanRequestKeys = row.TableTruncateChunkSize
			b.Header.LockTimeout = sessionData.LockTimeout
			b.Header.DeadlockTimeout = sessionData.DeadlockTimeout
			r.deleteSpans(ctx, b, spans, flowCtx.TraceKV)
			log.VEventf(ctx, 2, "fast delete: processing %d spans", len(spans))
			if err := txn.Run(ctx, b); err != nil {
				return row.ConvertBatchError(ctx, r.node.desc, b, false /* alwaysConvertCondFailed */)
			}

			spans = spans[:0]
			var err error
			if spans, err = r.processResults(b.Results, spans); err != nil {
				return err
			}
		}
	} else {
		log.Event(ctx, "autocommit enabled")
		// With autocommit, we're going to run the deleteRange in a single batch
		// without a limit, since limits and deleteRange aren't compatible with 1pc
		// transactions / autocommit. This isn't inherently safe, because without a
		// limit, this command could technically use up unlimited memory. However,
		// the optimizer only enables autoCommit if the maximum possible number of
		// keys to delete in this command are low, so we're made safe.
		b := txn.NewBatch()
		b.Header.LockTimeout = sessionData.LockTimeout
		b.Header.DeadlockTimeout = sessionData.DeadlockTimeout
		r.deleteSpans(ctx, b, spans, flowCtx.TraceKV)
		log.VEventf(ctx, 2, "fast delete: processing %d spans and committing", len(spans))
		if err := txn.CommitInBatch(ctx, b); err != nil {
			return row.ConvertBatchError(ctx, r.node.desc, b, false /* alwaysConvertCondFailed */)
		}
		if resumeSpans, err := r.processResults(b.Results, nil /* resumeSpans */); err != nil {
			return err
		} else if len(resumeSpans) != 0 {
			// This shouldn't ever happen - we didn't pass a limit into the batch.
			return errors.AssertionFailedf("deleteRange without a limit unexpectedly returned resumeSpans")
		}
	}

	// Possibly initiate a run of CREATE STATISTICS.
	flowCtx.Cfg.StatsRefresher.NotifyMutation(r.node.desc, r.node.rowCount)

	return nil
}

var _ planNode = &deleteRangeNode{}
var _ mutationPlanNode = &deleteRangeNode{}

func (d *deleteRangeNode) rowsWritten() int64 {
	return int64(d.rowCount)
}

func (d *deleteRangeNode) returnsRowsAffected() bool {
	// DeleteRange always returns the number of rows deleted.
	return true
}

func (d *deleteRangeNode) startExec(params runParams) error {
	panic("deleteRangeNode cannot be run in local mode")
}

// deleteSpans adds each input span to a Del or a DelRange command in the given
// batch.
func (r *deleteRangeRun) deleteSpans(
	ctx context.Context, b *kv.Batch, spans roachpb.Spans, traceKV bool,
) {
	for _, span := range spans {
		if span.EndKey == nil {
			if traceKV {
				log.VEventf(ctx, 2, "Del (locking) %s", span.Key)
			}
			// We use the locking Del here unconditionally since:
			// - if buffered writes are enabled, since we haven't performed the
			// read, we need to tell the KV layer to acquire the lock
			// explicitly.
			// - if buffered writes are disabled, then the KV layer will write
			// an intent which acts as a lock.
			b.DelMustAcquireExclusiveLock(span.Key)
		} else {
			if traceKV {
				log.VEventf(ctx, 2, "DelRange %s - %s", span.Key, span.EndKey)
			}
			b.DelRange(span.Key, span.EndKey, true /* returnKeys */)
		}
	}
}

// processResults parses the results of a DelRangeResponse, incrementing the
// rowCount we're going to return for each row. If any resume spans are
// encountered during result processing, they're appended to the resumeSpans
// input parameter.
func (r *deleteRangeRun) processResults(
	results []kv.Result, resumeSpans []roachpb.Span,
) (roachpb.Spans, error) {
	for _, result := range results {
		var prev []byte
		for _, keyBytes := range result.Keys {
			// If prefix is same, don't bother decoding key.
			if len(prev) > 0 && bytes.HasPrefix(keyBytes, prev) {
				continue
			}

			after, _, err := r.node.fetcher.DecodeIndexKey(keyBytes)
			if err != nil {
				return nil, err
			}
			k := keyBytes[:len(keyBytes)-len(after)]
			if !bytes.Equal(k, prev) {
				prev = k
				r.node.incAffectedRows()
			}
		}
		if result.ResumeSpan != nil && result.ResumeSpan.Valid() {
			resumeSpans = append(resumeSpans, *result.ResumeSpan)
		}
	}
	return resumeSpans, nil
}

// Next implements the planNode interface.
func (d *deleteRangeNode) Next(_ runParams) (bool, error) {
	panic("deleteRangeNode cannot be run in local mode")
}

// Values implements the planNode interface.
func (d *deleteRangeNode) Values() tree.Datums {
	panic("deleteRangeNode cannot be run in local mode")
}

// Close implements the planNode interface.
func (*deleteRangeNode) Close(ctx context.Context) {}

// deleteRangeProcessor is a LocalProcessor that wraps deleteRangeNode execution
// logic.
type deleteRangeProcessor struct {
	execinfra.ProcessorBase

	node *deleteRangeNode

	outputTypes []*types.T

	encDatumScratch rowenc.EncDatumRow
}

var _ execinfra.LocalProcessor = &deleteRangeProcessor{}

// Init initializes the deleteRangeProcessor.
func (d *deleteRangeProcessor) Init(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	post *execinfrapb.PostProcessSpec,
) error {
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.Mon, mon.MakeName("delete-range-mem"))
	return d.InitWithEvalCtx(
		ctx, d, post, d.outputTypes, flowCtx, flowCtx.EvalCtx, processorID, memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				d.close()
				return nil
			},
		},
	)
}

// SetInput sets the input RowSource for the deleteRangeProcessor.
func (d *deleteRangeProcessor) SetInput(ctx context.Context, input execinfra.RowSource) error {
	panic(errors.AssertionFailedf("deleteRangeProcessor does not have an input RowSource"))
}

// Start begins execution of the deleteRangeProcessor.
func (d *deleteRangeProcessor) Start(ctx context.Context) {
	d.StartInternal(ctx, "deleteRangeProcessor")

	run := &deleteRangeRun{node: d.node}

	// Run the delete range operation to completion.
	if err := run.executeDeleteRange(d.Ctx(), d.FlowCtx); err != nil {
		d.MoveToDraining(err)
	}
}

// Next implements the RowSource interface.
func (d *deleteRangeProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if d.State != execinfra.StateRunning {
		return nil, d.DrainHelper()
	}

	// Return next row from accumulated results. For deleteRangeProcessor, this
	// will always simply be the number of rows deleted.
	for d.node.next() {
		datumRow := d.node.values()
		if cap(d.encDatumScratch) < len(datumRow) {
			d.encDatumScratch = make(rowenc.EncDatumRow, len(datumRow))
		}
		encRow := d.encDatumScratch[:len(datumRow)]
		for i, datum := range datumRow {
			encRow[i] = rowenc.DatumToEncDatum(d.outputTypes[i], datum)
		}
		if outRow := d.ProcessRowHelper(encRow); outRow != nil {
			return outRow, nil
		}
	}

	// No more rows to return.
	d.MoveToDraining(nil)
	return nil, d.DrainHelper()
}

func (d *deleteRangeProcessor) close() {
	if d.InternalClose() {
		d.node = nil
		d.MemMonitor.Stop(d.Ctx())
	}
}

// ConsumerClosed implements the RowSource interface.
func (d *deleteRangeProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	d.close()
}
