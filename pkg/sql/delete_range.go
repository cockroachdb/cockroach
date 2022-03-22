// Copyright 2019 The Cockroach Authors.
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
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	// rowCount will be set to the count of rows deleted.
	rowCount int
}

var _ planNode = &deleteRangeNode{}
var _ planNodeFastPath = &deleteRangeNode{}
var _ batchedPlanNode = &deleteRangeNode{}
var _ mutationPlanNode = &deleteRangeNode{}

// BatchedNext implements the batchedPlanNode interface.
func (d *deleteRangeNode) BatchedNext(params runParams) (bool, error) {
	return false, nil
}

// BatchedCount implements the batchedPlanNode interface.
func (d *deleteRangeNode) BatchedCount() int {
	return d.rowCount
}

// BatchedValues implements the batchedPlanNode interface.
func (d *deleteRangeNode) BatchedValues(rowIdx int) tree.Datums {
	panic("invalid")
}

// FastPathResults implements the planNodeFastPath interface.
func (d *deleteRangeNode) FastPathResults() (int, bool) {
	return d.rowCount, true
}

func (d *deleteRangeNode) rowsWritten() int64 {
	return int64(d.rowCount)
}

// startExec implements the planNode interface.
func (d *deleteRangeNode) startExec(params runParams) error {
	if err := params.p.cancelChecker.Check(); err != nil {
		return err
	}

	// Configure the fetcher, which is only used to decode the returned keys from
	// the DeleteRange, and is never used to actually fetch kvs.
	var spec descpb.IndexFetchSpec
	if err := rowenc.InitIndexFetchSpec(
		&spec, params.ExecCfg().Codec, d.desc, d.desc.GetPrimaryIndex(), nil, /* columnIDs */
	); err != nil {
		return err
	}
	if err := d.fetcher.Init(
		params.ctx,
		false, /* reverse */
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		0, /* lockTimeout */
		params.p.alloc,
		nil, /* memMonitor */
		&spec,
	); err != nil {
		return err
	}

	ctx := params.ctx
	log.VEvent(ctx, 2, "fast delete: skipping scan")
	spans := make([]roachpb.Span, len(d.spans))
	copy(spans, d.spans)
	if !d.autoCommitEnabled {
		// Without autocommit, we're going to run each batch one by one, respecting
		// a max span request keys size. We use spans as a queue of spans to delete.
		// It'll be edited if there are any resume spans encountered (if any request
		// hits the key limit).
		for len(spans) != 0 {
			b := params.p.txn.NewBatch()
			b.Header.MaxSpanRequestKeys = row.TableTruncateChunkSize
			b.Header.LockTimeout = params.SessionData().LockTimeout
			d.deleteSpans(params, b, spans)
			if err := params.p.txn.Run(ctx, b); err != nil {
				return row.ConvertBatchError(ctx, d.desc, b)
			}

			spans = spans[:0]
			var err error
			if spans, err = d.processResults(b.Results, spans); err != nil {
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
		b := params.p.txn.NewBatch()
		b.Header.LockTimeout = params.SessionData().LockTimeout
		d.deleteSpans(params, b, spans)
		if err := params.p.txn.CommitInBatch(ctx, b); err != nil {
			return row.ConvertBatchError(ctx, d.desc, b)
		}
		if resumeSpans, err := d.processResults(b.Results, nil /* resumeSpans */); err != nil {
			return err
		} else if len(resumeSpans) != 0 {
			// This shouldn't ever happen - we didn't pass a limit into the batch.
			return errors.AssertionFailedf("deleteRange without a limit unexpectedly returned resumeSpans")
		}
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(d.desc, d.rowCount)

	return nil
}

// deleteSpans adds each input span to a DelRange command in the given batch.
func (d *deleteRangeNode) deleteSpans(params runParams, b *kv.Batch, spans roachpb.Spans) {
	ctx := params.ctx
	traceKV := params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()
	for _, span := range spans {
		if traceKV {
			log.VEventf(ctx, 2, "DelRange %s - %s", span.Key, span.EndKey)
		}
		b.DelRange(span.Key, span.EndKey, true /* returnKeys */)
	}
}

// processResults parses the results of a DelRangeResponse, incrementing the
// rowCount we're going to return for each row. If any resume spans are
// encountered during result processing, they're appended to the resumeSpans
// input parameter.
func (d *deleteRangeNode) processResults(
	results []kv.Result, resumeSpans []roachpb.Span,
) (roachpb.Spans, error) {
	for _, r := range results {
		var prev []byte
		for _, keyBytes := range r.Keys {
			// If prefix is same, don't bother decoding key.
			if len(prev) > 0 && bytes.HasPrefix(keyBytes, prev) {
				continue
			}

			after, _, err := d.fetcher.DecodeIndexKey(keyBytes)
			if err != nil {
				return nil, err
			}
			k := keyBytes[:len(keyBytes)-len(after)]
			if !bytes.Equal(k, prev) {
				prev = k
				d.rowCount++
			}
		}
		if r.ResumeSpan != nil && r.ResumeSpan.Valid() {
			resumeSpans = append(resumeSpans, *r.ResumeSpan)
		}
	}
	return resumeSpans, nil
}

// Next implements the planNode interface.
func (*deleteRangeNode) Next(params runParams) (bool, error) {
	// TODO(radu): this shouldn't be used, but it gets called when a cascade uses
	// delete-range. Investigate this.
	return false, nil
}

// Values implements the planNode interface.
func (*deleteRangeNode) Values() tree.Datums {
	panic("invalid")
}

// Close implements the planNode interface.
func (*deleteRangeNode) Close(ctx context.Context) {}
