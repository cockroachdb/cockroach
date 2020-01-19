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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	// interleavedFastPath is true if we can take the fast path despite operating
	// on an interleaved table.
	interleavedFastPath bool
	// spans are the spans to delete.
	spans roachpb.Spans
	// desc is the table descriptor the delete is operating on.
	desc *sqlbase.ImmutableTableDescriptor
	// interleavedDesc are the table descriptors of any child interleaved tables
	// the delete is operating on.
	interleavedDesc []*sqlbase.ImmutableTableDescriptor
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

// canDeleteFast determines if the deletion of `rows` can be done
// without actually scanning them.
// This should be called after plan simplification for optimal results.
//
// fkTables is required primarily when performing deletes with interleaved child
// tables as we need to pass those foreign key table references to the Fetcher
// (to ensure that keys deleted from child tables are truly within the range to be deleted).
//
// This logic should be kept in sync with exec.Builder.canUseDeleteRange.
// TODO(andyk): Remove when the heuristic planner code is removed.
func maybeCreateDeleteFastNode(
	ctx context.Context,
	source planNode,
	desc *ImmutableTableDescriptor,
	fkTables row.FkTableMetadata,
	fastPathInterleaved bool,
	rowsNeeded bool,
) (*deleteRangeNode, bool) {
	// Check that there are no secondary indexes, interleaving, FK
	// references checks, etc., ie. there is no extra work to be done
	// per row deleted.
	if !fastPathDeleteAvailable(ctx, desc) && !fastPathInterleaved {
		return nil, false
	}

	// If the rows are needed (a RETURNING clause), we can't skip them.
	if rowsNeeded {
		return nil, false
	}

	// Check whether the source plan is "simple": that it contains no remaining
	// filtering, limiting, sorting, etc. Note that this logic must be kept in
	// sync with the logic for setting scanNode.isDeleteSource (see doExpandPlan.)
	// TODO(dt): We could probably be smarter when presented with an
	// index-join, but this goes away anyway once we push-down more of
	// SQL.
	maybeScan := source
	if sel, ok := maybeScan.(*renderNode); ok {
		// There may be a projection to drop/rename some columns which the
		// optimizations did not remove at this point. We just ignore that
		// projection for the purpose of this check.
		maybeScan = sel.source.plan
	}

	scan, ok := maybeScan.(*scanNode)
	if !ok {
		// Not simple enough. Bail.
		return nil, false
	}

	// A scan ought to be simple enough, except when it's not: a scan
	// may have a remaining filter. We can't be fast over that.
	if scan.filter != nil {
		if log.V(2) {
			log.Infof(ctx, "delete forced to scan: values required for filter (%s)", scan.filter)
		}
		return nil, false
	}

	if scan.hardLimit != 0 {
		if log.V(2) {
			log.Infof(ctx, "delete forced to scan: scan has limit %d", scan.hardLimit)
		}
		return nil, false
	}

	interleavedDesc := make([]*sqlbase.ImmutableTableDescriptor, 0, len(fkTables))
	for _, tableEntry := range fkTables {
		interleavedDesc = append(interleavedDesc, tableEntry.Desc)
	}

	return &deleteRangeNode{
		interleavedFastPath: fastPathInterleaved,
		spans:               scan.spans,
		desc:                desc,
		interleavedDesc:     interleavedDesc,
	}, true
}

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

// startExec implements the planNode interface.
func (d *deleteRangeNode) startExec(params runParams) error {
	if err := params.p.cancelChecker.Check(); err != nil {
		return err
	}
	if err := params.p.maybeSetSystemConfig(d.desc.GetID()); err != nil {
		return err
	}

	if d.interleavedFastPath {
		for i := range d.spans {
			d.spans[i].EndKey = d.spans[i].EndKey.PrefixEnd()
		}
	}

	allTables := make([]row.FetcherTableArgs, len(d.interleavedDesc)+1)
	allTables[0] = row.FetcherTableArgs{
		Desc:  d.desc,
		Index: &d.desc.PrimaryIndex,
		Spans: d.spans,
	}
	for i, interleaved := range d.interleavedDesc {
		allTables[i+1] = row.FetcherTableArgs{
			Desc:  interleaved,
			Index: &interleaved.PrimaryIndex,
			Spans: d.spans,
		}
	}
	if err := d.fetcher.Init(
		false, false, false, &params.p.alloc, allTables...); err != nil {
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
			d.deleteSpans(params, b, spans)
			b.Header.MaxSpanRequestKeys = TableTruncateChunkSize
			if err := params.p.txn.Run(ctx, b); err != nil {
				return err
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
		d.deleteSpans(params, b, spans)
		if err := params.p.txn.CommitInBatch(ctx, b); err != nil {
			return err
		}
		if resumeSpans, err := d.processResults(b.Results, nil /* resumeSpans */); err != nil {
			return err
		} else if len(resumeSpans) != 0 {
			// This shouldn't ever happen - we didn't pass a limit into the batch.
			return errors.AssertionFailedf("deleteRange without a limit unexpectedly returned resumeSpans")
		}
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(d.desc.ID, d.rowCount)

	return nil
}

// deleteSpans adds each input span to a DelRange command in the given batch.
func (d *deleteRangeNode) deleteSpans(params runParams, b *client.Batch, spans roachpb.Spans) {
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
	results []client.Result, resumeSpans []roachpb.Span,
) (roachpb.Spans, error) {
	for _, r := range results {
		var prev []byte
		for _, keyBytes := range r.Keys {
			// If prefix is same, don't bother decoding key.
			if len(prev) > 0 && bytes.HasPrefix(keyBytes, prev) {
				continue
			}

			after, ok, _, err := d.fetcher.ReadIndexKey(keyBytes)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, errors.AssertionFailedf("key did not match descriptor")
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
	panic("invalid")
}

// Values implements the planNode interface.
func (*deleteRangeNode) Values() tree.Datums {
	panic("invalid")
}

// Close implements the planNode interface.
func (*deleteRangeNode) Close(ctx context.Context) {}
