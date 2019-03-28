// Copyright 2019 The Cockroach Authors.
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

package sql

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// deleteRangeNode implements DELETE on a primary index satisfying certain
// conditions that permit the direct use of the DeleteRange kv operation,
// instead of many point deletes.
type deleteRangeNode struct {
	// interleavedFastPath is true if we can take the fast path despite operating
	// on an interleaved table.
	interleavedFastPath bool
	// spans are the spans to delete.
	spans roachpb.Spans
	// desc is the table descriptor the delete is operating on.
	desc *sqlbase.ImmutableTableDescriptor
	// tableWriter orchestrates the deletion operation itself.
	tableWriter tableWriterBase
	// fetcher is around to decode the returned keys from the DeleteRange, so that
	// we can count the number of rows deleted.
	fetcher row.Fetcher

	// rowCount will be set to the count of rows deleted.
	rowCount int
}

var _ autoCommitNode = &deleteRangeNode{}
var _ planNode = &deleteRangeNode{}
var _ planNodeFastPath = &deleteRangeNode{}
var _ batchedPlanNode = &deleteRangeNode{}

// canDeleteFast determines if the deletion of `rows` can be done
// without actually scanning them.
// This should be called after plan simplification for optimal results.
//
// This logic should be kept in sync with exec.Builder.canUseDeleteRange.
// TODO(andyk): Remove when the heuristic planner code is removed.
func maybeCreateDeleteFastNode(
	ctx context.Context,
	source planNode,
	desc *ImmutableTableDescriptor,
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

	return &deleteRangeNode{
		interleavedFastPath: fastPathInterleaved,
		spans:               scan.spans,
		desc:                desc,
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
	if err := d.fetcher.Init(
		false, false, false, &params.p.alloc,
		row.FetcherTableArgs{
			Desc:  d.desc,
			Index: &d.desc.PrimaryIndex,
		}); err != nil {
		return err
	}
	d.tableWriter.init(params.p.txn)
	if d.interleavedFastPath {
		for i := range d.spans {
			d.spans[i].EndKey = d.spans[i].EndKey.PrefixEnd()
		}
	}
	ctx := params.ctx
	traceKV := params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()
	for _, span := range d.spans {
		log.VEvent(ctx, 2, "fast delete: skipping scan")
		if traceKV {
			log.VEventf(ctx, 2, "DelRange %s - %s", span.Key, span.EndKey)
		}
		d.tableWriter.b.DelRange(span.Key, span.EndKey, true /* returnKeys */)
	}

	if err := d.tableWriter.finalize(ctx, d.desc); err != nil {
		return err
	}

	for _, r := range d.tableWriter.b.Results {
		var prev []byte
		for _, i := range r.Keys {
			// If prefix is same, don't bother decoding key.
			if len(prev) > 0 && bytes.HasPrefix(i, prev) {
				continue
			}

			after, ok, err := d.fetcher.ReadIndexKey(i)
			if err != nil {
				return err
			}
			if !ok {
				return errors.Errorf("key did not match descriptor")
			}
			k := i[:len(i)-len(after)]
			if !bytes.Equal(k, prev) {
				prev = k
				d.rowCount++
			}
		}
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(
		&params.EvalContext().Settings.SV,
		d.desc.ID,
		d.rowCount,
	)

	return nil
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

// enableAutoCommit implements the autoCommitNode interface.
func (d *deleteRangeNode) enableAutoCommit() {
	d.tableWriter.enableAutoCommit()
}
