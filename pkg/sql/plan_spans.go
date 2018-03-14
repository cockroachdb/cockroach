// Copyright 2017 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// collectSpans collects the upper bound set of read and write spans that the
// planNode expects to touch when executed. The two sets do not need to be
// disjoint, and any span in the write set will be implicitly considered in
// the read set as well. There is also no guarantee that Spans within either
// set are disjoint. It is an error for a planNode to touch any span outside
// those that it reports from this method, but a planNode is not required to
// touch all spans that it reports.
func collectSpans(params runParams, plan planNode) (reads, writes roachpb.Spans, err error) {
	switch n := plan.(type) {
	case
		*valueGenerator,
		*valuesNode,
		*zeroNode,
		*unaryNode:
		return nil, nil, nil

	case *scanNode:
		return n.spans, nil, nil

	case *updateNode:
		return editNodeSpans(params, &n.run.editNodeRun)
	case *insertNode:
		if v, ok := n.run.editNodeRun.rows.(*valuesNode); ok {
			// subqueries, even within valuesNodes, can be arbitrarily complex,
			// so we can't run the valuesNode ahead of time if they are present.
			if v.isConst {
				return insertNodeWithValuesSpans(params, n, v)
			}
		}
		return editNodeSpans(params, &n.run.editNodeRun)
	case *upsertNode:
		return editNodeSpans(params, &n.run.editNodeRun)
	case *deleteNode:
		return editNodeSpans(params, &n.run.editNodeRun)

	case *delayedNode:
		return collectSpans(params, n.plan)
	case *distinctNode:
		return collectSpans(params, n.plan)
	case *explainDistSQLNode:
		return collectSpans(params, n.plan)
	case *explainPlanNode:
		return collectSpans(params, n.plan)
	case *showTraceNode:
		return collectSpans(params, n.plan)
	case *limitNode:
		return collectSpans(params, n.plan)
	case *spoolNode:
		return collectSpans(params, n.source)
	case *sortNode:
		return collectSpans(params, n.plan)
	case *groupNode:
		return collectSpans(params, n.plan)
	case *windowNode:
		return collectSpans(params, n.plan)
	case *ordinalityNode:
		return collectSpans(params, n.source)
	case *filterNode:
		return collectSpans(params, n.source.plan)
	case *renderNode:
		return collectSpans(params, n.source.plan)

	case *indexJoinNode:
		return indexJoinSpans(params, n)
	case *joinNode:
		return concatSpans(params, n.left.plan, n.right.plan)
	case *unionNode:
		return concatSpans(params, n.left, n.right)
	}

	panic(fmt.Sprintf("don't know how to collect spans for node %T", plan))
}

// editNodeSpans determines the read and write spans for an editNodeRun
// instance. It is conservative and assumes that anything in the table might be
// touched because it performs no predicate analysis. Interestingly, performing
// this analysis fully with optimal span bounds would reduce to the analysis
// required for predicate locking. Predicate locking is a concurrency control
// locking strategy where locks are based upon logical conditions.
//
// Where possible, we should try to specialize this analysis like we do with
// insertNodeWithValuesSpans.
func editNodeSpans(params runParams, r *editNodeRun) (reads, writes roachpb.Spans, err error) {
	readerReads, readerWrites, err := collectSpans(params, r.rows)
	if err != nil {
		return nil, nil, err
	}
	writerReads, writerWrites := tableWriterSpans(params, r.tw)

	return append(readerReads, writerReads...), append(readerWrites, writerWrites...), nil
}

func tableWriterSpans(params runParams, tw tableWriter) (reads, writes roachpb.Spans) {
	// We don't generally know which spans we will be modifying so we must be
	// conservative and assume anything in the table might change.
	tableSpans := tw.tableDesc().AllIndexSpans()
	fkReads := tw.fkSpanCollector().CollectSpans()
	return fkReads, tableSpans
}

// insertNodeWithValuesSpans is a special case of editNodeSpans. It tightens the
// predicted read and write-sets for INSERT ... VALUES statements. The function
// does so by evaluating the VALUES clause ahead of time and computing the
// specific index spans that will be touched by each VALUES tuple. valuesNode
// can not contain subqueries.
func insertNodeWithValuesSpans(
	params runParams, n *insertNode, v *valuesNode,
) (reads, writes roachpb.Spans, err error) {

	// addWriteKey adds a write span for the given index key.
	addWriteKey := func(key roachpb.Key) {
		writes = append(writes, roachpb.Span{
			Key:    key,
			EndKey: key.PrefixEnd(),
		})
	}

	// Run the valuesNode to completion while tracking its memory usage.
	// Importantly, we only Reset the valuesNode, instead of Closing it when
	// completed, so that the values don't need to be computed again during
	// plan execution.
	rowAcc := params.extendedEvalCtx.Mon.MakeBoundAccount()
	params.extendedEvalCtx.ActiveMemAcc = &rowAcc
	defer rowAcc.Close(params.ctx)

	defer v.Reset(params.ctx)
	if err = v.startExec(params); err != nil {
		return nil, nil, err
	}

	if err := forEachRow(params, v, func(values tree.Datums) error {
		// insertNode uses fillDefaults to fill all defaults if it's data source
		// is a valuesNode. This is important, because it means that the result
		// of all DEFAULT expressions will be retained from span collection to
		// plan execution.
		if a, e := len(values), len(n.insertCols); a < e {
			log.Fatalf(params.ctx, "missing columns for row; want %d, got %d", e, a)
		}

		// Determine the table spans that the current values tuple will mutate.
		ti := n.run.editNodeRun.tw.(*tableInserter)
		primaryKey, secondaryKeys, err := ti.ri.EncodeIndexesForRow(values)
		if err != nil {
			return err
		}
		addWriteKey(primaryKey)
		for _, secondaryKey := range secondaryKeys {
			addWriteKey(secondaryKey.Key)
		}

		// Determine the table spans that foreign key constraints will require
		// us to read during FK validation.
		fkReads, err := ti.fkSpanCollector().CollectSpansForValues(values)
		if err != nil {
			return err
		}
		reads = append(reads, fkReads...)
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return reads, writes, nil
}

func indexJoinSpans(params runParams, n *indexJoinNode) (reads, writes roachpb.Spans, err error) {
	indexReads, indexWrites, err := collectSpans(params, n.index)
	if err != nil {
		return nil, nil, err
	}
	if len(indexWrites) > 0 {
		return nil, nil, errors.Errorf("unexpected index scan span writes: %v", indexWrites)
	}
	// We can not be sure which spans in the table we will read based only on the
	// initial index span because we will dynamically lookup rows in the table based
	// on the result of the index scan. We conservatively report that we will read the
	// index span and the entire span for the table's primary index.
	primaryReads := n.table.desc.PrimaryIndexSpan()
	return append(indexReads, primaryReads), nil, nil
}

func concatSpans(params runParams, left, right planNode) (reads, writes roachpb.Spans, err error) {
	leftReads, leftWrites, err := collectSpans(params, left)
	if err != nil {
		return nil, nil, err
	}
	rightReads, rightWrites, err := collectSpans(params, right)
	if err != nil {
		return nil, nil, err
	}
	return append(leftReads, rightReads...), append(leftWrites, rightWrites...), nil
}
