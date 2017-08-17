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
	case *traceNode:
		return collectSpans(params, n.plan)
	case *limitNode:
		return collectSpans(params, n.plan)
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

func editNodeSpans(params runParams, r *editNodeRun) (reads, writes roachpb.Spans, err error) {
	scanReads, scanWrites, err := collectSpans(params, r.rows)
	if err != nil {
		return nil, nil, err
	}
	if len(scanWrites) > 0 {
		return nil, nil, errors.Errorf("unexpected scan span writes: %v", scanWrites)
	}

	writerReads, writerWrites, err := tableWriterSpans(params, r.tw)
	if err != nil {
		return nil, nil, err
	}

	sqReads, err := collectSubquerySpans(params, r.rows)
	if err != nil {
		return nil, nil, err
	}

	return append(scanReads, append(writerReads, sqReads...)...), writerWrites, nil
}

func tableWriterSpans(params runParams, tw tableWriter) (reads, writes roachpb.Spans, err error) {
	// We don't generally know which spans we will be modifying so we must be
	// conservative and assume anything in the table might change. See TODO on
	// tableWriter.spans for discussion on constraining spans wherever possible.
	tableSpans := tw.tableDesc().AllIndexSpans()
	fkReads, fkWrites := tw.fkSpanCollector().CollectSpans()
	if len(fkWrites) > 0 {
		return nil, nil, errors.Errorf("unexpected foreign key span writes: %v", fkWrites)
	}
	return fkReads, tableSpans, nil
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
