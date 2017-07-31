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

	"golang.org/x/net/context"

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
func collectSpans(ctx context.Context, plan planNode) (reads, writes roachpb.Spans, err error) {
	switch n := plan.(type) {
	case
		*valueGenerator,
		*valuesNode,
		*emptyNode:
		return nil, nil, nil

	case *scanNode:
		return n.spans, nil, nil

	case *updateNode:
		return n.run.collectSpans(ctx)
	case *insertNode:
		return n.run.collectSpans(ctx)
	case *deleteNode:
		return n.run.collectSpans(ctx)

	case *delayedNode:
		return collectSpans(ctx, n.plan)
	case *distinctNode:
		return collectSpans(ctx, n.plan)
	case *explainDistSQLNode:
		return collectSpans(ctx, n.plan)
	case *explainPlanNode:
		return collectSpans(ctx, n.plan)
	case *traceNode:
		return collectSpans(ctx, n.plan)
	case *limitNode:
		return collectSpans(ctx, n.plan)
	case *sortNode:
		return collectSpans(ctx, n.plan)
	case *groupNode:
		return collectSpans(ctx, n.plan)
	case *windowNode:
		return collectSpans(ctx, n.plan)
	case *ordinalityNode:
		return collectSpans(ctx, n.source)
	case *filterNode:
		return collectSpans(ctx, n.source.plan)
	case *renderNode:
		return collectSpans(ctx, n.source.plan)

	case *indexJoinNode:
		return indexJoinSpans(ctx, n)
	case *joinNode:
		return concatSpans(ctx, n.left.plan, n.right.plan)
	case *unionNode:
		return concatSpans(ctx, n.left, n.right)
	}

	panic(fmt.Sprintf("don't know how to collect spans for node %T", plan))
}

func indexJoinSpans(
	ctx context.Context, n *indexJoinNode,
) (reads, writes roachpb.Spans, err error) {
	indexReads, indexWrites, err := collectSpans(ctx, n.index)
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

func concatSpans(
	ctx context.Context, left, right planNode,
) (reads, writes roachpb.Spans, err error) {
	leftReads, leftWrites, err := collectSpans(ctx, left)
	if err != nil {
		return nil, nil, err
	}
	rightReads, rightWrites, err := collectSpans(ctx, right)
	if err != nil {
		return nil, nil, err
	}
	return append(leftReads, rightReads...), append(leftWrites, rightWrites...), nil
}
