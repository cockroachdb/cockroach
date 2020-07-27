// Copyright 2018 The Cockroach Authors.
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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// planToTree returns a representation of the plan as a
// roachpb.ExplainTreePlanNode tree.
func planToTree(ctx context.Context, top *planTop) *roachpb.ExplainTreePlanNode {
	var ob explain.OutputBuilder
	observer := planObserver{
		// We set followRowSourceToPlanNode to true, to instruct the plan observer
		// to follow the edges from rowSourceToPlanNodes (indicating that the prior
		// node was not plannable by DistSQL) to the original planNodes that were
		// replaced by DistSQL nodes. This prevents the walk from ending at these
		// special replacement nodes.
		// TODO(jordan): this is pretty hacky. We should modify DistSQL physical
		//  planning to avoid mutating its input planNode tree instead.
		followRowSourceToPlanNode: true,
		enterNode: func(ctx context.Context, nodeName string, plan planNode) (bool, error) {
			if plan == nil {
				ob.EnterMetaNode(nodeName)
			} else {
				ob.EnterNode(nodeName, planColumns(plan), planReqOrdering(plan))
			}
			return true, nil
		},
		expr: func(_ observeVerbosity, nodeName, fieldName string, n int, expr tree.Expr) {
			if expr == nil {
				return
			}
			ob.AddField(fieldName, tree.AsStringWithFlags(expr, sampledLogicalPlanFmtFlags))
		},
		spans: func(nodeName, fieldName string, index *sqlbase.IndexDescriptor, spans []roachpb.Span, hardLimitSet bool) {
			// TODO(jordan): it's expensive to serialize long span
			// strings. It's unfortunate that we're still calling
			// PrettySpans, just to check to see whether the output is - or
			// not. Unfortunately it's not so clear yet how to write a
			// shorter function. Suggestions welcome.
			spanss := sqlbase.PrettySpans(index, spans, 2)
			if spanss != "" {
				if spanss == "-" {
					spanss = getAttrForSpansAll(hardLimitSet)
				} else {
					// Spans contain literal values from the query and thus
					// cannot be spelled out in the collected plan.
					spanss = fmt.Sprintf("%d span%s", len(spans), util.Pluralize(int64(len(spans))))
				}
				ob.AddField(fieldName, spanss)
			}
		},
		attr: func(nodeName, fieldName, attr string) {
			ob.AddField(fieldName, attr)
		},
		leaveNode: func(nodeName string, plan planNode) error {
			ob.LeaveNode()
			return nil
		},
	}

	if err := observePlan(
		ctx, &top.planComponents, observer, true /* returnError */, sampledLogicalPlanFmtFlags,
	); err != nil {
		panic(errors.AssertionFailedf("error while walking plan to save it to statement stats: %s", err.Error()))
	}
	return ob.BuildProtoTree()
}
