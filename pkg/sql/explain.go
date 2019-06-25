// Copyright 2015 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// Explain executes the explain statement, providing debugging and analysis
// info about the wrapped statement.
//
// Privileges: the same privileges as the statement being explained.
func (p *planner) Explain(ctx context.Context, n *tree.Explain) (planNode, error) {
	opts, err := n.ParseOptions()
	if err != nil {
		return nil, err
	}

	defer func(save bool) { p.extendedEvalCtx.SkipNormalize = save }(p.extendedEvalCtx.SkipNormalize)
	p.extendedEvalCtx.SkipNormalize = opts.Flags.Contains(tree.ExplainFlagNoNormalize)

	switch opts.Mode {
	case tree.ExplainDistSQL:
		analyze := opts.Flags.Contains(tree.ExplainFlagAnalyze)
		if analyze && tree.IsStmtParallelized(n.Statement) {
			// TODO(nvanbenschoten): Lift this restriction. Then we
			// can remove tree.IsStmtParallelized.
			return nil, errors.New("EXPLAIN ANALYZE does not support RETURNING NOTHING statements")
		}
		// Build the plan for the query being explained.  We want to capture
		// all the analyzed sub-queries in the explain node, so we are going
		// to override the planner's subquery plan slice.
		defer func(s []subquery) { p.curPlan.subqueryPlans = s }(p.curPlan.subqueryPlans)
		p.curPlan.subqueryPlans = nil
		plan, err := p.newPlan(ctx, n.Statement, nil)
		if err != nil {
			return nil, err
		}
		return &explainDistSQLNode{
			plan:               plan,
			subqueryPlans:      p.curPlan.subqueryPlans,
			optimizeSubqueries: true,
			analyze:            analyze,
			stmtType:           n.Statement.StatementType(),
		}, nil

	case tree.ExplainPlan:
		if opts.Flags.Contains(tree.ExplainFlagAnalyze) {
			return nil, errors.New("EXPLAIN ANALYZE only supported with (DISTSQL) option")
		}
		// We may want to show placeholder types, so allow missing values.
		p.semaCtx.Placeholders.PermitUnassigned()
		return p.makeExplainPlanNode(ctx, &opts, n.Statement)

	case tree.ExplainOpt:
		return nil, errors.New("EXPLAIN (OPT) only supported with the cost-based optimizer")

	default:
		return nil, fmt.Errorf("unsupported EXPLAIN mode: %d", opts.Mode)
	}
}
