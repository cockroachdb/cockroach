// Copyright 2015 The Cockroach Authors.
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
		if analyze && IsStmtParallelized(n.Statement) {
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
