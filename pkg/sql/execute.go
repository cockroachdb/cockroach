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
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Execute creates a plan for an execute statement by substituting the plan for
// the prepared statement. This is not called in normal circumstances by
// the executor - it merely exists to enable explains and traces for execute
// statements.
func (p *planner) Execute(ctx context.Context, n *tree.Execute) (planNode, error) {
	if p.isPreparing {
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidPreparedStatementDefinitionError,
			"can't prepare an EXECUTE statement")
	} else if p.curPlan.plannedExecute {
		return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"can't have more than 1 EXECUTE per statement")
	}
	p.curPlan.plannedExecute = true
	ps, newPInfo, err := getPreparedStatementForExecute(
		p.preparedStatements, p.EvalContext().SessionData.SearchPath, n,
	)
	if err != nil {
		return nil, err
	}

	p.semaCtx.Placeholders.Assign(newPInfo)

	return p.newPlan(ctx, ps.Statement, nil /* desiredTypes */)
}

// getPreparedStatementForExecute implements the EXECUTE foo(args) SQL
// statement, returning the referenced prepared statement and correctly updated
// placeholder info.
// See https://www.postgresql.org/docs/current/static/sql-execute.html for details.
func getPreparedStatementForExecute(
	preparedStmts *PreparedStatements, searchPath sessiondata.SearchPath, s *tree.Execute,
) (ps *PreparedStatement, pInfo *tree.PlaceholderInfo, err error) {
	name := s.Name.String()
	prepared, ok := preparedStmts.Get(name)
	if !ok {
		return ps, pInfo, pgerror.NewErrorf(pgerror.CodeInvalidSQLStatementNameError,
			"prepared statement %q does not exist", name)
	}

	if len(prepared.TypeHints) != len(s.Params) {
		return ps, pInfo, pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"wrong number of parameters for prepared statement %q: expected %d, got %d",
			name, len(prepared.TypeHints), len(s.Params))
	}

	qArgs := make(tree.QueryArguments, len(s.Params))
	var t transform.ExprTransformContext
	for i, e := range s.Params {
		idx := strconv.Itoa(i + 1)
		typedExpr, err := sqlbase.SanitizeVarFreeExpr(e, prepared.TypeHints[idx], "EXECUTE parameter", nil /* semaCtx */, nil /* evalCtx */)
		if err != nil {
			return ps, pInfo, pgerror.NewError(pgerror.CodeWrongObjectTypeError, err.Error())
		}
		if err := t.AssertNoAggregationOrWindowing(
			typedExpr, "EXECUTE parameters", searchPath,
		); err != nil {
			return ps, pInfo, err
		}
		qArgs[idx] = typedExpr
	}
	return prepared, &tree.PlaceholderInfo{
			Values:    qArgs,
			TypeHints: prepared.TypeHints,
			Types:     prepared.Types},
		nil
}
