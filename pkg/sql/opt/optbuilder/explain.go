// Copyright 2018 The Cockroach Authors.
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

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
)

func (b *Builder) buildExplain(explain *tree.Explain, inScope *scope) (outScope *scope) {
	opts, err := explain.ParseOptions()
	if err != nil {
		panic(builderError{err})
	}

	// We don't allow the statement under Explain to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	stmtScope := b.buildStmt(explain.Statement, nil /* desiredTypes */, &scope{builder: b})
	outScope = inScope.push()

	var cols sqlbase.ResultColumns
	switch opts.Mode {
	case tree.ExplainPlan:
		telemetry.Inc(sqltelemetry.ExplainPlanUseCounter)
		if opts.Flags.Contains(tree.ExplainFlagVerbose) || opts.Flags.Contains(tree.ExplainFlagTypes) {
			cols = sqlbase.ExplainPlanVerboseColumns
		} else {
			cols = sqlbase.ExplainPlanColumns
		}

	case tree.ExplainDistSQL:
		analyze := opts.Flags.Contains(tree.ExplainFlagAnalyze)
		if analyze {
			telemetry.Inc(sqltelemetry.ExplainAnalyzeUseCounter)
		} else {
			telemetry.Inc(sqltelemetry.ExplainDistSQLUseCounter)
		}
		if analyze && tree.IsStmtParallelized(explain.Statement) {
			panic(pgerror.Newf(pgerror.CodeFeatureNotSupportedError,
				"EXPLAIN ANALYZE does not support RETURNING NOTHING statements"))
		}
		cols = sqlbase.ExplainDistSQLColumns

	case tree.ExplainOpt:
		if opts.Flags.Contains(tree.ExplainFlagVerbose) {
			telemetry.Inc(sqltelemetry.ExplainOptVerboseUseCounter)
		} else {
			telemetry.Inc(sqltelemetry.ExplainOptUseCounter)
		}
		cols = sqlbase.ExplainOptColumns

	default:
		panic(pgerror.Newf(pgerror.CodeFeatureNotSupportedError,
			"EXPLAIN ANALYZE does not support RETURNING NOTHING statements"))
	}
	b.synthesizeResultColumns(outScope, cols)

	input := stmtScope.expr.(memo.RelExpr)
	private := memo.ExplainPrivate{
		Options:  opts,
		ColList:  colsToColList(outScope.cols),
		Props:    stmtScope.makePhysicalProps(),
		StmtType: explain.Statement.StatementType(),
	}
	outScope.expr = b.factory.ConstructExplain(input, &private)
	return outScope
}
