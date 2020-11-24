// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

func (b *Builder) buildExplain(explain *tree.Explain, inScope *scope) (outScope *scope) {
	b.pushWithFrame()

	// We don't allow the statement under Explain to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	stmtScope := b.buildStmtAtRoot(explain.Statement, nil /* desiredTypes */, b.allocScope())

	b.popWithFrame(stmtScope)
	outScope = inScope.push()

	var cols colinfo.ResultColumns
	switch explain.Mode {
	case tree.ExplainPlan:
		telemetry.Inc(sqltelemetry.ExplainPlanUseCounter)
		cols = colinfo.ExplainPlanColumns

	case tree.ExplainDistSQL:
		telemetry.Inc(sqltelemetry.ExplainDistSQLUseCounter)
		cols = colinfo.ExplainDistSQLColumns

	case tree.ExplainOpt:
		if explain.Flags[tree.ExplainFlagVerbose] {
			telemetry.Inc(sqltelemetry.ExplainOptVerboseUseCounter)
		} else {
			telemetry.Inc(sqltelemetry.ExplainOptUseCounter)
		}
		cols = colinfo.ExplainPlanColumns

	case tree.ExplainVec:
		telemetry.Inc(sqltelemetry.ExplainVecUseCounter)
		cols = colinfo.ExplainPlanColumns

	default:
		panic(errors.Errorf("EXPLAIN mode %s not supported", explain.Mode))
	}
	b.synthesizeResultColumns(outScope, cols)

	input := stmtScope.expr.(memo.RelExpr)
	private := memo.ExplainPrivate{
		Options:  explain.ExplainOptions,
		Analyze:  false,
		ColList:  colsToColList(outScope.cols),
		Props:    stmtScope.makePhysicalProps(),
		StmtType: explain.Statement.StatementType(),
	}
	outScope.expr = b.factory.ConstructExplain(input, &private)
	return outScope
}

func (b *Builder) buildExplainAnalyze(
	explain *tree.ExplainAnalyze, inScope *scope,
) (outScope *scope) {
	if explain.Mode == tree.ExplainDebug {
		// This statement should have been handled by the executor.
		panic(errors.New("EXPLAIN ANALYZE (DEBUG) can only be used as a top-level statement"))
	}
	if explain.Mode != tree.ExplainDistSQL {
		panic(errors.Errorf("EXPLAIN ANALYZE mode %s not supported", explain.Mode))
	}
	if tree.IsStmtParallelized(explain.Statement) {
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"EXPLAIN ANALYZE does not support RETURNING NOTHING statements"))
	}

	// TODO(radu): eventually, all ANALYZE modes should be supported and handled
	// only as top-level statements.

	b.pushWithFrame()
	// We don't allow the statement under Explain to reference outer columns, so we
	// pass a "blank" scope rather than inScope.
	stmtScope := b.buildStmtAtRoot(explain.Statement, nil /* desiredTypes */, b.allocScope())

	b.popWithFrame(stmtScope)
	outScope = inScope.push()

	telemetry.Inc(sqltelemetry.ExplainAnalyzeUseCounter)
	b.synthesizeResultColumns(outScope, colinfo.ExplainDistSQLColumns)

	input := stmtScope.expr.(memo.RelExpr)
	private := memo.ExplainPrivate{
		Options:  explain.ExplainOptions,
		Analyze:  true,
		ColList:  colsToColList(outScope.cols),
		Props:    stmtScope.makePhysicalProps(),
		StmtType: explain.Statement.StatementType(),
	}
	outScope.expr = b.factory.ConstructExplain(input, &private)
	return outScope
}
