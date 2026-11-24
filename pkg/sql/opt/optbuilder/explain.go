// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	if _, ok := explain.Statement.(*tree.Execute); ok {
		// EXPLAIN (FINGERPRINT) EXECUTE is supported, but other modes are not.
		if explain.Mode != tree.ExplainFingerprint {
			panic(pgerror.New(
				pgcode.FeatureNotSupported, "EXPLAIN EXECUTE is not supported; use EXPLAIN ANALYZE",
			))
		}
	}

	var stmtScope *scope
	if explain.Mode == tree.ExplainFingerprint {
		// We don't actually need to build the statement for EXPLAIN (FINGERPRINT),
		// so don't. This allows someone to run EXPLAIN (FINGERPRINT) for statements
		// they don't have permission to execute, for example. Instead, we create a
		// dummy empty VALUES clause as input.
		emptyValues := &tree.LiteralValuesClause{Rows: tree.RawRows{}}
		stmtScope = b.buildLiteralValuesClause(emptyValues, nil /* desiredTypes */, inScope)
	} else {
		stmtScope = b.buildStmtAtRoot(explain.Statement, nil /* desiredTypes */)
	}

	outScope = inScope.push()

	switch explain.Mode {
	case tree.ExplainPlan:
		telemetry.Inc(sqltelemetry.ExplainPlanUseCounter)

	case tree.ExplainDistSQL:
		telemetry.Inc(sqltelemetry.ExplainDistSQLUseCounter)

	case tree.ExplainOpt:
		if explain.Flags[tree.ExplainFlagVerbose] {
			telemetry.Inc(sqltelemetry.ExplainOptVerboseUseCounter)
		} else {
			telemetry.Inc(sqltelemetry.ExplainOptUseCounter)
		}

	case tree.ExplainVec:
		telemetry.Inc(sqltelemetry.ExplainVecUseCounter)

	case tree.ExplainDDL:
		if explain.Flags[tree.ExplainFlagViz] {
			telemetry.Inc(sqltelemetry.ExplainDDLViz)
		} else if explain.Flags[tree.ExplainFlagVerbose] {
			telemetry.Inc(sqltelemetry.ExplainDDLVerbose)
		} else {
			telemetry.Inc(sqltelemetry.ExplainDDL)
		}

	case tree.ExplainGist:
		telemetry.Inc(sqltelemetry.ExplainGist)

	case tree.ExplainFingerprint:
		telemetry.Inc(sqltelemetry.ExplainFingerprint)

	default:
		panic(errors.Errorf("EXPLAIN mode %s not supported", explain.Mode))
	}
	b.synthesizeResultColumns(outScope, colinfo.ExplainPlanColumns)

	input := stmtScope.expr
	private := memo.ExplainPrivate{
		Options:  explain.ExplainOptions,
		ColList:  colsToColList(outScope.cols),
		Props:    stmtScope.makePhysicalProps(),
		StmtType: explain.Statement.StatementReturnType(),
	}
	if explain.Mode == tree.ExplainFingerprint {
		stmtFingerprintFmtMask := tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&b.evalCtx.Settings.SV))
		private.Fingerprint = tree.FormatStatementHideConstants(explain.Statement, stmtFingerprintFmtMask)
	}
	outScope.expr = b.factory.ConstructExplain(input, &private)
	return outScope
}
