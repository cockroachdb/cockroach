// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// buildUDF builds a set of memo groups that represents a procedure invocation.
func (b *Builder) buildProcedure(c *tree.Call, inScope *scope) *scope {
	// Disable memo reuse. Note that this is not strictly necessary because
	// optPlanningCtx does not attempt to reuse tree.Call statements, but exists
	// for explicitness.
	//
	// TODO(mgartner): Enable memo reuse with CALL statements. This will require
	// adding the resolved routine overload to the metadata so that we can track
	// when a statement is stale.
	b.DisableMemoReuse = true
	outScope := inScope.push()

	// Resolve the procedure.
	o, err := b.catalog.ResolveProcedure(b.ctx, c.Name, &b.evalCtx.SessionData().SearchPath)
	if err != nil {
		panic(err)
	}

	// Build the routine.
	routine := b.buildProcUDF(c, o, inScope)

	// Build a call expression.
	outScope.expr = b.factory.ConstructCall(routine)
	return outScope
}

func (b *Builder) buildProcUDF(
	c *tree.Call, o *tree.Overload, inScope *scope,
) (out opt.ScalarExpr) {
	// TODO(mgartner): Build argument expressions.
	var args memo.ScalarListExpr
	if len(c.Exprs) > 0 {
		panic(unimplemented.New("CALL", "procedures with arguments not supported"))
	}

	// Create a new scope for building the statements in the function body. We
	// start with an empty scope because a statement in the function body cannot
	// refer to anything from the outer expression.
	//
	// TODO(mgartner): We may need to set bodyScope.atRoot=true to prevent
	// CTEs that mutate and are not at the top-level.
	bodyScope := b.allocScope()

	// TODO(mgartner): Once other UDFs can be referenced from within a UDF, a
	// boolean will not be sufficient to track whether or not we are in a UDF.
	// We'll need to track the depth of the UDFs we are building expressions
	// within.
	// TODO(mgartner): Rename insideUDF.
	b.insideUDF = true
	isSetReturning := o.Class == tree.GeneratorClass
	isMultiColDataSource := false

	// Build an expression for each statement in the function body.
	var body []memo.RelExpr
	var bodyProps []*physical.Required
	switch o.Language {
	case tree.RoutineLangSQL:
		// Parse the function body.
		stmts, err := parser.Parse(o.Body)
		if err != nil {
			panic(err)
		}
		body = make([]memo.RelExpr, len(stmts))
		bodyProps = make([]*physical.Required, len(stmts))

		for i := range stmts {
			stmtScope := b.buildStmtAtRootWithScope(stmts[i].AST, nil /* desiredTypes */, bodyScope)
			expr, physProps := stmtScope.expr, stmtScope.makePhysicalProps()
			body[i] = expr
			bodyProps[i] = physProps
		}
	case tree.RoutineLangPLpgSQL:
		// TODO(mgartner): Add support for PLpgSQL procedures.
		if o.IsProcedure {
			panic(unimplemented.New("CALL", "PLpgSQL procedures not supported"))
		}
	default:
		panic(errors.AssertionFailedf("unexpected language: %v", o.Language))
	}

	b.insideUDF = false

	// TODO(mgartner): Build argument expressions.
	var params opt.ColList
	out = b.factory.ConstructUDFCall(
		args,
		&memo.UDFCallPrivate{
			Def: &memo.UDFDefinition{
				Name:               c.Name.String(),
				Typ:                types.Void,
				Volatility:         o.Volatility,
				SetReturning:       isSetReturning,
				CalledOnNullInput:  o.CalledOnNullInput,
				MultiColDataSource: isMultiColDataSource,
				Body:               body,
				BodyProps:          bodyProps,
				Params:             params,
			},
		},
	)

	return b.finishBuildScalar(nil /* texpr */, out, inScope, nil /* outScope */, nil /* outCol */)
}
