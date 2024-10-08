// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// buildUDF builds a set of memo groups that represents a user-defined function
// invocation.
//
// TODO(mgartner): This function also builds built-in functions defined with a
// SQL body. Consider renaming it to make it more clear.
func (b *Builder) buildUDF(
	f *tree.FuncExpr,
	def *tree.ResolvedFunctionDefinition,
	inScope, outScope *scope,
	outCol *scopeColumn,
	colRefs *opt.ColSet,
) opt.ScalarExpr {
	o := f.ResolvedOverload()
	b.factory.Metadata().AddUserDefinedRoutine(o, f.Func.ReferenceByName)

	if o.Type == tree.ProcedureRoutine {
		panic(errors.WithHint(
			pgerror.Newf(
				pgcode.WrongObjectType,
				"%s(%s) is a procedure", def.Name, o.Types.String(),
			),
			"To call a procedure, use CALL.",
		))
	}

	// Check for execution privileges for user-defined overloads. Built-in
	// overloads do not need to be checked.
	if o.Type == tree.UDFRoutine {
		if err := b.catalog.CheckExecutionPrivilege(b.ctx, o.Oid); err != nil {
			panic(err)
		}
	}

	// Build the routine.
	routine, rtyp, isMultiColDataSource := b.buildRoutine(f, def, inScope, colRefs)

	// Synthesize an output columns if necessary.
	if outCol == nil {
		if isMultiColDataSource {
			// TODO(harding): Add the returns record property during create function.
			f.ResolvedOverload().ReturnsRecordType = types.IsRecordType(rtyp)
			return b.finishBuildGeneratorFunction(f, f.ResolvedOverload(), routine, inScope, outScope, outCol)
		}
		if outScope != nil {
			outCol = b.synthesizeColumn(outScope, scopeColName(""), f.ResolvedType(), nil /* expr */, routine)
		}
	}

	return b.finishBuildScalar(f, routine, inScope, outScope, outCol)
}

// buildProcedure builds a set of memo groups that represents a procedure
// invocation.
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

	// Type-check the procedure.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require("CALL argument", tree.RejectSubqueries)
	b.semaCtx.Properties.Ancestors.Push(tree.CallAncestor)
	typedExpr, err := tree.TypeCheck(b.ctx, c.Proc, b.semaCtx, types.Any)
	if err != nil {
		panic(err)
	}
	f, ok := typedExpr.(*tree.FuncExpr)
	if !ok {
		panic(errors.AssertionFailedf("expected FuncExpr"))
	}

	// Resolve the procedure reference.
	def, err := f.Func.Resolve(b.ctx, b.semaCtx.SearchPath, b.semaCtx.FunctionResolver)
	if err != nil {
		panic(err)
	}

	o := f.ResolvedOverload()
	if o.Type != tree.ProcedureRoutine {
		panic(errors.WithHint(
			pgerror.Newf(
				pgcode.WrongObjectType,
				"%s(%s) is not a procedure", def.Name, o.Types.String(),
			),
			"To call a function, use SELECT.",
		))
	}

	// Check for execution privileges.
	if err := b.catalog.CheckExecutionPrivilege(b.ctx, o.Oid); err != nil {
		panic(err)
	}

	// Build the routine.
	routine, _, _ := b.buildRoutine(c.Proc, def, inScope, nil /* colRefs */)
	routine = b.finishBuildScalar(nil /* texpr */, routine, inScope,
		nil /* outScope */, nil /* outCol */)

	// Build a call expression.
	outScope.expr = b.factory.ConstructCall(routine)
	return outScope
}

// buildRoutine returns an expression representing the invocation of a
// user-defined function or procedure. It also returns the return type of the
// routine and a boolean that is true if the routine returns multiple columns.
func (b *Builder) buildRoutine(
	f *tree.FuncExpr, def *tree.ResolvedFunctionDefinition, inScope *scope, colRefs *opt.ColSet,
) (out opt.ScalarExpr, returnType *types.T, isMultiColDataSource bool) {
	o := f.ResolvedOverload()
	b.factory.Metadata().AddUserDefinedRoutine(o, f.Func.ReferenceByName)

	// Validate that the return types match the original return types defined in
	// the function. Return types like user defined return types may change
	// since the function was first created.
	rtyp := f.ResolvedType()
	if rtyp.UserDefined() {
		funcReturnType, err := tree.ResolveType(b.ctx,
			&tree.OIDTypeReference{OID: rtyp.Oid()}, b.semaCtx.TypeResolver)
		if err != nil {
			panic(err)
		}
		if !funcReturnType.Identical(rtyp) {
			panic(pgerror.Newf(
				pgcode.InvalidFunctionDefinition,
				"return type mismatch in function declared to return %s", rtyp.Name()))
		}
	}
	// If returning a RECORD type, the function return type needs to be modified
	// because when we first parse the CREATE FUNCTION, the RECORD is
	// represented as a tuple with any types and execution requires the types to
	// be concrete in order to decode them correctly. We can determine the types
	// from the result columns or tuple of the last statement.
	finishResolveType := func(lastStmtScope *scope) *types.T {
		if types.IsRecordType(rtyp) {
			if len(lastStmtScope.cols) == 1 &&
				lastStmtScope.cols[0].typ.Family() == types.TupleFamily {
				// When the final statement returns a single tuple, we can use
				// the tuple's types as the function return type.
				rtyp = lastStmtScope.cols[0].typ
			} else {
				// Get the types from the individual columns of the last
				// statement.
				tc := make([]*types.T, len(lastStmtScope.cols))
				tl := make([]string, len(lastStmtScope.cols))
				for i, col := range lastStmtScope.cols {
					tc[i] = col.typ
					tl[i] = col.name.MetadataName()
				}
				rtyp = types.MakeLabeledTuple(tc, tl)
			}
			f.SetTypeAnnotation(rtyp)
		}
		return rtyp
	}

	// Build the argument expressions.
	var args memo.ScalarListExpr
	if len(f.Exprs) > 0 {
		args = make(memo.ScalarListExpr, len(f.Exprs))
		for i, pexpr := range f.Exprs {
			args[i] = b.buildScalar(
				pexpr.(tree.TypedExpr),
				inScope,
				nil, /* outScope */
				nil, /* outCol */
				colRefs,
			)
		}
	}

	// Create a new scope for building the statements in the function body. We
	// start with an empty scope because a statement in the function body cannot
	// refer to anything from the outer expression. If there are function
	// parameters, we add them as columns to the scope so that references to
	// them can be resolved.
	//
	// TODO(mgartner): We may need to set bodyScope.atRoot=true to prevent
	// CTEs that mutate and are not at the top-level.
	bodyScope := b.allocScope()
	var params opt.ColList
	if o.Types.Length() > 0 {
		paramTypes, ok := o.Types.(tree.ParamTypes)
		if !ok {
			panic(unimplemented.NewWithIssue(88947,
				"variadiac user-defined functions are not yet supported"))
		}
		params = make(opt.ColList, len(paramTypes))
		for i := range paramTypes {
			paramType := &paramTypes[i]
			argColName := funcParamColName(tree.Name(paramType.Name), i)
			col := b.synthesizeColumn(bodyScope, argColName, paramType.Typ, nil /* expr */, nil /* scalar */)
			col.setParamOrd(i)
			params[i] = col.id
		}
	}

	// TODO(mgartner): Once other UDFs can be referenced from within a UDF, a
	// boolean will not be sufficient to track whether or not we are in a UDF.
	// We'll need to track the depth of the UDFs we are building expressions
	// within.
	b.insideUDF = true
	isSetReturning := o.Class == tree.GeneratorClass
	isMultiColDataSource = false

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
		// Add a VALUES (NULL) statement if the return type of the function is
		// VOID. We cannot simply project NULL from the last statement because
		// all column would be pruned and the contents of last statement would
		// not be executed.
		// TODO(mgartner): This will add some planning overhead for every
		// invocation of the function. Is there a more efficient way to do this?
		if rtyp.Family() == types.VoidFamily {
			stmts = append(stmts, statements.Statement[tree.Statement]{
				AST: &tree.Select{
					Select: &tree.ValuesClause{
						Rows: []tree.Exprs{{tree.DNull}},
					},
				},
			})
		}
		body = make([]memo.RelExpr, len(stmts))
		bodyProps = make([]*physical.Required, len(stmts))

		for i := range stmts {
			stmtScope := b.buildStmtAtRootWithScope(stmts[i].AST, nil /* desiredTypes */, bodyScope)
			expr, physProps := stmtScope.expr, stmtScope.makePhysicalProps()

			// The last statement produces the output of the UDF.
			if i == len(stmts)-1 {
				rtyp = finishResolveType(stmtScope)
				expr, physProps, isMultiColDataSource =
					b.finishBuildLastStmt(stmtScope, bodyScope, isSetReturning, f)
			}
			body[i] = expr
			bodyProps[i] = physProps
		}
	case tree.RoutineLangPLpgSQL:
		// Parse the function body.
		stmt, err := plpgsql.Parse(o.Body)
		if err != nil {
			panic(err)
		}
		// Add a RETURN NULL statement if the return type of the function is
		// VOID and the last statement is not already a RETURN statement. This
		// ensures that all possible code paths lead to a RETURN statement.
		// TODO(#108298): There is a parsing bug that affects some PLpgSQL
		// functions with VOID return types.
		if rtyp.Family() == types.VoidFamily {
			lastStmt := stmt.AST.Body[len(stmt.AST.Body)-1]
			if _, ok := lastStmt.(*plpgsqltree.Return); !ok {
				stmt.AST.Body = append(stmt.AST.Body, &plpgsqltree.Return{Expr: tree.DNull})
			}
		}
		var expr memo.RelExpr
		var physProps *physical.Required
		var plBuilder plpgsqlBuilder
		plBuilder.init(b, colRefs, o.Types.(tree.ParamTypes), stmt.AST, rtyp)
		stmtScope := plBuilder.build(stmt.AST, bodyScope)
		rtyp = finishResolveType(stmtScope)
		expr, physProps, isMultiColDataSource =
			b.finishBuildLastStmt(stmtScope, bodyScope, isSetReturning, f)
		body = []memo.RelExpr{expr}
		bodyProps = []*physical.Required{physProps}
	default:
		panic(errors.AssertionFailedf("unexpected language: %v", o.Language))
	}

	b.insideUDF = false

	routine := b.factory.ConstructUDFCall(
		args,
		&memo.UDFCallPrivate{
			Def: &memo.UDFDefinition{
				Name:               def.Name,
				Typ:                f.ResolvedType(),
				Volatility:         o.Volatility,
				SetReturning:       isSetReturning,
				CalledOnNullInput:  o.CalledOnNullInput,
				MultiColDataSource: isMultiColDataSource,
				RoutineType:        o.Type,
				Body:               body,
				BodyProps:          bodyProps,
				Params:             params,
			},
		},
	)
	return routine, rtyp, isMultiColDataSource
}
