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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
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
	b.factory.Metadata().AddUserDefinedFunction(o, f.Func.ReferenceByName)

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
	routine, isMultiColDataSource := b.buildRoutine(f, def, inScope, outScope, colRefs)

	// Synthesize an output columns if necessary.
	if outCol == nil {
		if isMultiColDataSource {
			return b.finishBuildGeneratorFunction(f, o, routine, inScope, outScope, outCol)
		}
		if outScope != nil {
			outCol = b.synthesizeColumn(outScope, scopeColName(""), f.ResolvedType(), nil /* expr */, routine)
		}
	} else if b.insideDataSource {
		// When we have a single OUT parameter, it becomes the output column
		// name.
		var firstOutParamName tree.Name
		var numOutParams int
		for _, param := range o.RoutineParams {
			if param.IsOutParam() {
				numOutParams++
				if numOutParams == 1 {
					firstOutParamName = param.Name
				}
			}
			if numOutParams == 2 {
				break
			}
		}
		if numOutParams == 1 && firstOutParamName != "" {
			outCol.name = scopeColName(firstOutParamName)
		}
	}

	if b.trackSchemaDeps {
		b.schemaFunctionDeps.Add(int(o.Oid))
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

	// Type-check the procedure and its arguments. Subqueries are disallowed in
	// arguments. Note that we don't use defer to reset semaCtx.Properties
	// because it must be reset before the call to buildRoutine below, or else
	// subqueries would be disallowed in the body of procedures.
	originalProps := b.semaCtx.Properties
	b.semaCtx.Properties.Require("CALL argument", tree.RejectSubqueries)
	typedExpr := inScope.resolveType(c.Proc, types.Any)
	b.semaCtx.Properties = originalProps
	f, ok := typedExpr.(*tree.FuncExpr)
	if !ok {
		panic(pgerror.Newf(pgcode.WrongObjectType,
			"%s is not a procedure", c.Proc.Func.String(),
		))
	}

	// Resolve the procedure reference.
	def, err := f.Func.Resolve(b.ctx, b.semaCtx.SearchPath, b.semaCtx.FunctionResolver)
	if err != nil {
		panic(err)
	}

	o := f.ResolvedOverload()
	if o.Type != tree.ProcedureRoutine {
		typeNames := make([]string, len(f.Exprs))
		for i, expr := range f.Exprs {
			typeNames[i] = expr.(tree.TypedExpr).ResolvedType().String()
		}
		panic(errors.WithHint(
			pgerror.Newf(
				pgcode.WrongObjectType,
				"%s(%s) is not a procedure", def.Name, strings.Join(typeNames, ", "),
			),
			"To call a function, use SELECT.",
		))
	}
	if b.insideSQLRoutine {
		for _, p := range o.RoutineParams {
			if tree.IsOutParamClass(p.Class) {
				panic(pgerror.New(pgcode.FeatureNotSupported,
					"calling procedures with output arguments is not supported in SQL functions",
				))
			}
		}
	}

	// Synthesize output columns for OUT parameters. We can use the return type
	// to synthesize the columns, since it's based on the OUT parameters.
	if rTyp := f.ResolvedType(); rTyp.Family() != types.VoidFamily {
		if len(rTyp.TupleContents()) == 0 {
			panic(errors.AssertionFailedf("expected procedure to return a record"))
		}
		for i := range rTyp.TupleContents() {
			colName := scopeColName(tree.Name(rTyp.TupleLabels()[i]))
			b.synthesizeColumn(outScope, colName, rTyp.TupleContents()[i], f, nil /* scalar */)
		}
	}

	// Check for execution privileges.
	if err := b.catalog.CheckExecutionPrivilege(b.ctx, o.Oid); err != nil {
		panic(err)
	}

	// Build the routine.
	routine, _ := b.buildRoutine(f, def, inScope, outScope, nil /* colRefs */)
	routine = b.finishBuildScalar(nil /* texpr */, routine, inScope,
		nil /* outScope */, nil /* outCol */)

	// Build a call expression.
	callPrivate := &memo.CallPrivate{Columns: outScope.colList()}
	outScope.expr = b.factory.ConstructCall(routine, callPrivate)
	return outScope
}

// buildRoutine returns an expression representing the invocation of a
// user-defined function or procedure. It also returns the return type of the
// routine and a boolean that is true if the routine returns multiple columns.
//
// - outScope is only used for stored procedures, specifically when there is a
// transaction control statement. This is necessary because transaction control
// statements have to construct a new CALL statement to resume execution.
func (b *Builder) buildRoutine(
	f *tree.FuncExpr,
	def *tree.ResolvedFunctionDefinition,
	inScope, outScope *scope,
	colRefs *opt.ColSet,
) (out opt.ScalarExpr, isMultiColDataSource bool) {
	o := f.ResolvedOverload()
	isProc := o.Type == tree.ProcedureRoutine
	b.factory.Metadata().AddUserDefinedFunction(o, f.Func.ReferenceByName)

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
	finishResolveType := func(lastStmtScope *scope) {
		if types.IsWildcardTupleType(rtyp) {
			if len(lastStmtScope.cols) == 1 &&
				lastStmtScope.cols[0].typ.Family() == types.TupleFamily {
				// When the final statement returns a single tuple, we can use
				// the tuple's types as the function return type.
				f.SetTypeAnnotation(lastStmtScope.cols[0].typ)
			} else {
				// Get the types from the individual columns of the last
				// statement.
				tc := make([]*types.T, len(lastStmtScope.cols))
				tl := make([]string, len(lastStmtScope.cols))
				for i, col := range lastStmtScope.cols {
					tc[i] = col.typ
					tl[i] = col.name.MetadataName()
				}
				f.SetTypeAnnotation(types.MakeLabeledTuple(tc, tl))
			}
		}
	}

	// Build the argument expressions.
	var args memo.ScalarListExpr
	if len(f.Exprs) > 0 {
		args = make(memo.ScalarListExpr, 0, len(f.Exprs))
		for i, pexpr := range f.Exprs {
			if isProc && o.RoutineParams[i].Class == tree.RoutineParamOut {
				// For procedures, OUT parameters need to be specified in the
				// CALL statement, but they are not evaluated and shouldn't be
				// passed down to the UDF Call (since the body can only
				// reference the input parameters which we refer to by their
				// ordinals).
				continue
			}
			args = append(args, b.buildScalar(
				pexpr.(tree.TypedExpr),
				inScope,
				nil, /* outScope */
				nil, /* outCol */
				colRefs,
			))
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
		// Add all input parameters to the scope.
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

	if b.trackSchemaDeps {
		b.schemaFunctionDeps.Add(int(o.Oid))
	}
	// Do not track any other routine invocations inside this routine, since
	// for the schema changer we only need depth 1. Also keep track of when
	// we have are executing inside a UDF (this could be nested so we need to
	// track the previous state).
	oldTrackingSchemaDeps := b.trackSchemaDeps
	oldInsideUDF := b.insideUDF
	defer func() {
		b.trackSchemaDeps = oldTrackingSchemaDeps
		b.insideUDF = oldInsideUDF
	}()
	b.trackSchemaDeps = false
	b.insideUDF = true
	isSetReturning := o.Class == tree.GeneratorClass
	isMultiColDataSource = false

	// Build an expression for each statement in the function body.
	var body []memo.RelExpr
	var bodyProps []*physical.Required
	var bodyStmts []string
	switch o.Language {
	case tree.RoutineLangSQL:
		// Parse the function body.
		stmts, err := parser.Parse(o.Body)
		if err != nil {
			panic(err)
		}
		// Add a VALUES (NULL) statement if the return type of the function is
		// VOID. We cannot simply project NULL from the last statement because
		// all columns would be pruned and the contents of last statement would
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

		b.withinSQLRoutine(func() {
			for i := range stmts {
				stmtScope := b.buildStmtAtRootWithScope(stmts[i].AST, nil /* desiredTypes */, bodyScope)
				expr, physProps := stmtScope.expr, stmtScope.makePhysicalProps()

				// The last statement produces the output of the UDF.
				if i == len(stmts)-1 {
					finishResolveType(stmtScope)
					expr, physProps, isMultiColDataSource =
						b.finishBuildLastStmt(stmtScope, bodyScope, isSetReturning, f)
				}
				body[i] = expr
				bodyProps[i] = physProps
			}
		})

		if b.verboseTracing {
			bodyStmts = make([]string, len(stmts))
			for i := range stmts {
				bodyStmts[i] = stmts[i].AST.String()
			}
		}
	case tree.RoutineLangPLpgSQL:
		// Parse the function body.
		stmt, err := plpgsql.Parse(o.Body)
		if err != nil {
			panic(err)
		}
		routineParams := make([]routineParam, 0, len(o.RoutineParams))
		for _, param := range o.RoutineParams {
			// TODO(yuzefovich): can we avoid type resolution here?
			typ, err := tree.ResolveType(b.ctx, param.Type, b.semaCtx.TypeResolver)
			if err != nil {
				panic(err)
			}
			routineParams = append(routineParams, routineParam{
				name:  param.Name,
				typ:   typ,
				class: param.Class,
			})
		}
		var expr memo.RelExpr
		var physProps *physical.Required
		plBuilder := newPLpgSQLBuilder(
			b, def.Name, stmt.AST.Label, colRefs, routineParams, rtyp, isProc, outScope,
		)
		stmtScope := plBuilder.buildRootBlock(stmt.AST, bodyScope, routineParams)
		finishResolveType(stmtScope)
		expr, physProps, isMultiColDataSource =
			b.finishBuildLastStmt(stmtScope, bodyScope, isSetReturning, f)
		body = []memo.RelExpr{expr}
		bodyProps = []*physical.Required{physProps}
		if b.verboseTracing {
			bodyStmts = []string{stmt.String()}
		}
	default:
		panic(errors.AssertionFailedf("unexpected language: %v", o.Language))
	}

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
				RoutineLang:        o.Language,
				Body:               body,
				BodyProps:          bodyProps,
				BodyStmts:          bodyStmts,
				Params:             params,
			},
		},
	)
	return routine, isMultiColDataSource
}

// finishBuildLastStmt manages the columns returned by the last statement of a
// UDF. Depending on the context and return type of the UDF, this may mean
// expanding a tuple into multiple columns, or combining multiple columns into
// a tuple.
func (b *Builder) finishBuildLastStmt(
	stmtScope *scope, bodyScope *scope, isSetReturning bool, f *tree.FuncExpr,
) (expr memo.RelExpr, physProps *physical.Required, isMultiColDataSource bool) {
	expr, physProps = stmtScope.expr, stmtScope.makePhysicalProps()
	rtyp := f.ResolvedType()

	// Add a LIMIT 1 to the last statement if the UDF is not
	// set-returning. This is valid because any other rows after the
	// first can simply be ignored. The limit could be beneficial
	// because it could allow additional optimization.
	if !isSetReturning {
		b.buildLimit(&tree.Limit{Count: tree.NewDInt(1)}, b.allocScope(), stmtScope)
		expr = stmtScope.expr
		// The limit expression will maintain the desired ordering, if any,
		// so the physical props ordering can be cleared. The presentation
		// must remain.
		physProps.Ordering = props.OrderingChoice{}
	}

	// Only a single column can be returned from a UDF, unless it is used as a
	// data source. Data sources may output multiple columns, and if the
	// statement body produces a tuple it needs to be expanded into columns.
	// When not used as a data source, combine statements producing multiple
	// columns into a tuple. If the last statement is already returning a
	// tuple and the function has a record return type, then we do not need to
	// wrap the output in another tuple.
	cols := physProps.Presentation
	isSingleTupleResult := len(stmtScope.cols) == 1 &&
		stmtScope.cols[0].typ.Family() == types.TupleFamily
	if b.insideDataSource && rtyp.Family() == types.TupleFamily {
		// When the UDF is used as a data source and expects to output a tuple
		// type, its output needs to be a row of columns instead of the usual
		// tuple. If the last statement output a tuple, we need to expand the
		// tuple into individual columns.
		isMultiColDataSource = true
		if isSingleTupleResult {
			stmtScope = bodyScope.push()
			elems := make([]scopeColumn, len(rtyp.TupleContents()))
			for i := range rtyp.TupleContents() {
				e := b.factory.ConstructColumnAccess(b.factory.ConstructVariable(cols[0].ID), memo.TupleOrdinal(i))
				col := b.synthesizeColumn(stmtScope, scopeColName(""), rtyp.TupleContents()[i], nil, e)
				elems[i] = *col
			}
			expr = b.constructProject(expr, elems)
			physProps = stmtScope.makePhysicalProps()
		}
	} else if len(cols) > 1 || (types.IsRecordType(rtyp) && !isSingleTupleResult) {
		// Only a single column can be returned from a UDF, unless it is used as a
		// data source (see comment above). If there are multiple columns, combine
		// them into a tuple. If the last statement is already returning a tuple
		// and the function has a record return type, then do not wrap the
		// output in another tuple.
		elems := make(memo.ScalarListExpr, len(cols))
		for i := range cols {
			elems[i] = b.factory.ConstructVariable(cols[i].ID)
		}
		tup := b.factory.ConstructTuple(elems, rtyp)
		stmtScope = bodyScope.push()
		col := b.synthesizeColumn(stmtScope, scopeColName(""), rtyp, nil /* expr */, tup)
		expr = b.constructProject(expr, []scopeColumn{*col})
		physProps = stmtScope.makePhysicalProps()
	}

	// We must preserve the presentation of columns as physical
	// properties to prevent the optimizer from pruning the output
	// column. If necessary, we add an assignment cast to the result
	// column so that its type matches the function return type. Record return
	// types do not need an assignment cast, since at this point the return
	// column is already a tuple.
	cols = physProps.Presentation
	if len(cols) > 0 {
		returnCol := physProps.Presentation[0].ID
		returnColMeta := b.factory.Metadata().ColumnMeta(returnCol)
		if !types.IsRecordType(rtyp) && !isMultiColDataSource && !returnColMeta.Type.Identical(rtyp) {
			if !cast.ValidCast(returnColMeta.Type, rtyp, cast.ContextAssignment) {
				panic(sqlerrors.NewInvalidAssignmentCastError(
					returnColMeta.Type, rtyp, returnColMeta.Alias))
			}
			cast := b.factory.ConstructAssignmentCast(
				b.factory.ConstructVariable(physProps.Presentation[0].ID),
				rtyp,
			)
			stmtScope = bodyScope.push()
			col := b.synthesizeColumn(stmtScope, scopeColName(""), rtyp, nil /* expr */, cast)
			expr = b.constructProject(expr, []scopeColumn{*col})
			physProps = stmtScope.makePhysicalProps()
		}
	}
	return expr, physProps, isMultiColDataSource
}

func (b *Builder) withinSQLRoutine(fn func()) {
	defer func(origValue bool) {
		b.insideSQLRoutine = origValue
	}(b.insideSQLRoutine)
	b.insideSQLRoutine = true
	fn()
}
