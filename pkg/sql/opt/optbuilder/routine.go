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
	routine := b.buildRoutine(f, def, inScope, outScope, colRefs)

	// Synthesize an output columns if necessary.
	if outCol == nil {
		if b.insideDataSource && len(f.ResolvedType().TupleContents()) > 0 {
			return b.finishBuildGeneratorFunction(f, routine, inScope, outScope, outCol)
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

	// Type check and resolve the procedure.
	proc, def := b.resolveProcedureDefinition(inScope, c.Proc)

	// Synthesize output columns for OUT parameters. We can use the return type
	// to synthesize the columns, since it's based on the OUT parameters.
	if rTyp := proc.ResolvedType(); rTyp.Family() != types.VoidFamily {
		if len(rTyp.TupleContents()) == 0 {
			panic(errors.AssertionFailedf("expected procedure to return a record"))
		}
		for i := range rTyp.TupleContents() {
			colName := scopeColName(tree.Name(rTyp.TupleLabels()[i]))
			b.synthesizeColumn(outScope, colName, rTyp.TupleContents()[i], proc, nil /* scalar */)
		}
	}

	// Build the routine.
	routine := b.buildRoutine(proc, def, inScope, outScope, nil /* colRefs */)
	routine = b.finishBuildScalar(nil /* texpr */, routine, inScope,
		nil /* outScope */, nil /* outCol */)

	// Build a call expression.
	callPrivate := &memo.CallPrivate{Columns: outScope.colList()}
	outScope.expr = b.factory.ConstructCall(routine, callPrivate)
	return outScope
}

// resolveProcedureDefinition type-checks and resolves the given procedure
// reference, and checks its privileges.
func (b *Builder) resolveProcedureDefinition(
	inScope *scope, proc *tree.FuncExpr,
) (f *tree.FuncExpr, def *tree.ResolvedFunctionDefinition) {
	// Type-check the procedure and its arguments. Subqueries are disallowed in
	// arguments.
	typedExpr := inScope.resolveTypeAndReject(proc, types.Any,
		"CALL argument", tree.RejectSubqueries)
	f, ok := typedExpr.(*tree.FuncExpr)
	if !ok {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%s is not a procedure", proc.Func.String()))
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

	// Check for execution privileges.
	if err := b.catalog.CheckExecutionPrivilege(b.ctx, o.Oid); err != nil {
		panic(err)
	}
	return f, def
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
) opt.ScalarExpr {
	o := f.ResolvedOverload()
	isProc := o.Type == tree.ProcedureRoutine
	invocationTypes := make([]*types.T, len(f.Exprs))
	for i, expr := range f.Exprs {
		texpr, ok := expr.(tree.TypedExpr)
		if !ok {
			panic(errors.AssertionFailedf("expected input expressions to be already type-checked"))
		}
		invocationTypes[i] = texpr.ResolvedType()
	}
	b.factory.Metadata().AddUserDefinedFunction(o, invocationTypes, f.Func.ReferenceByName)

	// Validate that the return types match the original return types defined in
	// the function. Return types like user defined return types may change
	// since the function was first created.
	originalReturnType := f.ResolvedType()
	if originalReturnType.UserDefined() {
		funcReturnType, err := tree.ResolveType(b.ctx,
			&tree.OIDTypeReference{OID: originalReturnType.Oid()}, b.semaCtx.TypeResolver)
		if err != nil {
			panic(err)
		}
		if !funcReturnType.Identical(originalReturnType) {
			panic(pgerror.Newf(
				pgcode.InvalidFunctionDefinition,
				"return type mismatch in function declared to return %s", originalReturnType.Name(),
			))
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
		// Check whether some arguments were omitted. We need to use the
		// corresponding DEFAULT expressions if so.
		if numDefaultsToUse := o.Types.Length() - len(args); numDefaultsToUse > 0 {
			var defaultParamOrdinals []int
			for i, param := range o.RoutineParams {
				if param.DefaultVal != nil {
					defaultParamOrdinals = append(defaultParamOrdinals, i)
				}
			}
			if len(defaultParamOrdinals) < numDefaultsToUse {
				panic(errors.AssertionFailedf(
					"incorrect overload resolution:\nneeded args: %v\nprovided args: %v\nroutine params: %v",
					o.Types, f.Exprs, o.RoutineParams,
				))
			}
			// Skip parameters for which the arguments were specified
			// explicitly.
			defaultParamOrdinals = defaultParamOrdinals[len(defaultParamOrdinals)-numDefaultsToUse:]
			for _, paramOrdinal := range defaultParamOrdinals {
				param := o.RoutineParams[paramOrdinal]
				if !param.IsInParam() {
					// Such a routine shouldn't have been created in the first
					// place.
					panic(errors.AssertionFailedf(
						"non-input routine parameter %d has DEFAULT expression: %v",
						paramOrdinal, o.RoutineParams,
					))
				}
				// TODO(yuzefovich): parameter type resolution logic is
				// partially duplicated with handling of PLpgSQL routines below.
				typ, err := tree.ResolveType(b.ctx, param.Type, b.semaCtx.TypeResolver)
				if err != nil {
					panic(err)
				}
				texpr, err := tree.TypeCheck(b.ctx, param.DefaultVal, b.semaCtx, typ)
				if err != nil {
					panic(err)
				}
				arg := b.buildScalar(texpr, inScope, nil /* outScope */, nil /* outCol */, colRefs)
				if resolved := texpr.ResolvedType(); !resolved.Identical(typ) {
					if !cast.ValidCast(resolved, typ, cast.ContextAssignment) {
						// Missing assignment cast between these two types
						// should've been caught earlier during creation time.
						panic(errors.AssertionFailedf(
							"DEFAULT expression has type %s, need type %s, assignment cast isn't possible",
							resolved.SQLStringForError(), typ.SQLStringForError(),
						))
					}
					arg = b.factory.ConstructCast(arg, typ)
				}
				args = append(args, arg)
			}
		}
		// Add all input parameters to the scope.
		paramTypes, ok := o.Types.(tree.ParamTypes)
		if !ok {
			panic(unimplemented.NewWithIssue(88947,
				"variadiac user-defined functions are not yet supported"))
		}
		if len(paramTypes) != len(args) {
			panic(errors.AssertionFailedf(
				"different number of static parameters %d and actual arguments %d", len(paramTypes), len(args),
			))
		}
		params = make(opt.ColList, len(paramTypes))
		for i := range paramTypes {
			paramType := &paramTypes[i]
			argColName := funcParamColName(tree.Name(paramType.Name), i)
			// Use the statically defined parameter type (unless it is a wildcard, in
			// which case we use the actual argument type).
			argType := paramType.Typ
			if argType.IsWildcardType() {
				argType = args[i].DataType()
			}
			col := b.synthesizeColumn(bodyScope, argColName, argType, nil /* expr */, nil /* scalar */)
			col.setParamOrd(i)
			params[i] = col.id
		}
	}

	if b.trackSchemaDeps {
		b.schemaFunctionDeps.Add(int(o.Oid))
	}
	// Do not track any other routine invocations inside this routine, since
	// for the schema changer we only need depth 1. Also keep track of when
	// we have are executing inside a UDF, and whether the routine is used as a
	// data source (this could be nested, so we need to track the previous state).
	defer func(trackSchemaDeps, insideUDF, insideDataSource bool) {
		b.trackSchemaDeps = trackSchemaDeps
		b.insideUDF = insideUDF
		b.insideDataSource = insideDataSource
	}(b.trackSchemaDeps, b.insideUDF, b.insideDataSource)
	oldInsideDataSource := b.insideDataSource
	b.insideDataSource = false
	b.trackSchemaDeps = false
	b.insideUDF = true
	isSetReturning := o.Class == tree.GeneratorClass

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
		if originalReturnType.Family() == types.VoidFamily {
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
					expr, physProps = b.finishBuildLastStmt(
						stmtScope, bodyScope, inScope, isSetReturning, oldInsideDataSource, f,
					)
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
			b, def.Name, stmt.AST.Label, colRefs, routineParams, originalReturnType, isProc, outScope,
		)
		stmtScope := plBuilder.buildRootBlock(stmt.AST, bodyScope, routineParams)
		expr, physProps = b.finishBuildLastStmt(
			stmtScope, bodyScope, inScope, isSetReturning, oldInsideDataSource, f,
		)
		body = []memo.RelExpr{expr}
		bodyProps = []*physical.Required{physProps}
		if b.verboseTracing {
			bodyStmts = []string{stmt.String()}
		}
	default:
		panic(errors.AssertionFailedf("unexpected language: %v", o.Language))
	}

	// NOTE: originalReturnType may not be up-to-date after the last statement is
	// built, so we call f.ResolvedType() again here.
	rTyp := f.ResolvedType()
	multiColDataSource := len(rTyp.TupleContents()) > 0 && oldInsideDataSource
	routine := b.factory.ConstructUDFCall(
		args,
		&memo.UDFCallPrivate{
			Def: &memo.UDFDefinition{
				Name:               def.Name,
				Typ:                rTyp,
				Volatility:         o.Volatility,
				SetReturning:       isSetReturning,
				CalledOnNullInput:  o.CalledOnNullInput,
				MultiColDataSource: multiColDataSource,
				RoutineType:        o.Type,
				RoutineLang:        o.Language,
				Body:               body,
				BodyProps:          bodyProps,
				BodyStmts:          bodyStmts,
				Params:             params,
			},
		},
	)
	return routine
}

// finishBuildLastStmt manages the columns returned by the last statement of a
// routine. Depending on the context and return type of the routine, this may
// mean expanding a tuple into multiple columns, or combining multiple columns
// into a tuple.
//
// finishBuildLastStmt also determines the final return type for the routine
// based on the last statement's result columns, and updates the type annotation
// for the FuncExpr accordingly.
func (b *Builder) finishBuildLastStmt(
	stmtScope, bodyScope, inScope *scope, isSetReturning, insideDataSource bool, f *tree.FuncExpr,
) (expr memo.RelExpr, physProps *physical.Required) {
	// After this call to finalizeRoutineReturnType, the type annotation will
	// reflect the final resolved type of the function.
	//
	// NOTE: the result columns of the last statement may not reflect this type
	// until after the call to maybeAddRoutineAssignmentCasts. Therefore, the
	// logic below must take care in distinguishing the resolved return type from
	// the result column type(s).
	b.finalizeRoutineReturnType(f, stmtScope, inScope, insideDataSource)
	expr, physProps = stmtScope.expr, stmtScope.makePhysicalProps()
	rTyp := f.ResolvedType()

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

	// Depending on the context in which the UDF was called, it may be necessary
	// to either combine multiple result columns into a tuple, or to expand a
	// tuple result column into multiple columns.
	cols := physProps.Presentation
	scopeCols := stmtScope.cols
	isSingleTupleResult := len(scopeCols) == 1 && scopeCols[0].typ.Family() == types.TupleFamily
	if insideDataSource {
		// The UDF is a data source. If it returns a composite type and the last
		// statement returns a single tuple column, the elements of the column
		// should be expanded into individual columns.
		if rTyp.Family() == types.TupleFamily && isSingleTupleResult {
			expr, physProps = b.expandRoutineTupleIntoCols(cols[0].ID, bodyScope.push(), expr)
		}
	} else {
		// Only a single column can be returned from a routine, unless it is a UDF
		// used as a data source (see comment above). There are three cases in which
		// we must wrap the column(s) from the last statement into a single tuple:
		//   1. The last statement has multiple result columns.
		//   2. The routine returns RECORD, and the (single) result column cannot
		//      be coerced to the return type. Note that a procedure with OUT-params
		//      always wraps the OUT-param types in a record.
		if len(cols) > 1 || (rTyp.Family() == types.TupleFamily && !scopeCols[0].typ.Equivalent(rTyp) &&
			!cast.ValidCast(scopeCols[0].typ, rTyp, cast.ContextAssignment)) {
			expr, physProps = b.combineRoutineColsIntoTuple(cols, bodyScope.push(), expr)
		}
	}

	// We must preserve the presentation of columns as physical properties to
	// prevent the optimizer from pruning the output column(s). If necessary, we
	// add an assignment cast to the result column(s) so that its type matches the
	// function return type.
	cols = physProps.Presentation
	return b.maybeAddRoutineAssignmentCasts(cols, bodyScope, rTyp, expr, physProps, insideDataSource)
}

// finalizeRoutineReturnType updates the routine's return type, taking into
// account the result columns of the last statement, as well as the column
// definition list if one was specified.
func (b *Builder) finalizeRoutineReturnType(
	f *tree.FuncExpr, stmtScope, inScope *scope, insideDataSource bool,
) {
	// If the function was defined using the wildcard RETURNS RECORD option with
	// no OUT-parameters, its actual return type is inferred either from a
	// column-definition list or from the types of the columns in the last
	// statement. This is necessary because wildcard types are only valid during
	// type-checking; the execution engine cannot handle them.
	rTyp := f.ResolvedType()
	if rTyp.Identical(types.AnyTuple) {
		if len(stmtScope.cols) == 1 && stmtScope.cols[0].typ.Family() == types.TupleFamily {
			// When the final statement returns a single tuple column, the column's
			// type becomes the routine's return type.
			rTyp = stmtScope.cols[0].typ
		} else {
			// Get the types from the columns of the last statement.
			tc := make([]*types.T, len(stmtScope.cols))
			tl := make([]string, len(stmtScope.cols))
			for i, col := range stmtScope.cols {
				tc[i] = col.typ
				tl[i] = col.name.MetadataName()
			}
			rTyp = types.MakeLabeledTuple(tc, tl)
		}
	}
	if insideDataSource {
		// If the routine is used as a data source, there must be a column
		// definition list, which must be compatible with the last statement's
		// columns.
		b.validateGeneratorFunctionReturnType(f.ResolvedOverload(), rTyp, inScope)
		if f.ResolvedOverload().ReturnsRecordType {
			// The validation happens for every routine used as a data source, but we
			// only update the type using the column definition list for
			// RECORD-returning routines.
			rTyp = b.getColumnDefinitionListTypes(inScope)
		}
	}
	f.SetTypeAnnotation(rTyp)
}

// combineRoutineColsIntoTuple is a helper to combine individual result columns
// into a single tuple column.
func (b *Builder) combineRoutineColsIntoTuple(
	cols physical.Presentation, stmtScope *scope, inputExpr memo.RelExpr,
) (memo.RelExpr, *physical.Required) {
	elems := make(memo.ScalarListExpr, len(cols))
	typContents := make([]*types.T, len(cols))
	for i := range cols {
		elems[i] = b.factory.ConstructVariable(cols[i].ID)
		typContents[i] = b.factory.Metadata().ColumnMeta(cols[i].ID).Type
	}
	colTyp := types.MakeTuple(typContents)
	tup := b.factory.ConstructTuple(elems, colTyp)
	col := b.synthesizeColumn(stmtScope, scopeColName(""), colTyp, nil /* expr */, tup)
	return b.constructProject(inputExpr, []scopeColumn{*col}), stmtScope.makePhysicalProps()
}

// expandRoutineTupleIntoCols is a helper to expand the elements of a single
// tuple result column into individual result columns.
func (b *Builder) expandRoutineTupleIntoCols(
	tupleColID opt.ColumnID, stmtScope *scope, inputExpr memo.RelExpr,
) (memo.RelExpr, *physical.Required) {
	colTyp := b.factory.Metadata().ColumnMeta(tupleColID).Type
	elems := make([]scopeColumn, len(colTyp.TupleContents()))
	for i := range colTyp.TupleContents() {
		varExpr := b.factory.ConstructVariable(tupleColID)
		e := b.factory.ConstructColumnAccess(varExpr, memo.TupleOrdinal(i))
		col := b.synthesizeColumn(stmtScope, scopeColName(""), colTyp.TupleContents()[i], nil, e)
		elems[i] = *col
	}
	return b.constructProject(inputExpr, elems), stmtScope.makePhysicalProps()
}

// maybeAddRoutineAssignmentCasts checks whether the result columns of the last
// statement in a routine match up with the return type. If not, it attempts to
// assignment-cast the columns to the correct type.
func (b *Builder) maybeAddRoutineAssignmentCasts(
	cols physical.Presentation,
	bodyScope *scope,
	rTyp *types.T,
	expr memo.RelExpr,
	physProps *physical.Required,
	insideDataSource bool,
) (memo.RelExpr, *physical.Required) {
	if rTyp.Family() == types.VoidFamily {
		// Void routines don't return a result, so a cast is not necessary.
		return expr, physProps
	}
	var desiredTypes []*types.T
	if insideDataSource && rTyp.Family() == types.TupleFamily {
		// The result column(s) should match the elements of the composite return
		// type.
		desiredTypes = rTyp.TupleContents()
	} else {
		// There should be a single result column that directly matches the return
		// type.
		desiredTypes = []*types.T{rTyp}
	}
	if len(desiredTypes) != len(cols) {
		panic(errors.AssertionFailedf("expected types and cols to be the same length"))
	}
	needCast := false
	md := b.factory.Metadata()
	for i, col := range cols {
		colTyp, expectedTyp := md.ColumnMeta(col.ID).Type, desiredTypes[i]
		if !colTyp.Identical(expectedTyp) {
			needCast = true
			break
		}
	}
	if !needCast {
		return expr, physProps
	}
	stmtScope := bodyScope.push()
	for i, col := range cols {
		colTyp, expectedTyp := md.ColumnMeta(col.ID).Type, desiredTypes[i]
		scalar := b.factory.ConstructVariable(cols[i].ID)
		if !colTyp.Identical(expectedTyp) {
			if !cast.ValidCast(colTyp, expectedTyp, cast.ContextAssignment) {
				panic(errors.AssertionFailedf("invalid cast should have been caught earlier"))
			}
			scalar = b.factory.ConstructAssignmentCast(scalar, expectedTyp)
		}
		b.synthesizeColumn(stmtScope, scopeColName(""), expectedTyp, nil /* expr */, scalar)
	}
	return b.constructProject(expr, stmtScope.cols), stmtScope.makePhysicalProps()
}

func (b *Builder) withinSQLRoutine(fn func()) {
	defer func(origValue bool) {
		b.insideSQLRoutine = origValue
	}(b.insideSQLRoutine)
	b.insideSQLRoutine = true
	fn()
}
