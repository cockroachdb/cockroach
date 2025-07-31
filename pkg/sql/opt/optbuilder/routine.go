// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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
		if err := b.catalog.CheckExecutionPrivilege(b.ctx, o.Oid, b.checkPrivilegeUser); err != nil {
			panic(err)
		}
	}

	// Trigger functions cannot be directly invoked.
	if f.ResolvedType().Identical(types.Trigger) {
		// Note: Postgres also uses the "0A000" error code.
		panic(pgerror.New(pgcode.FeatureNotSupported,
			"trigger functions can only be called as triggers",
		))
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

	return b.finishBuildScalar(f, routine, outScope, outCol)
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
	routine = b.finishBuildScalar(nil /* texpr */, routine,
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
	typedExpr := inScope.resolveTypeAndReject(proc, types.AnyElement,
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
	if err := b.catalog.CheckExecutionPrivilege(b.ctx, o.Oid, b.checkPrivilegeUser); err != nil {
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
	b.factory.Metadata().AddUserDefinedRoutine(o, invocationTypes, f.Func.ReferenceByName)

	// Validate that the return types match the original return types defined in
	// the function. Return types like user defined return types may change
	// since the function was first created.
	if f.ResolvedType().UserDefined() {
		funcReturnType, err := tree.ResolveType(b.ctx,
			&tree.OIDTypeReference{OID: f.ResolvedType().Oid()}, b.semaCtx.TypeResolver)
		if err != nil {
			panic(err)
		}
		if !funcReturnType.Identical(f.ResolvedType()) {
			panic(pgerror.Newf(
				pgcode.InvalidFunctionDefinition,
				"return type mismatch in function declared to return %s", f.ResolvedType().Name(),
			))
		}
	}

	// Build the argument expressions.
	var args memo.ScalarListExpr
	var argTypes []*types.T
	if len(f.Exprs) > 0 {
		args = make(memo.ScalarListExpr, 0, len(f.Exprs))
		argTypes = make([]*types.T, 0, len(f.Exprs))
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
			argTypes = append(argTypes, pexpr.(tree.TypedExpr).ResolvedType())
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
	var polyArgTyp *types.T
	if o.Types.Length() > 0 {
		// If necessary, add DEFAULT arguments.
		args, argTypes = b.addDefaultArgs(f, args, argTypes, bodyScope, colRefs)

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

		// Check the parameters for polymorphic types, and resolve to a concrete
		// type if any exist.
		if b.evalCtx.SessionData().OptimizerUsePolymorphicParameterFix {
			var numPolyParams int
			_, numPolyParams, polyArgTyp = tree.ResolvePolymorphicArgTypes(
				paramTypes, argTypes, nil /* anyElemTyp */, true, /* enforceConsistency */
			)
			if numPolyParams > 0 {
				if polyArgTyp == nil {
					// All supplied arguments were NULL, so a type could not be resolved
					// for the polymorphic parameters.
					panic(pgerror.New(pgcode.DatatypeMismatch,
						"could not determine polymorphic type because input has type unknown",
					))
				}
				// If the routine returns a polymorphic type, use the resolved
				// polymorphic argument type to determine the concrete return type.
				b.maybeResolvePolymorphicReturnType(f, polyArgTyp)
			}
		}

		// Add any needed casts from argument type to parameter type, and add a
		// correctly typed column to the bodyScope for each parameter.
		params = make(opt.ColList, len(paramTypes))
		for i := range paramTypes {
			argTyp := argTypes[i]
			desiredTyp := maybeReplacePolymorphicType(paramTypes[i].Typ, polyArgTyp)
			if desiredTyp.Identical(types.AnyTuple) {
				// This is a RECORD-typed parameter. Use the actual argument type.
				desiredTyp = argTyp
			}
			if !argTyp.Identical(desiredTyp) {
				if !cast.ValidCast(argTyp, desiredTyp, cast.ContextAssignment) {
					// Missing assignment cast between these two types should've been
					// caught earlier, during routine creation or overload resolution.
					panic(errors.AssertionFailedf(
						"argument expression has type %s, need type %s, assignment cast isn't possible",
						argTyp.SQLStringForError(), desiredTyp.SQLStringForError(),
					))
				}
				args[i] = b.factory.ConstructCast(args[i], desiredTyp)
			}
			argColName := funcParamColName(tree.Name(paramTypes[i].Name), i)
			col := b.synthesizeColumn(bodyScope, argColName, desiredTyp, nil /* expr */, nil /* scalar */)
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
	defer func(
		trackSchemaDeps,
		insideUDF,
		insideDataSource,
		insideSQLRoutine bool,
		checkPrivilegeUser username.SQLUsername,
	) {
		b.trackSchemaDeps = trackSchemaDeps
		b.insideUDF = insideUDF
		b.insideDataSource = insideDataSource
		b.insideSQLRoutine = insideSQLRoutine
		b.checkPrivilegeUser = checkPrivilegeUser
	}(b.trackSchemaDeps, b.insideUDF, b.insideDataSource, b.insideSQLRoutine, b.checkPrivilegeUser)
	oldInsideDataSource := b.insideDataSource
	b.insideDataSource = false
	b.trackSchemaDeps = false
	b.insideUDF = true
	b.insideSQLRoutine = o.Language == tree.RoutineLangSQL
	isSetReturning := o.Class == tree.GeneratorClass
	// If this is a user-defined routine that has a security mode of DEFINER, we
	// need to override our checkPrivilegeUser to be the owner of the routine.
	if o.Type != tree.BuiltinRoutine && o.SecurityMode == tree.RoutineDefiner {
		checkPrivUser, err := b.catalog.GetRoutineOwner(b.ctx, o.Oid)
		if err != nil {
			panic(err)
		}
		b.checkPrivilegeUser = checkPrivUser
	}

	// Special handling for set-returning PL/pgSQL functions.
	//
	// resultBufferID is used by set-returning PL/pgSQL functions to allow
	// sub-routines to add to the result set at arbitrary points during execution.
	var resultBufferID memo.RoutineResultBufferID
	if isSetReturning && o.Language == tree.RoutineLangPLpgSQL {
		// Allocate the result buffer ID so that sub-routines can add to the
		// result set.
		resultBufferID = b.factory.Memo().NextRoutineResultBufferID()
		if o.ReturnsRecordType {
			// A PL/pgSQL function that returns SETOF RECORD must be used as a data
			// source and gets its concrete return type from the column definition
			// list.
			if !oldInsideDataSource {
				// NOTE: This is the same error as returned by Postgres.
				panic(
					errors.WithHint(
						pgerror.New(pgcode.FeatureNotSupported,
							"materialize mode required, but it is not allowed in this context",
						),
						"PL/pgSQL functions that return SETOF RECORD are only allowed in data source "+
							"context with a column definition list, "+
							"e.g. SELECT * FROM my_func() AS (a INT, b STRING)",
					),
				)
			}
			rTyp := b.getColumnDefinitionListTypes(inScope)
			if rTyp == nil {
				panic(needColumnDefListForRecordErr)
			}
			f.SetTypeAnnotation(rTyp)
		}
		b.validateGeneratorFunctionReturnType(f.ResolvedOverload(), f.ResolvedType(), inScope)
	}

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
		if f.ResolvedType().Family() == types.VoidFamily {
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

			// The last statement produces the output of the UDF.
			if i == len(stmts)-1 {
				rTyp := b.finalizeRoutineReturnType(f, stmtScope, inScope, oldInsideDataSource)
				stmtScope = b.finishRoutineReturnStmt(stmtScope, isSetReturning, oldInsideDataSource, rTyp)
			}
			body[i] = stmtScope.expr
			bodyProps[i] = stmtScope.makePhysicalProps()
		}

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
				typ:   maybeReplacePolymorphicType(typ, polyArgTyp),
				class: param.Class,
			})
		}
		options := basePLOptions().
			SetIsSetReturning(isSetReturning).
			SetInsideDataSource(oldInsideDataSource).
			SetIsProcedure(isProc)
		plBuilder := newPLpgSQLBuilder(
			b, options, def.Name, stmt.AST.Label, colRefs,
			routineParams, f.ResolvedType(), outScope, resultBufferID,
		)
		stmtScope := plBuilder.buildRootBlock(stmt.AST, bodyScope, routineParams)
		if !isSetReturning {
			// Set-returning functions add to the result set during execution rather
			// than directly returning the result of the last statement. The PL/pgSQL
			// statements used to add to the result set handle their own validation.
			rTyp := b.finalizeRoutineReturnType(f, stmtScope, inScope, oldInsideDataSource)
			stmtScope = b.finishRoutineReturnStmt(stmtScope, isSetReturning, oldInsideDataSource, rTyp)
		}
		body = []memo.RelExpr{stmtScope.expr}
		bodyProps = []*physical.Required{stmtScope.makePhysicalProps()}
		if b.verboseTracing {
			bodyStmts = []string{stmt.String()}
		}
	default:
		panic(errors.AssertionFailedf("unexpected language: %v", o.Language))
	}

	multiColDataSource := len(f.ResolvedType().TupleContents()) > 0 && oldInsideDataSource
	routine := b.factory.ConstructUDFCall(
		args,
		&memo.UDFCallPrivate{
			Def: &memo.UDFDefinition{
				Name:               def.Name,
				Typ:                f.ResolvedType(),
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
				ResultBufferID:     resultBufferID,
			},
		},
	)
	return routine
}

// finishRoutineReturnStmt manages the output columns for a statement that will
// be added to the result set of a routine. Depending on the context and return
// type of the routine, this may mean expanding a tuple into multiple columns,
// or combining multiple columns into a tuple.
func (b *Builder) finishRoutineReturnStmt(
	stmtScope *scope, isSetReturning, insideDataSource bool, rTyp *types.T,
) *scope {
	// NOTE: the result columns of the last statement may not reflect the return
	// type until after the call to maybeAddRoutineAssignmentCasts. Therefore, the
	// logic below must take care in distinguishing the resolved return type from
	// the result column type(s).
	//
	// Add a LIMIT 1 to the last statement if the UDF is not
	// set-returning. This is valid because any other rows after the
	// first can simply be ignored. The limit could be beneficial
	// because it could allow additional optimization.
	if !isSetReturning {
		b.buildLimit(&tree.Limit{Count: tree.NewDInt(1)}, b.allocScope(), stmtScope)
	}

	// Depending on the context in which the UDF was called, it may be necessary
	// to either combine multiple result columns into a tuple, or to expand a
	// tuple result column into multiple columns.
	isSingleTupleResult := len(stmtScope.cols) == 1 &&
		stmtScope.cols[0].typ.Family() == types.TupleFamily
	if insideDataSource {
		// The UDF is a data source. If it returns a composite type and the last
		// statement returns a single tuple column, the elements of the column
		// should be expanded into individual columns.
		if rTyp.Family() == types.TupleFamily && isSingleTupleResult {
			stmtScope = b.expandRoutineTupleIntoCols(stmtScope)
		}
	} else {
		// Only a single column can be returned from a routine, unless it is a UDF
		// used as a data source (see comment above). There are three cases in which
		// we must wrap the column(s) from the last statement into a single tuple:
		//   1. The last statement has multiple result columns.
		//   2. The routine returns RECORD, and the (single) result column cannot
		//      be coerced to the return type. Note that a procedure with OUT-params
		//      always wraps the OUT-param types in a record.
		if len(stmtScope.cols) > 1 ||
			(rTyp.Family() == types.TupleFamily && !stmtScope.cols[0].typ.Equivalent(rTyp) &&
				!cast.ValidCast(stmtScope.cols[0].typ, rTyp, cast.ContextAssignment)) {
			stmtScope = b.combineRoutineColsIntoTuple(stmtScope)
		}
	}

	// If necessary, we add an assignment cast to the result column(s) so that its
	// type matches the function return type.
	return b.maybeAddRoutineAssignmentCasts(stmtScope, rTyp, insideDataSource)
}

// finalizeRoutineReturnType updates the routine's return type, taking into
// account the result columns of the last statement, as well as the column
// definition list if one was specified. It returns the final resolved type of
// the function.
func (b *Builder) finalizeRoutineReturnType(
	f *tree.FuncExpr, stmtScope, inScope *scope, insideDataSource bool,
) *types.T {
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
	if !f.ResolvedOverload().ReturnsRecordType {
		if err := validateReturnType(b.ctx, b.semaCtx, rTyp, stmtScope.cols); err != nil {
			panic(err)
		}
	}
	f.SetTypeAnnotation(rTyp)
	return rTyp
}

// combineRoutineColsIntoTuple is a helper to combine individual result columns
// into a single tuple column.
func (b *Builder) combineRoutineColsIntoTuple(stmtScope *scope) *scope {
	outScope := stmtScope.push()
	elems := make(memo.ScalarListExpr, len(stmtScope.cols))
	typContents := make([]*types.T, len(stmtScope.cols))
	for i := range stmtScope.cols {
		elems[i] = b.factory.ConstructVariable(stmtScope.cols[i].id)
		typContents[i] = stmtScope.cols[i].typ
	}
	colTyp := types.MakeTuple(typContents)
	tup := b.factory.ConstructTuple(elems, colTyp)
	b.synthesizeColumn(outScope, scopeColName(""), colTyp, nil /* expr */, tup)
	b.constructProjectForScope(stmtScope, outScope)
	return outScope
}

// expandRoutineTupleIntoCols is a helper to expand the elements of a single
// tuple result column into individual result columns.
func (b *Builder) expandRoutineTupleIntoCols(stmtScope *scope) *scope {
	// Assume that the input scope has a single tuple column.
	if buildutil.CrdbTestBuild {
		if len(stmtScope.cols) != 1 {
			panic(errors.AssertionFailedf("expected a single tuple column"))
		}
	}
	tupleColID := stmtScope.cols[0].id
	outScope := stmtScope.push()
	colTyp := b.factory.Metadata().ColumnMeta(tupleColID).Type
	for i := range colTyp.TupleContents() {
		varExpr := b.factory.ConstructVariable(tupleColID)
		e := b.factory.ConstructColumnAccess(varExpr, memo.TupleOrdinal(i))
		b.synthesizeColumn(outScope, scopeColName(""), colTyp.TupleContents()[i], nil, e)
	}
	b.constructProjectForScope(stmtScope, outScope)
	return outScope
}

// maybeAddRoutineAssignmentCasts checks whether the result columns of the last
// statement in a routine match up with the return type. If not, it attempts to
// assignment-cast the columns to the correct type.
func (b *Builder) maybeAddRoutineAssignmentCasts(
	stmtScope *scope, rTyp *types.T, insideDataSource bool,
) *scope {
	if rTyp.Family() == types.VoidFamily {
		// Void routines don't return a result, so a cast is not necessary.
		return stmtScope
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
	if len(desiredTypes) != len(stmtScope.cols) {
		panic(errors.AssertionFailedf("expected types and cols to be the same length"))
	}
	needCast := false
	for i, col := range stmtScope.cols {
		if !col.typ.Identical(desiredTypes[i]) {
			needCast = true
			break
		}
	}
	if !needCast {
		return stmtScope
	}
	outScope := stmtScope.push()
	for i, col := range stmtScope.cols {
		scalar := b.factory.ConstructVariable(col.id)
		if !col.typ.Identical(desiredTypes[i]) {
			if !cast.ValidCast(col.typ, desiredTypes[i], cast.ContextAssignment) {
				panic(errors.AssertionFailedf(
					"invalid cast from %s to %s should have been caught earlier",
					col.typ.SQLStringForError(), desiredTypes[i].SQLStringForError(),
				))
			}
			scalar = b.factory.ConstructAssignmentCast(scalar, desiredTypes[i])
		}
		b.synthesizeColumn(outScope, scopeColName(""), desiredTypes[i], nil /* expr */, scalar)
	}
	b.constructProjectForScope(stmtScope, outScope)
	return outScope
}

// addDefaultArgs adds DEFAULT arguments to the list of user-supplied arguments
// if the user-supplied arguments are fewer than the number of parameters.
func (b *Builder) addDefaultArgs(
	f *tree.FuncExpr,
	args memo.ScalarListExpr,
	argTypes []*types.T,
	inScope *scope,
	colRefs *opt.ColSet,
) (memo.ScalarListExpr, []*types.T) {
	o := f.ResolvedOverload()

	// Check whether some arguments were omitted. We need to use the
	// corresponding DEFAULT expressions if so.
	numDefaultsToUse := o.Types.Length() - len(args)
	if numDefaultsToUse <= 0 {
		return args, argTypes
	}
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
		args = append(args, arg)
		argTypes = append(argTypes, texpr.ResolvedType())
	}
	return args, argTypes
}

// maybeResolvePolymorphicReturnType checks whether the return type of the
// routine is polymorphic and if so, uses the resolved polymorphic argument type
// to determine the concrete return type.
func (b *Builder) maybeResolvePolymorphicReturnType(f *tree.FuncExpr, polyArgTyp *types.T) {
	originalRTyp := f.ResolvedType()
	if originalRTyp.IsPolymorphicType() {
		f.SetTypeAnnotation(maybeReplacePolymorphicType(originalRTyp, polyArgTyp))
	} else if originalRTyp.Family() == types.TupleFamily && !f.ResolvedOverload().ReturnsRecordType {
		var hasPolymorphicOutParam bool
		for _, typ := range originalRTyp.TupleContents() {
			if typ.IsPolymorphicType() {
				hasPolymorphicOutParam = true
				break
			}
		}
		if hasPolymorphicOutParam {
			outParamTypes := make([]*types.T, len(originalRTyp.TupleContents()))
			for i, outParamTyp := range originalRTyp.TupleContents() {
				outParamTypes[i] = maybeReplacePolymorphicType(outParamTyp, polyArgTyp)
			}
			f.SetTypeAnnotation(types.MakeLabeledTuple(outParamTypes, originalRTyp.TupleLabels()))
		}
	}
}

// maybeReplacePolymorphicType checks whether the given type is polymorphic and
// if so, replaces it with the given polymorphic argument type. It returns the
// original type if it is not polymorphic.
func maybeReplacePolymorphicType(originalTyp, polyArgTyp *types.T) *types.T {
	if !originalTyp.IsPolymorphicType() || polyArgTyp == nil {
		return originalTyp
	}
	switch originalTyp.Family() {
	case types.ArrayFamily:
		if polyArgTyp.Family() == types.ArrayFamily {
			panic(pgerror.Newf(pgcode.UndefinedObject,
				"could not find array type for data type %s", polyArgTyp.Name(),
			))
		}
		return types.MakeArray(polyArgTyp)
	default:
		return polyArgTyp
	}
}

func (b *Builder) withinNestedPLpgSQLCall(fn func()) {
	defer func(origValue bool) {
		b.insideNestedPLpgSQLCall = origValue
	}(b.insideNestedPLpgSQLCall)
	b.insideNestedPLpgSQLCall = true
	fn()
}

const doBlockRoutineName = "inline_code_block"

// buildDo builds a SQL DO statement into an anonymous routine that is called
// from the current scope.
func (b *Builder) buildDo(do *tree.DoBlock, inScope *scope) *scope {
	// Disable memo reuse. Note that this is not strictly necessary because
	// optPlanningCtx does not attempt to reuse tree.DoBlock statements, but
	// exists for explicitness.
	//
	// TODO(drewk): Enable memo reuse with DO statements.
	b.DisableMemoReuse = true

	defer func(oldInsideFuncDep bool) { b.insideFuncDef = oldInsideFuncDep }(b.insideFuncDef)
	b.insideFuncDef = true

	// Build the routine body.
	var bodyStmts []string
	if b.verboseTracing {
		fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
		fmtCtx.FormatNode(do.Code)
		bodyStmts = []string{fmtCtx.CloseAndGetString()}
	}
	doBlockImpl, ok := do.Code.(*plpgsqltree.DoBlock)
	if !ok {
		panic(errors.AssertionFailedf("expected a plpgsql block"))
	}
	bodyScope := b.buildPLpgSQLDoBody(doBlockImpl)

	// Build a CALL expression that invokes the routine.
	outScope := inScope.push()
	routine := b.factory.ConstructUDFCall(
		memo.ScalarListExpr{},
		&memo.UDFCallPrivate{
			Def: &memo.UDFDefinition{
				Name:        doBlockRoutineName,
				Typ:         types.Void,
				Volatility:  volatility.Volatile,
				RoutineType: tree.ProcedureRoutine,
				RoutineLang: tree.RoutineLangPLpgSQL,
				Body:        []memo.RelExpr{bodyScope.expr},
				BodyProps:   []*physical.Required{bodyScope.makePhysicalProps()},
				BodyStmts:   bodyStmts,
			},
		},
	)
	routine = b.finishBuildScalar(
		nil /* texpr */, routine, nil /* outScope */, nil, /* outCol */
	)
	outScope.expr = b.factory.ConstructCall(routine, &memo.CallPrivate{})
	return outScope
}

// buildDoBody builds the body of the anonymous routine for a DO statement.
func (b *Builder) buildPLpgSQLDoBody(do *plpgsqltree.DoBlock) *scope {
	// Build an expression for each statement in the function body.
	options := basePLOptions().WithIsProcedure().WithIsDoBlock()
	plBuilder := newPLpgSQLBuilder(
		b, options, doBlockRoutineName, do.Block.Label, nil, /* colRefs */
		nil /* routineParams */, types.Void, nil /* outScope */, 0, /* resultBufferID */
	)
	// Allocate a fresh scope, since DO blocks do not take parameters or reference
	// variables or columns from the calling context.
	bodyScope := b.allocScope()
	stmtScope := plBuilder.buildRootBlock(do.Block, bodyScope, nil /* routineParams */)
	return b.finishRoutineReturnStmt(
		stmtScope, false /* isSetReturning */, false /* insideDataSource */, types.Void,
	)
}
