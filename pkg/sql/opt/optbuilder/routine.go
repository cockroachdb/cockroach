// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
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

	// builtins should have access to unsafe internals
	if o.Type == tree.BuiltinRoutine {
		defer b.DisableUnsafeInternalCheck()()
	}

	// Check for execution privileges for user-defined overloads. Built-in
	// overloads do not need to be checked. Use checkExecutePrivilegeUser rather
	// than checkPrivilegeUser so that views (which override checkPrivilegeUser
	// to the view owner) still check EXECUTE against the invoker.
	if o.Type == tree.UDFRoutine {
		if err := b.catalog.CheckExecutionPrivilege(b.ctx, o.Oid, b.checkExecutePrivilegeUser()); err != nil {
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

	if b.trackSchemaDeps && o.Type != tree.BuiltinRoutine {
		b.schemaFunctionDeps.Add(int(funcdesc.UserDefinedFunctionOIDToID(o.Oid)))
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

	// Check for execution privileges. Use checkExecutePrivilegeUser rather
	// than checkPrivilegeUser so that views (which override checkPrivilegeUser
	// to the view owner) still check EXECUTE against the invoker.
	if err := b.catalog.CheckExecutionPrivilege(b.ctx, o.Oid, b.checkExecutePrivilegeUser()); err != nil {
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

	if b.trackSchemaDeps && o.Type != tree.BuiltinRoutine {
		b.schemaFunctionDeps.Add(int(funcdesc.UserDefinedFunctionOIDToID(o.Oid)))
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
		dataSourcePrivilegeUserOverride,
		executePrivilegeUserOverride username.SQLUsername,
	) {
		b.trackSchemaDeps = trackSchemaDeps
		b.insideUDF = insideUDF
		b.insideDataSource = insideDataSource
		b.insideSQLRoutine = insideSQLRoutine
		b.dataSourcePrivilegeUserOverride = dataSourcePrivilegeUserOverride
		b.executePrivilegeUserOverride = executePrivilegeUserOverride
	}(b.trackSchemaDeps, b.insideUDF, b.insideDataSource, b.insideSQLRoutine, b.dataSourcePrivilegeUserOverride, b.executePrivilegeUserOverride)
	oldInsideDataSource := b.insideDataSource
	b.insideDataSource = false
	b.trackSchemaDeps = false
	b.insideUDF = true
	b.insideSQLRoutine = o.Language == tree.RoutineLangSQL
	isSetReturning := o.Class == tree.GeneratorClass
	var routineOwner username.SQLUsername
	if o.SecurityMode == tree.RoutineDefiner {
		// SECURITY DEFINER: override both privilege users to the routine
		// owner, so all privilege checks inside the routine body use the
		// definer's privileges. Builtins have no descriptor owner; treat
		// NodeUser as the implicit definer so they can read system tables
		// when their SQL body needs to (e.g. the pg_*_size builtins).
		var checkPrivUser username.SQLUsername
		if o.Type == tree.BuiltinRoutine {
			checkPrivUser = username.NodeUserName()
		} else {
			var err error
			checkPrivUser, err = b.catalog.GetRoutineOwner(b.ctx, o.Oid)
			if err != nil {
				panic(err)
			}
		}
		b.dataSourcePrivilegeUserOverride = checkPrivUser
		b.executePrivilegeUserOverride = checkPrivUser
		routineOwner = checkPrivUser
		// Push the owner as the effective user so DDL routed through the
		// declarative schema changer, which runs at build time, sees the
		// definer. Legacy planNode DDLs get the runtime push in
		// routineGenerator.Start.
		defer b.evalCtx.PushEffectiveUser(checkPrivUser)()
	} else if o.Type != tree.BuiltinRoutine {
		// SECURITY INVOKER (default): set dataSourcePrivilegeUserOverride to
		// the invoker. This is necessary because a view may have overridden
		// dataSourcePrivilegeUserOverride to the view owner, but a
		// SECURITY INVOKER function called by the view should run with the
		// invoker's privileges, matching PostgreSQL behavior.
		b.dataSourcePrivilegeUserOverride = b.executePrivilegeUserOverride
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
	var bodyTags []string
	var bodyASTs []tree.Statement
	switch o.Language {
	case tree.RoutineLangSQL:
		// Parse the function body.
		stmts, err := parser.Parse(o.Body)
		if err != nil {
			panic(err)
		}

		// Extract the ASTs for buildSQLRoutineBodyStmts.
		stmtASTs := make([]tree.Statement, len(stmts))
		for i := range stmts {
			stmtASTs[i] = stmts[i].AST
		}
		body, bodyProps, bodyTags = b.buildSQLRoutineBodyStmts(
			stmtASTs, bodyScope, f.ResolvedType(), f, inScope, isSetReturning,
			oldInsideDataSource,
		)

		// Collect the original (pre-VOID-append) ASTs for the UDFDefinition.
		bodyASTs = stmtASTs

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
			SetIsProcedure(isProc).
			SetSecurity(o.SecurityMode, routineOwner)
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
		bodyTags = []string{stmt.AST.Label}
		// The root block is not an explicit statement, so we set the AST to nil.
		bodyASTs = []tree.Statement{nil}
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
				BodyTags:           bodyTags,
				BodyASTs:           bodyASTs,
				Params:             params,
				ResultBufferID:     resultBufferID,
				SecurityMode:       o.SecurityMode,
				RoutineOwner:       routineOwner,
			},
		},
	)
	return routine
}

// buildSQLRoutineBodyStmts builds all body statements of a SQL routine into
// RelExprs at plan time (the eager path), called from buildRoutine.
//
// It appends the synthetic VOID-return statement when needed, then builds each
// statement via buildOneBodyStmt. funcExpr (always non-nil here) and inScope
// are required because the final statement's return type may still need
// finalization at plan time, e.g. resolving AnyTuple for RETURNS RECORD
// routines; see buildOneBodyStmt for how funcExpr selects between finalization
// and validation. rTyp is the return type (funcExpr.ResolvedType()), used for
// the VOID-return check.
//
// The deferred (execution-time) path does not go through this function; it
// builds statements directly via buildOneBodyStmt from
// sqlRoutineBodyBuilder.Build and BuildStmt.
func (b *Builder) buildSQLRoutineBodyStmts(
	stmtASTs []tree.Statement,
	bodyScope *scope,
	rTyp *types.T,
	funcExpr *tree.FuncExpr,
	inScope *scope,
	isSetReturning bool,
	insideDataSource bool,
) (body []memo.RelExpr, bodyProps []*physical.Required, bodyTags []string) {
	stmtASTs, appendedNullForVoidReturn := maybeAppendVoidReturnStmt(stmtASTs, rTyp)
	lastIdx := len(stmtASTs) - 1
	body = make([]memo.RelExpr, len(stmtASTs))
	bodyProps = make([]*physical.Required, len(stmtASTs))
	bodyTags = make([]string, len(stmtASTs))
	for i, ast := range stmtASTs {
		expr, props, tag := b.buildOneBodyStmt(
			ast, bodyScope, rTyp, funcExpr, inScope, isSetReturning, insideDataSource, i, lastIdx,
		)
		body[i] = expr
		bodyProps[i] = props
		// We don't need a statement tag for the artificial appended `SELECT NULL`
		// statement.
		if appendedNullForVoidReturn && i == lastIdx {
			bodyTags[i] = ""
		} else {
			bodyTags[i] = tag
		}
	}
	return body, bodyProps, bodyTags
}

// buildOneBodyStmt builds a single SQL routine body statement at stmtIdx into a
// RelExpr, returning the expression, its required physical properties, and its
// statement tag. It is the shared per-statement helper used by both the eager
// (plan-time) and deferred (execution-time) build paths.
//
// lastIdx is the index of the final body statement. The final statement
// produces the routine's output, so when stmtIdx == lastIdx the statement
// receives return-type handling and finalization (LIMIT 1 for
// non-set-returning routines, tuple wrapping, and assignment casts) via
// finishRoutineReturnStmt.
//
// funcExpr selects between the two build paths:
//
//   - Eager path (funcExpr != nil): called at plan time from
//     buildSQLRoutineBodyStmts. The return type may still need finalization
//     (e.g. resolving AnyTuple for RETURNS RECORD routines), so
//     finalizeRoutineReturnType is called; funcExpr and inScope are required.
//
//   - Deferred path (funcExpr == nil): called at execution time from
//     sqlRoutineBodyBuilder.Build and BuildStmt. The return type is already
//     resolved, so only validateReturnType runs to confirm the rebuilt body
//     columns are still compatible.
func (b *Builder) buildOneBodyStmt(
	ast tree.Statement,
	bodyScope *scope,
	rTyp *types.T,
	funcExpr *tree.FuncExpr,
	inScope *scope,
	isSetReturning bool,
	insideDataSource bool,
	stmtIdx int,
	lastIdx int,
) (expr memo.RelExpr, props *physical.Required, tag string) {
	// TODO(michae2): We should be checking the statement hints cache here to
	// find any external statement hints that could apply to this statement.
	stmtScope := b.buildStmtAtRootWithScope(ast, nil /* desiredTypes */, bodyScope)

	// The last statement produces the output of the routine.
	if stmtIdx == lastIdx {
		if funcExpr != nil {
			// Eager path: finalize the return type. This handles AnyTuple
			// resolution for RETURNS RECORD routines, column-definition-list
			// validation for data-source usage, and type annotation on the
			// FuncExpr. See finalizeRoutineReturnType for details.
			rTyp = b.finalizeRoutineReturnType(
				funcExpr, stmtScope, inScope, insideDataSource,
			)
		} else {
			// Deferred path: the return type is already resolved. Just
			// validate that the rebuilt body columns are compatible.
			if err := validateReturnType(
				b.ctx, b.semaCtx, rTyp, stmtScope.cols,
			); err != nil {
				panic(err)
			}
		}
		stmtScope = b.finishRoutineReturnStmt(stmtScope, isSetReturning, insideDataSource, rTyp)
	}
	return stmtScope.expr, stmtScope.makePhysicalProps(), ast.StatementTag()
}

// maybeAppendVoidReturnStmt appends a synthetic VALUES (NULL) statement to
// stmtASTs when the routine returns VOID, returning the (possibly extended)
// statement list and whether the synthetic statement was appended. The input
// slice is never mutated.
//
// The synthetic statement is required because we cannot simply project NULL
// from the last statement: doing so would prune all columns and the contents
// of the last statement would not be executed.
func maybeAppendVoidReturnStmt(
	stmtASTs []tree.Statement, rTyp *types.T,
) (augmented []tree.Statement, appended bool) {
	if rTyp.Family() != types.VoidFamily {
		return stmtASTs, false
	}
	augmented = append(append([]tree.Statement(nil), stmtASTs...), &tree.Select{
		Select: &tree.ValuesClause{
			Rows: []tree.Exprs{{tree.DNull}},
		},
	})
	return augmented, true
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
	outScope.copyOrdering(stmtScope)
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
	outScope.copyOrdering(stmtScope)
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
	outScope.copyOrdering(stmtScope)
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

	doBlockImpl, ok := do.Code.(*plpgsqltree.DoBlock)
	if !ok {
		panic(errors.AssertionFailedf("expected a plpgsql block"))
	}

	defer func(oldInsideFuncDep bool, oldAnn tree.Annotations) {
		b.insideFuncDef = oldInsideFuncDep
		b.semaCtx.Annotations = oldAnn
		b.evalCtx.Annotations = &b.semaCtx.Annotations
	}(b.insideFuncDef, b.semaCtx.Annotations)
	b.insideFuncDef = true
	b.semaCtx.Annotations = doBlockImpl.Annotations
	b.evalCtx.Annotations = &b.semaCtx.Annotations

	// Build the routine body.
	var bodyStmts []string
	if b.verboseTracing {
		fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
		fmtCtx.FormatNode(do.Code)
		bodyStmts = []string{fmtCtx.CloseAndGetString()}
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
				BodyASTs:    []tree.Statement{nil},
			},
		},
	)
	routine = b.finishBuildScalar(
		nil /* texpr */, routine, nil /* outScope */, nil, /* outCol */
	)
	outScope.expr = b.factory.ConstructCall(routine, &memo.CallPrivate{})
	return outScope
}

// sqlRoutineBodyBuilder implements memo.RoutineBodyBuilder for SQL routines.
// It captures all metadata needed to build the routine body at execution time
// instead of plan time.
//
// Construct instances with newSQLRoutineBodyBuilder, which performs the
// VOID-return statement augmentation once so that NumStmts is stable.
//
// TODO(janexing): when hooking deferred optbuild up with the production code,
// use newSQLRoutineBodyBuilder (rather than the struct literal) to ensure the
// VOID-return statement augmentation is in place.
type sqlRoutineBodyBuilder struct {
	// stmtASTs holds the body statements to build, including the synthetic
	// VALUES (NULL) statement appended for VOID-returning routines. It is set
	// by newSQLRoutineBodyBuilder.
	stmtASTs         []tree.Statement
	paramTypes       []*types.T
	paramNames       []tree.Name
	rTyp             *types.T
	isSetReturning   bool
	insideDataSource bool
	privilegeUser    string
	routineType      tree.RoutineType
	stmtTreeInitFn   func() statementTree
}

var _ memo.RoutineBodyBuilder = &sqlRoutineBodyBuilder{}

// newSQLRoutineBodyBuilder returns a sqlRoutineBodyBuilder for the given
// configuration. The caller supplies the raw (pre-VOID-append) body statements
// in cfg.stmtASTs; the synthetic VALUES (NULL) statement for VOID-returning
// routines is appended here so that NumStmts and per-statement indexing are
// stable for the lifetime of the builder.
func newSQLRoutineBodyBuilder(cfg sqlRoutineBodyBuilder) *sqlRoutineBodyBuilder {
	cfg.stmtASTs, _ = maybeAppendVoidReturnStmt(cfg.stmtASTs, cfg.rTyp)
	return &cfg
}

// NumStmts is part of the memo.RoutineBodyBuilder interface.
func (rb *sqlRoutineBodyBuilder) NumStmts() int {
	return len(rb.stmtASTs)
}

// Build is part of the memo.RoutineBodyBuilder interface. All body statements
// are built into the single provided factory using one Builder, so that the
// resulting RelExprs share a memo and a single set of parameter columns.
func (rb *sqlRoutineBodyBuilder) Build(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	catalog cat.Catalog,
	factoryI interface{},
) (body []memo.RelExpr, bodyProps []*physical.Required, params opt.ColList, retErr error) {
	// Enact panic handling similar to Builder.Build().
	defer errorutil.MaybeCatchPanic(&retErr, nil /* errCallback */)

	b, bodyScope, params := rb.newBodyBuilder(ctx, semaCtx, evalCtx, catalog, factoryI)

	// Builtin routines need access to internal tables.
	if rb.routineType == tree.BuiltinRoutine {
		defer b.DisableUnsafeInternalCheck()()
	}

	lastIdx := len(rb.stmtASTs) - 1
	body = make([]memo.RelExpr, len(rb.stmtASTs))
	bodyProps = make([]*physical.Required, len(rb.stmtASTs))
	for i, ast := range rb.stmtASTs {
		// Pass funcExpr=nil to indicate the deferred path (the return type is
		// already resolved).
		body[i], bodyProps[i], _ = b.buildOneBodyStmt(
			ast, bodyScope, rb.rTyp, nil /* funcExpr */, nil, /* inScope */
			rb.isSetReturning, rb.insideDataSource, i, lastIdx,
		)
	}
	return body, bodyProps, params, nil
}

// BuildStmt is part of the memo.RoutineBodyBuilder interface. Each call uses a
// fresh Builder so that the statement is built against the provided catalog,
// which may reflect DDL applied by earlier statements.
func (rb *sqlRoutineBodyBuilder) BuildStmt(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	catalog cat.Catalog,
	factoryI interface{},
	stmtIdx int,
) (stmt memo.RelExpr, props *physical.Required, params opt.ColList, retErr error) {
	// Enact panic handling similar to Builder.Build().
	defer errorutil.MaybeCatchPanic(&retErr, nil /* errCallback */)

	if stmtIdx < 0 || stmtIdx >= len(rb.stmtASTs) {
		return nil, nil, nil, errors.AssertionFailedf(
			"body statement index %d out of range [0, %d)", stmtIdx, len(rb.stmtASTs),
		)
	}

	b, bodyScope, params := rb.newBodyBuilder(ctx, semaCtx, evalCtx, catalog, factoryI)

	// Builtin routines need access to internal tables.
	if rb.routineType == tree.BuiltinRoutine {
		defer b.DisableUnsafeInternalCheck()()
	}

	// Pass funcExpr=nil to indicate the deferred path (the return type is
	// already resolved).
	stmt, props, _ = b.buildOneBodyStmt(
		rb.stmtASTs[stmtIdx], bodyScope, rb.rTyp, nil /* funcExpr */, nil, /* inScope */
		rb.isSetReturning, rb.insideDataSource, stmtIdx, len(rb.stmtASTs)-1,
	)
	return stmt, props, params, nil
}

// newBodyBuilder creates and configures a Builder for building this routine's
// body statements at execution time, returning the builder, a body scope
// populated with the routine's parameter columns, and the parameter column
// list.
//
// Callers building a builtin routine must additionally disable the
// unsafe-internals check on the returned builder for the duration of the build;
// see Build and BuildStmt.
func (rb *sqlRoutineBodyBuilder) newBodyBuilder(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	catalog cat.Catalog,
	factoryI interface{},
) (b *Builder, bodyScope *scope, params opt.ColList) {
	factory := factoryI.(*norm.Factory)
	b = New(ctx, semaCtx, evalCtx, catalog, factory, nil /* stmt */)

	// Initialize the statement tree from the captured init function.
	if rb.stmtTreeInitFn != nil {
		b.stmtTree = rb.stmtTreeInitFn()
	}

	// Configure the builder for routine body building.
	b.insideUDF = true
	b.insideSQLRoutine = true
	b.trackSchemaDeps = false

	// Restore the effective privilege user for SECURITY DEFINER contexts.
	if rb.privilegeUser != "" {
		privUser := username.MakeSQLUsernameFromPreNormalizedString(rb.privilegeUser)
		b.dataSourcePrivilegeUserOverride = privUser
		b.executePrivilegeUserOverride = privUser
	}

	// The stable parameter-column-ID guarantee documented on
	// RoutineBodyBuilder.BuildStmt holds only because the parameter columns are
	// the first columns synthesized into the factory. Guard against a future
	// change allocating a column earlier, which would silently shift the IDs.
	if n := b.factory.Metadata().NumColumns(); n != 0 {
		panic(errors.AssertionFailedf(
			"expected no columns synthesized before routine parameters, found %d", n,
		))
	}

	// Create the body scope with parameter columns.
	bodyScope = b.allocScope()
	params = make(opt.ColList, len(rb.paramTypes))
	for i := range rb.paramTypes {
		var name tree.Name
		if i < len(rb.paramNames) {
			name = rb.paramNames[i]
		}
		argColName := funcParamColName(name, i)
		col := b.synthesizeColumn(
			bodyScope, argColName, rb.paramTypes[i], nil /* expr */, nil, /* scalar */
		)
		col.setParamOrd(i)
		params[i] = col.id
	}
	return b, bodyScope, params
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
