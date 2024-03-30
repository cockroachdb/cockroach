// Copyright 2022 The Cockroach Authors.
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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql"
	plpgsqlparser "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

func (b *Builder) buildCreateFunction(cf *tree.CreateRoutine, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	if cf.Name.ExplicitCatalog {
		if string(cf.Name.CatalogName) != b.evalCtx.SessionData().Database {
			panic(unimplemented.New("CREATE FUNCTION", "cross-db references not supported"))
		}
	}

	activeVersion := b.evalCtx.Settings.Version.ActiveVersion(b.ctx)
	if cf.IsProcedure && !activeVersion.IsActive(clusterversion.V23_2) {
		panic(unimplemented.New("procedures", "procedures are not yet supported"))
	}

	sch, resName := b.resolveSchemaForCreateFunction(&cf.Name)
	schID := b.factory.Metadata().AddSchema(sch)
	cf.Name.ObjectNamePrefix = resName

	b.insideFuncDef = true
	b.trackSchemaDeps = true
	// Make sure datasource names are qualified.
	b.qualifyDataSourceNamesInAST = true
	oldEvalCtxAnn := b.evalCtx.Annotations
	oldSemaCtxAnn := b.semaCtx.Annotations
	defer func() {
		b.insideFuncDef = false
		b.trackSchemaDeps = false
		b.schemaDeps = nil
		b.schemaTypeDeps = intsets.Fast{}
		b.qualifyDataSourceNamesInAST = false
		b.evalCtx.Annotations = oldEvalCtxAnn
		b.semaCtx.Annotations = oldSemaCtxAnn

		switch recErr := recover().(type) {
		case nil:
			// No error.
		case error:
			if errors.Is(recErr, tree.ErrRoutineUndefined) {
				panic(
					errors.WithHint(
						recErr,
						"There is probably a typo in function name. Or the intention was to use a user-defined "+
							"function in the function body, which is currently not supported.",
					),
				)
			}
			panic(recErr)
		default:
			panic(recErr)
		}
	}()

	if cf.RoutineBody != nil {
		panic(unimplemented.New("CREATE FUNCTION sql_body", "CREATE FUNCTION...sql_body unimplemented"))
	}

	if err := tree.ValidateRoutineOptions(cf.Options, cf.IsProcedure); err != nil {
		panic(err)
	}

	// Look for function body string from function options.
	// Note that function body can be an empty string.
	funcBodyFound := false
	languageFound := false
	var funcBodyStr string
	var language tree.RoutineLanguage
	for _, option := range cf.Options {
		switch opt := option.(type) {
		case tree.RoutineBodyStr:
			funcBodyFound = true
			funcBodyStr = string(opt)
		case tree.RoutineLanguage:
			languageFound = true
			language = opt
			// Check the language here, before attempting to parse the function body.
			if _, err := funcinfo.FunctionLangToProto(opt); err != nil {
				panic(err)
			}
		}
	}

	if !funcBodyFound {
		panic(pgerror.New(pgcode.InvalidFunctionDefinition, "no function body specified"))
	}
	if !languageFound {
		panic(pgerror.New(pgcode.InvalidFunctionDefinition, "no language specified"))
	}
	if language == tree.RoutineLangPLpgSQL {
		if !activeVersion.IsActive(clusterversion.V23_2) {
			panic(unimplemented.New("PLpgSQL", "PLpgSQL is not supported until version 23.2"))
		}
		if err := plpgsql.CheckClusterSupportsPLpgSQL(b.evalCtx.Settings); err != nil {
			panic(err)
		}
	}

	// Track the dependencies in the arguments, return type, and statements in
	// the function body.
	var deps opt.SchemaDeps
	var typeDeps opt.SchemaTypeDeps
	var functionDeps opt.SchemaFunctionDeps

	afterBuildStmt := func() {
		functionDeps.UnionWith(b.schemaFunctionDeps)
		deps = append(deps, b.schemaDeps...)
		typeDeps.UnionWith(b.schemaTypeDeps)
		// Reset the tracked dependencies for next statement.
		b.schemaDeps = nil
		b.schemaTypeDeps = intsets.Fast{}
		b.schemaFunctionDeps = intsets.Fast{}

		// Reset the annotations to the original values
		b.evalCtx.Annotations = oldEvalCtxAnn
		b.semaCtx.Annotations = oldSemaCtxAnn
	}

	if language == tree.RoutineLangPLpgSQL {
		paramNameSeen := make(map[tree.Name]struct{})
		for _, param := range cf.Params {
			if param.Name != "" {
				checkDuplicateParamName(param, paramNameSeen)
			}
		}
	} else {
		// For SQL routines, input and output parameters form separate
		// "namespaces".
		paramNameSeenIn, paramNameSeenOut := make(map[tree.Name]struct{}), make(map[tree.Name]struct{})
		for _, param := range cf.Params {
			if param.Name != "" {
				if param.IsInParam() {
					checkDuplicateParamName(param, paramNameSeenIn)
				}
				if param.IsOutParam() {
					checkDuplicateParamName(param, paramNameSeenOut)
				}
			}
		}
	}

	// bodyScope is the base scope for each statement in the body. We add the
	// named parameters to the scope so that references to them in the body can
	// be resolved.
	bodyScope := b.allocScope()
	// routineParams are all parameters of PLpgSQL routines.
	var routineParams []routineParam
	var outParamTypes []*types.T
	// When multiple OUT parameters are present, parameter names become the
	// labels in the output RECORD type.
	var outParamNames []string
	for i := range cf.Params {
		param := &cf.Params[i]
		typ, err := tree.ResolveType(b.ctx, param.Type, b.semaCtx.TypeResolver)
		if err != nil {
			panic(err)
		}
		if param.IsOutParam() {
			outParamTypes = append(outParamTypes, typ)
			paramName := string(param.Name)
			if paramName == "" {
				paramName = fmt.Sprintf("column%d", len(outParamTypes))
			}
			outParamNames = append(outParamNames, paramName)
		}
		// The parameter type must be supported by the current cluster version.
		checkUnsupportedType(b.ctx, b.semaCtx, typ)
		if types.IsRecordType(typ) {
			if language == tree.RoutineLangSQL {
				panic(pgerror.Newf(pgcode.InvalidFunctionDefinition,
					"SQL functions cannot have arguments of type record"))
			} else if language == tree.RoutineLangPLpgSQL {
				panic(unimplemented.NewWithIssueDetail(105713,
					"PL/pgSQL functions with RECORD input arguments",
					"PL/pgSQL functions with RECORD input arguments are not yet supported",
				))
			}
		}

		// Add all input parameters to the base scope of the body.
		if tree.IsInParamClass(param.Class) {
			paramColName := funcParamColName(param.Name, i)
			col := b.synthesizeColumn(bodyScope, paramColName, typ, nil /* expr */, nil /* scalar */)
			col.setParamOrd(i)
		}

		// Collect the user defined type dependencies.
		typedesc.GetTypeDescriptorClosure(typ).ForEach(func(id descpb.ID) {
			typeDeps.Add(int(id))
		})

		// Collect the parameters for PLpgSQL routines.
		if language == tree.RoutineLangPLpgSQL {
			routineParams = append(routineParams, routineParam{
				name:  param.Name,
				typ:   typ,
				class: param.Class,
			})
		}
	}

	// Determine OUT parameter based return type.
	var outParamType *types.T
	if (cf.IsProcedure && len(outParamTypes) > 0) || len(outParamTypes) > 1 {
		outParamType = types.MakeLabeledTuple(outParamTypes, outParamNames)
	} else if len(outParamTypes) == 1 {
		outParamType = outParamTypes[0]
	}

	var funcReturnType *types.T
	var err error
	if cf.ReturnType != nil {
		funcReturnType, err = tree.ResolveType(b.ctx, cf.ReturnType.Type, b.semaCtx.TypeResolver)
		if err != nil {
			panic(err)
		}
	}
	if outParamType != nil {
		if funcReturnType != nil && !funcReturnType.Equivalent(outParamType) {
			panic(pgerror.Newf(pgcode.InvalidFunctionDefinition, "function result type must be %s because of OUT parameters", outParamType.Name()))
		}
		// Override the return types so that we do return type validation and SHOW
		// CREATE correctly.
		funcReturnType = outParamType
		cf.ReturnType = &tree.RoutineReturnType{
			Type: outParamType,
		}
	} else if funcReturnType == nil {
		if cf.IsProcedure {
			// A procedure doesn't need a return type. Use a VOID return type to avoid
			// errors in shared logic later.
			funcReturnType = types.Void
			cf.ReturnType = &tree.RoutineReturnType{
				Type: types.Void,
			}
		} else {
			panic(pgerror.New(pgcode.InvalidFunctionDefinition, "function result type must be specified"))
		}
	}
	// We disallow creating functions that return UNKNOWN, for consistency with postgres.
	if funcReturnType.Family() == types.UnknownFamily {
		if language == tree.RoutineLangSQL {
			panic(pgerror.New(pgcode.InvalidFunctionDefinition, "SQL functions cannot return type unknown"))
		} else if language == tree.RoutineLangPLpgSQL {
			panic(pgerror.New(pgcode.InvalidFunctionDefinition, "PL/pgSQL functions cannot return type unknown"))
		}
	}
	// Collect the user defined type dependency of the return type.
	typedesc.GetTypeDescriptorClosure(funcReturnType).ForEach(func(id descpb.ID) {
		typeDeps.Add(int(id))
	})

	targetVolatility := tree.GetRoutineVolatility(cf.Options)
	fmtCtx := tree.NewFmtCtx(tree.FmtSerializable)

	// Validate each statement and collect the dependencies.
	var stmtScope *scope
	switch language {
	case tree.RoutineLangSQL:
		// Parse the function body.
		stmts, err := parser.Parse(funcBodyStr)
		if err != nil {
			panic(err)
		}
		b.withinSQLRoutine(func() {
			for i, stmt := range stmts {
				// Add statement ast into CreateRoutine node for logging purpose, and set
				// the annotations for this statement so names can be resolved.
				cf.BodyStatements = append(cf.BodyStatements, stmt.AST)
				ann := tree.MakeAnnotations(stmt.NumAnnotations)
				cf.BodyAnnotations = append(cf.BodyAnnotations, &ann)

				// The defer logic will reset the annotations to the old value.
				b.semaCtx.Annotations = ann
				b.evalCtx.Annotations = &ann

				// We need to disable stable function folding because we want to catch the
				// volatility of stable functions. If folded, we only get a scalar and
				// lose the volatility.
				b.factory.FoldingControl().TemporarilyDisallowStableFolds(func() {
					stmtScope = b.buildStmtAtRootWithScope(stmts[i].AST, nil /* desiredTypes */, bodyScope)
				})
				checkStmtVolatility(targetVolatility, stmtScope, stmt.AST)

				// Format the statements with qualified datasource names.
				formatFuncBodyStmt(fmtCtx, stmt.AST, language, i > 0 /* newLine */)
				afterBuildStmt()
			}
		})
	case tree.RoutineLangPLpgSQL:
		if cf.ReturnType != nil && cf.ReturnType.SetOf {
			panic(unimplemented.NewWithIssueDetail(105240,
				"set-returning PL/pgSQL functions",
				"set-returning PL/pgSQL functions are not yet supported",
			))
		}

		// Parse the function body.
		stmt, err := plpgsqlparser.Parse(funcBodyStr)
		if err != nil {
			panic(err)
		}

		// We need to disable stable function folding because we want to catch the
		// volatility of stable functions. If folded, we only get a scalar and lose
		// the volatility.
		b.factory.FoldingControl().TemporarilyDisallowStableFolds(func() {
			plBuilder := newPLpgSQLBuilder(
				b, cf.Name.Object(), stmt.AST.Label, nil, /* colRefs */
				routineParams, funcReturnType, cf.IsProcedure, nil, /* outScope */
			)
			stmtScope = plBuilder.buildRootBlock(stmt.AST, bodyScope, routineParams)
		})
		checkStmtVolatility(targetVolatility, stmtScope, stmt)

		// Format the statements with qualified datasource names.
		formatFuncBodyStmt(fmtCtx, stmt.AST, language, false /* newLine */)
		afterBuildStmt()
	default:
		panic(errors.AssertionFailedf("unexpected language: %v", language))
	}

	if stmtScope != nil {
		// Validate that the result type of the last statement matches the
		// return type of the function.
		// TODO(mgartner): stmtScope.cols does not describe the result
		// columns of the statement. We should use physical.Presentation
		// instead.
		isSQLProcedure := cf.IsProcedure && language == tree.RoutineLangSQL
		err = validateReturnType(b.ctx, b.semaCtx, funcReturnType, stmtScope.cols, isSQLProcedure)
		if err != nil {
			panic(err)
		}
	}

	if targetVolatility == tree.RoutineImmutable && len(deps) > 0 {
		panic(
			pgerror.Newf(
				pgcode.InvalidParameterValue,
				"referencing relations is not allowed in immutable function",
			),
		)
	}

	// Override the function body so that references are fully qualified.
	for i, option := range cf.Options {
		if _, ok := option.(tree.RoutineBodyStr); ok {
			cf.Options[i] = tree.RoutineBodyStr(fmtCtx.CloseAndGetString())
			break
		}
	}

	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructCreateFunction(
		&memo.CreateFunctionPrivate{
			Schema:   schID,
			Syntax:   cf,
			Deps:     deps,
			TypeDeps: typeDeps,
			FuncDeps: functionDeps,
		},
	)
	return outScope
}

func formatFuncBodyStmt(
	fmtCtx *tree.FmtCtx, ast tree.NodeFormatter, lang tree.RoutineLanguage, newLine bool,
) {
	if newLine {
		fmtCtx.WriteString("\n")
	}
	fmtCtx.FormatNode(ast)
	if lang != tree.RoutineLangPLpgSQL {
		// PL/pgSQL body statements handle semicolons.
		fmtCtx.WriteString(";")
	}
}

func validateReturnType(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	expected *types.T,
	cols []scopeColumn,
	isSQLProcedure bool,
) error {
	// The return type must be supported by the current cluster version.
	checkUnsupportedType(ctx, semaCtx, expected)
	for i := range cols {
		checkUnsupportedType(ctx, semaCtx, cols[i].typ)
	}

	// If return type is void, any column types are valid.
	if expected.Equivalent(types.Void) {
		return nil
	}

	if len(cols) == 0 {
		return pgerror.WithCandidateCode(
			errors.WithDetail(
				errors.Newf("return type mismatch in function declared to return %s", expected.Name()),
				"Function's final statement must be SELECT or INSERT/UPDATE/DELETE RETURNING.",
			),
			pgcode.InvalidFunctionDefinition,
		)
	}

	// If return type is RECORD and the tuple content types unspecified by OUT
	// parameters, any column types are valid. This is the case when we have
	// RETURNS RECORD without OUT params - we don't need to check the types
	// below.
	if types.IsRecordType(expected) && types.IsWildcardTupleType(expected) {
		return nil
	}

	if len(cols) == 1 {
		typeToCheck := expected
		if isSQLProcedure && types.IsRecordType(expected) && len(expected.TupleContents()) == 1 {
			// For SQL procedures with output parameters we get a record type
			// even with a single column.
			typeToCheck = expected.TupleContents()[0]
		}
		if !typeToCheck.Equivalent(cols[0].typ) &&
			!cast.ValidCast(cols[0].typ, typeToCheck, cast.ContextAssignment) {
			return pgerror.WithCandidateCode(
				errors.WithDetailf(
					errors.Newf("return type mismatch in function declared to return %s", expected.Name()),
					"Actual return type is %s", cols[0].typ.Name(),
				),
				pgcode.InvalidFunctionDefinition,
			)
		}
		return nil
	}

	// If the last statement return multiple columns, then the expected Family
	// should be a tuple type.
	if expected.Family() != types.TupleFamily {
		return pgerror.WithCandidateCode(
			errors.WithDetailf(
				errors.Newf("return type mismatch in function declared to return %s", expected.Name()),
				"Actual return type is record",
			),
			pgcode.InvalidFunctionDefinition,
		)
	}

	i := 0
	for _, typ := range expected.TupleContents() {
		if i < len(cols) {
			if !typ.Equivalent(cols[i].typ) {
				return pgerror.WithCandidateCode(
					errors.WithDetailf(
						errors.Newf("return type mismatch in function declared to return %s", expected.Name()),
						"Final statement returns %s instead of %s at column %d",
						cols[i].typ.Name(), typ.Name(), i+1,
					),
					pgcode.InvalidFunctionDefinition,
				)
			}
			i++
			continue
		}

		// Ran out of columns from last statement.
		return pgerror.WithCandidateCode(
			errors.WithDetailf(
				errors.Newf("return type mismatch in function declared to return %s", expected.Name()),
				"Final statement returns too few columns",
			),
			pgcode.InvalidFunctionDefinition,
		)
	}

	// If there are more columns from last statement than the tuple.
	if i < len(cols) {
		return pgerror.WithCandidateCode(
			errors.WithDetailf(
				errors.New("return type mismatch in function declared to return record"),
				"Final statement returns too many columns",
			),
			pgcode.InvalidFunctionDefinition,
		)
	}

	return nil
}

func checkStmtVolatility(
	expectedVolatility tree.RoutineVolatility, stmtScope *scope, stmt fmt.Stringer,
) {
	switch expectedVolatility {
	case tree.RoutineImmutable:
		if stmtScope.expr.Relational().VolatilitySet.HasVolatile() {
			panic(pgerror.Newf(pgcode.InvalidParameterValue, "volatile statement not allowed in immutable function: %s", stmt.String()))
		}
		if stmtScope.expr.Relational().VolatilitySet.HasStable() {
			panic(pgerror.Newf(pgcode.InvalidParameterValue, "stable statement not allowed in immutable function: %s", stmt.String()))
		}
	case tree.RoutineStable:
		if stmtScope.expr.Relational().VolatilitySet.HasVolatile() {
			panic(pgerror.Newf(pgcode.InvalidParameterValue, "volatile statement not allowed in stable function: %s", stmt.String()))
		}
	}
}

func checkUnsupportedType(ctx context.Context, semaCtx *tree.SemaContext, typ *types.T) {
	if err := tree.CheckUnsupportedType(ctx, semaCtx, typ); err != nil {
		panic(err)
	}
}

func checkDuplicateParamName(param tree.RoutineParam, seen map[tree.Name]struct{}) {
	if _, ok := seen[param.Name]; ok {
		// Argument names cannot be used more than once.
		panic(pgerror.Newf(
			pgcode.InvalidFunctionDefinition, "parameter name %q used more than once", param.Name,
		))
	}
	seen[param.Name] = struct{}{}
}
