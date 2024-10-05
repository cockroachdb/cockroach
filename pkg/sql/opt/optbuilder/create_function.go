// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	// TODO(chengxiong,mgartner): this is a hack to disallow UDF usage in UDF and
	// we will need to lift this hack when we plan to allow it.
	preFuncResolver := b.semaCtx.FunctionResolver
	b.semaCtx.FunctionResolver = nil

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

		b.semaCtx.FunctionResolver = preFuncResolver
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

	if err := tree.ValidateRoutineOptions(cf.Options); err != nil {
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
		if err := plpgsql.CheckClusterSupportsPLpgSQL(b.evalCtx.Settings, b.evalCtx.ClusterID); err != nil {
			panic(err)
		}
	}

	// Track the dependencies in the arguments, return type, and statements in
	// the function body.
	var deps opt.SchemaDeps
	var typeDeps opt.SchemaTypeDeps

	afterBuildStmt := func() {
		deps = append(deps, b.schemaDeps...)
		typeDeps.UnionWith(b.schemaTypeDeps)
		// Reset the tracked dependencies for next statement.
		b.schemaDeps = nil
		b.schemaTypeDeps = intsets.Fast{}

		// Reset the annotations to the original values
		b.evalCtx.Annotations = oldEvalCtxAnn
		b.semaCtx.Annotations = oldSemaCtxAnn
	}

	// bodyScope is the base scope for each statement in the body. We add the
	// named parameters to the scope so that references to them in the body can
	// be resolved.
	bodyScope := b.allocScope()
	var paramTypes tree.ParamTypes
	for i := range cf.Params {
		param := &cf.Params[i]
		typ, err := tree.ResolveType(b.ctx, param.Type, b.semaCtx.TypeResolver)
		if err != nil {
			panic(err)
		}
		if param.Class == tree.RoutineParamIn || param.Class == tree.RoutineParamInOut {
			if typ.Family() == types.VoidFamily {
				panic(pgerror.Newf(pgcode.InvalidFunctionDefinition, "SQL functions cannot have arguments of type VOID"))
			}
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

		// Add the parameter to the base scope of the body.
		paramColName := funcParamColName(param.Name, i)
		col := b.synthesizeColumn(bodyScope, paramColName, typ, nil /* expr */, nil /* scalar */)
		col.setParamOrd(i)

		// Collect the user defined type dependencies.
		typedesc.GetTypeDescriptorClosure(typ).ForEach(func(id descpb.ID) {
			typeDeps.Add(int(id))
		})

		// Collect the parameters for PLpgSQL routines.
		if language == tree.RoutineLangPLpgSQL {
			paramTypes = append(paramTypes, tree.ParamType{
				Name: param.Name.String(),
				Typ:  typ,
			})
		}
	}

	// Collect the user defined type dependency of the return type.
	funcReturnType, err := tree.ResolveType(b.ctx, cf.ReturnType.Type, b.semaCtx.TypeResolver)
	if err != nil {
		panic(err)
	}
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
			formatFuncBodyStmt(fmtCtx, stmt.AST, i > 0 /* newLine */)
			afterBuildStmt()
		}
	case tree.RoutineLangPLpgSQL:
		if cf.ReturnType.SetOf {
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
			var plBuilder plpgsqlBuilder
			plBuilder.init(b, nil /* colRefs */, paramTypes, stmt.AST, funcReturnType)
			stmtScope = plBuilder.build(stmt.AST, bodyScope)
		})
		checkStmtVolatility(targetVolatility, stmtScope, stmt)

		// Format the statements with qualified datasource names.
		formatFuncBodyStmt(fmtCtx, stmt.AST, false /* newLine */)
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
		err = validateReturnType(b.ctx, b.semaCtx, funcReturnType, stmtScope.cols)
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
		},
	)
	return outScope
}

func formatFuncBodyStmt(fmtCtx *tree.FmtCtx, ast tree.NodeFormatter, newLine bool) {
	if newLine {
		fmtCtx.WriteString("\n")
	}
	fmtCtx.FormatNode(ast)
	fmtCtx.WriteString(";")
}

func validateReturnType(
	ctx context.Context, semaCtx *tree.SemaContext, expected *types.T, cols []scopeColumn,
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

	// If return type is RECORD, any column types are valid.
	if types.IsRecordType(expected) {
		return nil
	}

	if len(cols) == 1 {
		if !expected.Equivalent(cols[0].typ) &&
			!cast.ValidCast(cols[0].typ, expected, cast.ContextAssignment) {
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
