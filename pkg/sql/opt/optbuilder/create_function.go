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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/plsql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree/utils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

func (b *Builder) buildCreateFunction(cf *tree.CreateFunction, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	if cf.FuncName.ExplicitCatalog {
		if string(cf.FuncName.CatalogName) != b.evalCtx.SessionData().Database {
			panic(unimplemented.New("CREATE FUNCTION", "cross-db references not supported"))
		}
	}

	sch, resName := b.resolveSchemaForCreateFunction(&cf.FuncName)
	schID := b.factory.Metadata().AddSchema(sch)
	cf.FuncName.ObjectNamePrefix = resName
	preFuncResolver := b.semaCtx.FunctionResolver

	b.insideFuncDef = true
	b.trackSchemaDeps = true
	// Make sure datasource names are qualified.
	b.qualifyDataSourceNamesInAST = true
	defer func() {
		b.insideFuncDef = false
		b.trackSchemaDeps = false
		b.schemaDeps = nil
		b.schemaTypeDeps = intsets.Fast{}
		b.qualifyDataSourceNamesInAST = false
		b.semaCtx.FunctionResolver = preFuncResolver

		switch recErr := recover().(type) {
		case nil:
			// No error.
		case error:
			if errors.Is(recErr, tree.ErrFunctionUndefined) {
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

	if err := tree.ValidateFuncOptions(cf.Options); err != nil {
		panic(err)
	}

	// Look for function body string from function options.
	// Note that function body can be an empty string.
	funcBodyFound := false
	languageFound := false
	var lang catpb.Function_Language
	var funcBodyStr string
	var err error
	for _, option := range cf.Options {
		switch opt := option.(type) {
		case tree.FunctionBodyStr:
			funcBodyFound = true
			funcBodyStr = string(opt)
		case tree.FunctionLanguage:
			languageFound = true
			// Check the language here, before attempting to parse the function body.
			if lang, err = funcinfo.FunctionLangToProto(opt); err != nil {
				panic(err)
			}

			if opt == tree.FunctionLangPLpgSQL {
				if err := utils.ParseAndCollectTelemetryForPLpgSQLFunc(cf); err != nil {
					// Until plpgsql is fully implemented DealWithPlpgSQlFunc will always
					// return an error.
					panic(err)
				}
			}
		}
	}

	if !funcBodyFound {
		panic(pgerror.New(pgcode.InvalidFunctionDefinition, "no function body specified"))
	}
	if !languageFound {
		panic(pgerror.New(pgcode.InvalidFunctionDefinition, "no language specified"))
	}

	// Track the dependencies in the arguments, return type, and statements in
	// the function body.
	var deps opt.SchemaDeps
	var typeDeps opt.SchemaTypeDeps

	// Collect the user defined type dependency of the return type.
	funcReturnType, err := tree.ResolveType(b.ctx, cf.ReturnType.Type, b.semaCtx.TypeResolver)
	if err != nil {
		panic(err)
	}
	typedesc.GetTypeDescriptorClosure(funcReturnType).ForEach(func(id descpb.ID) {
		typeDeps.Add(int(id))
	})

	// bodyScope is the base scope for each statement in the body. We add the
	// named parameters to the scope so that references to them in the body can
	// be resolved.
	bodyScope := b.allocScope()
	paramTypes := make(tree.ParamTypes, len(cf.Params))
	for i := range cf.Params {
		param := &cf.Params[i]
		typ, err := tree.ResolveType(b.ctx, param.Type, b.semaCtx.TypeResolver)
		if err != nil {
			panic(err)
		}

		// Add the parameter to the base scope of the body.
		paramColName := funcParamColName(param.Name, i)
		col := b.synthesizeColumn(bodyScope, paramColName, typ, nil /* expr */, nil /* scalar */)
		col.setParamOrd(i)

		// Collect the user defined type dependencies.
		typedesc.GetTypeDescriptorClosure(typ).ForEach(func(id descpb.ID) {
			typeDeps.Add(int(id))
		})

		paramTypes[i] = tree.ParamType{Name: param.Name.String(), Typ: typ}
	}

	b.semaCtx.FunctionResolver = b.makeRecursiveFunctionResolver(
		cf, sch.Name(), paramTypes, funcReturnType,
	)

	// Parse the function body.
	var stmts statements.Statements
	switch lang {
	case catpb.Function_SQL:
		stmts, err = parser.Parse(funcBodyStr)
	case catpb.Function_PLPGSQL:
		_, err = plsql.Parse(funcBodyStr)
	}
	if err != nil {
		panic(err)
	}

	targetVolatility := tree.GetFuncVolatility(cf.Options)
	// Validate each statement and collect the dependencies.
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	for i, stmt := range stmts {
		var stmtScope *scope
		// We need to disable stable function folding because we want to catch the
		// volatility of stable functions. If folded, we only get a scalar and lose
		// the volatility.
		b.factory.FoldingControl().TemporarilyDisallowStableFolds(func() {
			stmtScope = b.buildStmt(stmts[i].AST, nil /* desiredTypes */, bodyScope)
		})
		checkStmtVolatility(targetVolatility, stmtScope, stmt.AST)

		// Format the statements with qualified datasource names.
		formatFuncBodyStmt(fmtCtx, stmt.AST, i > 0 /* newLine */)

		// Validate that the result type of the last statement matches the
		// return type of the function.
		if i == len(stmts)-1 {
			// TODO(mgartner): stmtScope.cols does not describe the result
			// columns of the statement. We should use physical.Presentation
			// instead.
			err := validateReturnType(funcReturnType, stmtScope.cols)
			if err != nil {
				panic(err)
			}
		}

		deps = append(deps, b.schemaDeps...)
		typeDeps.UnionWith(b.schemaTypeDeps)
		// Add statement ast into CreateFunction node for logging purpose.
		cf.BodyStatements = append(cf.BodyStatements, stmt.AST)
		// Reset the tracked dependencies for next statement.
		b.schemaDeps = nil
		b.schemaTypeDeps = intsets.Fast{}
	}

	if targetVolatility == tree.FunctionImmutable && len(deps) > 0 {
		panic(
			pgerror.Newf(
				pgcode.InvalidParameterValue,
				"referencing relations is not allowed in immutable function",
			),
		)
	}

	// Override the function body so that references are fully qualified.
	for i, option := range cf.Options {
		if _, ok := option.(tree.FunctionBodyStr); ok {
			cf.Options[i] = tree.FunctionBodyStr(fmtCtx.String())
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

func formatFuncBodyStmt(fmtCtx *tree.FmtCtx, ast tree.Statement, newLine bool) {
	if newLine {
		fmtCtx.WriteString("\n")
	}
	fmtCtx.FormatNode(ast)
	fmtCtx.WriteString(";")
}

func validateReturnType(expected *types.T, cols []scopeColumn) error {
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
						errors.Newf("return type mismatch in function declared to return record"),
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
				errors.New("return type mismatch in function declared to return record"),
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
	expectedVolatility tree.FunctionVolatility, stmtScope *scope, stmt tree.Statement,
) {
	switch expectedVolatility {
	case tree.FunctionImmutable:
		if stmtScope.expr.Relational().VolatilitySet.HasVolatile() {
			panic(pgerror.Newf(pgcode.InvalidParameterValue, "volatile statement not allowed in immutable function: %s", stmt.String()))
		}
		if stmtScope.expr.Relational().VolatilitySet.HasStable() {
			panic(pgerror.Newf(pgcode.InvalidParameterValue, "stable statement not allowed in immutable function: %s", stmt.String()))
		}
	case tree.FunctionStable:
		if stmtScope.expr.Relational().VolatilitySet.HasVolatile() {
			panic(pgerror.Newf(pgcode.InvalidParameterValue, "volatile statement not allowed in stable function: %s", stmt.String()))
		}
	}
}

// recursiveFunctionResolver first attempts to resolve a function reference by
// name as an existing function, but if this fails, it attempts to resolve the
// reference as a recursive call. For a recursive function call,
// recursiveFunctionResolver returns a placeholder function overload that
// contains enough information to satisfy type-checking while the function is
// created.
type recursiveFunctionResolver struct {
	res  tree.FunctionReferenceResolver
	name *tree.FunctionName
	def  *tree.ResolvedFunctionDefinition
}

// ResolveFunction implements the FunctionReferenceResolver interface.
func (r *recursiveFunctionResolver) ResolveFunction(
	ctx context.Context, name *tree.UnresolvedName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	def, err := r.res.ResolveFunction(ctx, name, path)
	if err != nil && errors.Is(err, tree.ErrFunctionUndefined) {
		if fn, err := name.ToFunctionName(); err == nil {
			if (!fn.ExplicitCatalog || fn.Catalog() != r.name.Catalog()) &&
				(!fn.ExplicitSchema || fn.Schema() != r.name.Schema()) &&
				fn.Object() == r.name.Object() {
				//nolint:returnerrcheck
				return r.def, nil
			}
		}
	}
	return def, err
}

// ResolveFunctionByOID implements the FunctionReferenceResolver interface.
func (r *recursiveFunctionResolver) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (*tree.FunctionName, *tree.Overload, error) {
	return r.res.ResolveFunctionByOID(ctx, oid)
}

func (b *Builder) makeRecursiveFunctionResolver(
	cf *tree.CreateFunction,
	schemaName *cat.SchemaName,
	paramTypes tree.TypeList,
	returnType *types.T,
) *recursiveFunctionResolver {
	var functionVol tree.FunctionVolatility
	var isLeakproof, calledOnNullInput bool
	for _, option := range cf.Options {
		switch t := option.(type) {
		case tree.FunctionVolatility:
			functionVol = t
		case tree.FunctionLeakproof:
			isLeakproof = true
		case tree.FunctionNullInputBehavior:
			calledOnNullInput = t == tree.FunctionCalledOnNullInput
		}
	}

	var vol volatility.V
	switch functionVol {
	case tree.FunctionVolatile:
		vol = volatility.Volatile
	case tree.FunctionStable:
		vol = volatility.Stable
	case tree.FunctionImmutable:
		vol = volatility.Immutable
	default:
		panic(errors.AssertionFailedf("unknown volatility"))
	}
	if isLeakproof {
		if vol != volatility.Immutable {
			panic(errors.AssertionFailedf("function is leakproof but not immutable"))
		}
		vol = volatility.Leakproof
	}

	qualifiedFnName := tree.MakeFunctionNameFromPrefix(*schemaName, tree.Name(cf.FuncName.Object()))
	return &recursiveFunctionResolver{
		res:  b.semaCtx.FunctionResolver,
		name: &qualifiedFnName,
		def: tree.QualifyBuiltinFunctionDefinition(
			tree.NewFunctionDefinition(
				cf.FuncName.Object(),
				&tree.FunctionProperties{},
				[]tree.Overload{
					{
						Types:             paramTypes,
						ReturnType:        tree.FixedReturnType(returnType),
						Volatility:        vol,
						IsUDF:             true,
						ReturnSet:         cf.ReturnType.IsSet,
						CalledOnNullInput: calledOnNullInput,
					},
				}),
			schemaName.Schema(),
		),
	}
}
