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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

func (b *Builder) buildCreateFunction(cf *tree.CreateFunction, inScope *scope) (outScope *scope) {
	b.DisableMemoReuse = true
	if cf.FuncName.ExplicitCatalog {
		if string(cf.FuncName.CatalogName) != b.evalCtx.SessionData().Database {
			panic(unimplemented.New("CREATE FUNCTION", "cross-db references not supported"))
		}
	}
	if cf.ReturnType.IsSet {
		panic(unimplemented.NewWithIssue(86391, "user-defined functions with SETOF return types are not supported"))
	}

	sch, resName := b.resolveSchemaForCreateFunction(&cf.FuncName)
	schID := b.factory.Metadata().AddSchema(sch)
	cf.FuncName.ObjectNamePrefix = resName

	// TODO(chengxiong,mgartner): this is a hack to disallow UDF usage in UDF and
	// we will need to lift this hack when we plan to allow it.
	preFuncResolver := b.semaCtx.FunctionResolver
	b.semaCtx.FunctionResolver = nil

	b.insideFuncDef = true
	b.trackSchemaDeps = true
	// Make sure datasource names are qualified.
	b.qualifyDataSourceNamesInAST = true
	defer func() {
		b.insideFuncDef = false
		b.trackSchemaDeps = false
		b.schemaDeps = nil
		b.schemaTypeDeps = util.FastIntSet{}
		b.qualifyDataSourceNamesInAST = false

		b.semaCtx.FunctionResolver = preFuncResolver
		maybePanicOnUnknownFunction("function body")
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
	var funcBodyStr string
	for _, option := range cf.Options {
		switch opt := option.(type) {
		case tree.FunctionBodyStr:
			funcBodyFound = true
			funcBodyStr = string(opt)
		case tree.FunctionLanguage:
			languageFound = true
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

	// bodyScope is the base scope for each statement in the body. We add the
	// named arguments to the scope so that references to them in the body can
	// be resolved.
	bodyScope := b.allocScope()
	for i := range cf.Args {
		arg := &cf.Args[i]
		typ, err := tree.ResolveType(b.ctx, arg.Type, b.semaCtx.TypeResolver)
		if err != nil {
			panic(err)
		}

		// Add the argument to the base scope of the body.
		argColName := funcArgColName(arg.Name, i)
		col := b.synthesizeColumn(bodyScope, argColName, typ, nil /* expr */, nil /* scalar */)
		col.setArgOrd(i)

		// Collect the user defined type dependencies.
		typeIDs, err := typedesc.GetTypeDescriptorClosure(typ)
		if err != nil {
			panic(err)
		}
		for typeID := range typeIDs {
			typeDeps.Add(int(typeID))
		}
	}

	// Collect the user defined type dependency of the return type.
	funcReturnType, err := tree.ResolveType(b.ctx, cf.ReturnType.Type, b.semaCtx.TypeResolver)
	if err != nil {
		panic(err)
	}
	typeIDs, err := typedesc.GetTypeDescriptorClosure(funcReturnType)
	if err != nil {
		panic(err)
	}
	for typeID := range typeIDs {
		typeDeps.Add(int(typeID))
	}

	// Parse the function body.
	stmts, err := parser.Parse(funcBodyStr)
	if err != nil {
		panic(err)
	}

	// Validate each statement and collect the dependencies.
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	for i, stmt := range stmts {
		stmtScope := b.buildStmt(stmts[i].AST, nil /* desiredTypes */, bodyScope)

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
		// Reset the tracked dependencies for next statement.
		b.schemaDeps = nil
		b.schemaTypeDeps = util.FastIntSet{}
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
