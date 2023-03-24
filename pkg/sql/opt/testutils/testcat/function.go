// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcat

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ tree.FunctionReferenceResolver = (*Catalog)(nil)

// ResolveFunction part of the tree.FunctionReferenceResolver interface.
func (tc *Catalog) ResolveFunction(
	ctx context.Context, name *tree.UnresolvedName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	fn, err := name.ToFunctionName()
	if err != nil {
		return nil, err
	}

	// Attempt to resolve to a built-in function first.
	def, err := tree.GetBuiltinFuncDefinition(fn, path)
	if err != nil {
		return nil, err
	}
	if def != nil {
		return def, nil
	}
	// Otherwise, try to resolve to a user-defined function.
	if def, ok := tc.udfs[name.String()]; ok {
		return def, nil
	}
	return nil, errors.Wrapf(tree.ErrFunctionUndefined, "unknown function: %s", name)
}

// ResolveFunctionByOID part of the tree.FunctionReferenceResolver interface.
func (tc *Catalog) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (*tree.FunctionName, *tree.Overload, error) {
	return nil, nil, errors.AssertionFailedf("ResolveFunctionByOID not supported in test catalog")
}

// CreateFunction handles the CREATE FUNCTION statement.
func (tc *Catalog) CreateFunction(c *tree.CreateFunction) {
	name := c.FuncName.String()
	if _, ok := tree.FunDefs[name]; ok {
		panic(fmt.Errorf("built-in function with name %q already exists", name))
	}
	if _, ok := tc.udfs[name]; ok {
		// TODO(mgartner): The test catalog should support multiple overloads
		// with the same name if their arguments are different.
		panic(fmt.Errorf("user-defined function with name %q already exists", name))
	}
	if c.RoutineBody != nil {
		panic(fmt.Errorf("routine body of BEGIN ATOMIC is not supported"))
	}

	// Resolve the parameter names and types.
	paramTypes := make(tree.ParamTypes, len(c.Params))
	for i := range c.Params {
		param := &c.Params[i]
		typ, err := tree.ResolveType(context.Background(), param.Type, tc)
		if err != nil {
			panic(err)
		}
		paramTypes.SetAt(i, string(param.Name), typ)
	}

	// Resolve the return type.
	retType, err := tree.ResolveType(context.Background(), c.ReturnType.Type, tc)
	if err != nil {
		panic(err)
	}

	// Retrieve the function body, volatility, and calledOnNullInput.
	body, v, calledOnNullInput := collectFuncOptions(c.Options)

	if tc.udfs == nil {
		tc.udfs = make(map[string]*tree.ResolvedFunctionDefinition)
	}

	overload := &tree.Overload{
		Types:             paramTypes,
		ReturnType:        tree.FixedReturnType(retType),
		IsUDF:             true,
		Body:              body,
		Volatility:        v,
		CalledOnNullInput: calledOnNullInput,
	}
	if c.ReturnType.IsSet {
		overload.Class = tree.GeneratorClass
	}
	prefixedOverload := tree.MakeQualifiedOverload("public", overload)
	def := &tree.ResolvedFunctionDefinition{
		Name: name,
		// TODO(mgartner): Consider setting Class and CompositeInsensitive fo
		// overloads.
		Overloads: []tree.QualifiedOverload{prefixedOverload},
	}
	tc.udfs[name] = def
}

func collectFuncOptions(
	o tree.FunctionOptions,
) (body string, v volatility.V, calledOnNullInput bool) {
	// The default volatility is VOLATILE.
	v = volatility.Volatile

	// The default leakproof option is NOT LEAKPROOF.
	leakproof := false

	// The default is CALLED ON NULL INPUT, which is equivalent to
	// CalledOnNullInput=true in function overloads.
	calledOnNullInput = true

	for _, option := range o {
		switch t := option.(type) {
		case tree.FunctionBodyStr:
			body = strings.Trim(string(t), "\n")

		case tree.FunctionVolatility:
			switch t {
			case tree.FunctionImmutable:
				v = volatility.Immutable
			case tree.FunctionStable:
				v = volatility.Stable
			}

		case tree.FunctionLeakproof:
			leakproof = bool(t)

		case tree.FunctionNullInputBehavior:
			switch t {
			case tree.FunctionReturnsNullOnNullInput, tree.FunctionStrict:
				calledOnNullInput = false
			}

		case tree.FunctionLanguage:
			if t != tree.FunctionLangSQL && t != tree.FunctionLangPLpgSQL {
				panic(fmt.Errorf("LANGUAGE must be SQL or plpgsql"))
			}

		default:
			ctx := tree.NewFmtCtx(tree.FmtSimple)
			option.Format(ctx)
			panic(fmt.Errorf("function option %s is not supported", ctx.String()))
		}
	}

	if leakproof && v == volatility.Immutable {
		v = volatility.Leakproof
	} else if leakproof {
		panic(fmt.Errorf("LEAKPROOF functions must be IMMUTABLE"))
	}

	return body, v, calledOnNullInput
}

// formatFunction nicely formats a function definition creating in the opt test
// catalog using a treeprinter for debugging and testing.
func formatFunction(fn *tree.ResolvedFunctionDefinition) string {
	if len(fn.Overloads) != 1 {
		panic(fmt.Errorf("functions with multiple overloads not supported"))
	}
	o := fn.Overloads[0]
	tp := treeprinter.New()
	nullStr := ""
	if !o.CalledOnNullInput {
		nullStr = ", called-on-null-input=false"
	}
	child := tp.Childf(
		"FUNCTION %s%s [%s%s]",
		fn.Name, o.Signature(false /* simplify */), o.Volatility, nullStr,
	)
	child.Child(o.Body)
	return tp.String()
}
