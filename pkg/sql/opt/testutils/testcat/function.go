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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

var _ tree.FunctionReferenceResolver = (*Catalog)(nil)

// ResolveFunction part of the tree.FunctionReferenceResolver interface.
func (tc *Catalog) ResolveFunction(
	name *tree.UnresolvedName, path tree.SearchPath,
) (*tree.FunctionDefinition, error) {
	// Attempt to resolve to a built-in function first.
	if def, err := name.ResolveFunction(path); err == nil {
		return def, nil
	}
	// Otherwise, try to resolve to a user-defined function.
	if def, ok := tc.udfs[name.String()]; ok {
		return def, nil
	}
	return nil, pgerror.Newf(pgcode.UndefinedFunction, "unknown function: %s", name)
}

// CreateFunction handles the CREATE FUNCTION statement.
func (tc *Catalog) CreateFunction(c *tree.CreateFunction) {
	name := c.FuncName.String()
	if _, ok := tree.FunDefs[name]; ok {
		panic(fmt.Errorf("built-in function with name %q already exists", name))
	}
	if _, ok := tc.udfs[name]; ok {
		panic(fmt.Errorf("user-defined function with name %q already exists", name))
	}
	if c.RoutineBody != nil {
		panic(fmt.Errorf("routine body of BEGIN ATOMIC is not supported"))
	}

	// Resolve the argument names and types.
	argTypes := make(tree.ArgTypes, len(c.Args))
	for i := range c.Args {
		arg := &c.Args[i]
		typ, err := tree.ResolveType(context.Background(), arg.Type, tc)
		if err != nil {
			panic(err)
		}
		argTypes.SetAt(i, arg.Name.String(), typ)
	}

	// Resolve the return type.
	retType, err := tree.ResolveType(context.Background(), c.ReturnType.Type, tc)
	if err != nil {
		panic(err)
	}

	// Retrieve the function body, volatility, and nullableArgs.
	body, v, nullableArgs := collectFuncOptions(c.Options)

	if tc.udfs == nil {
		tc.udfs = make(map[string]*tree.FunctionDefinition)
	}
	tc.udfs[name] = tree.NewFunctionDefinition(
		name,
		&tree.FunctionProperties{
			// TODO(mgartner): Consider setting Class and CompositeInsensitive.
		},
		[]tree.Overload{{
			Types:        argTypes,
			ReturnType:   tree.FixedReturnType(retType),
			Body:         body,
			Volatility:   v,
			NullableArgs: nullableArgs,
		}},
	)
}

func collectFuncOptions(o tree.FunctionOptions) (body string, v volatility.V, nullableArgs bool) {
	// The default volatility is VOLATILE.
	v = volatility.Volatile

	// The default leakproof option is NOT LEAKPROOF.
	leakproof := false

	// The default for nullableArgs is CALLED ON NULL INPUT, which is equivalent
	// to NullableArgs=true in function overloads.
	nullableArgs = true

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
				nullableArgs = false
			}

		case tree.FunctionLanguage:
			if t != tree.FunctionLangSQL {
				panic(fmt.Errorf("LANGUAGE must be SQL"))
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

	return body, v, nullableArgs
}

// formatFunction nicely formats a function definition creating in the opt test
// catalog using a treeprinter for debugging and testing.
func formatFunction(fn *tree.FunctionDefinition) string {
	if len(fn.Definition) != 1 {
		panic(fmt.Errorf("functions with multiple overloads not supported"))
	}
	o := fn.Definition[0].(*tree.Overload)
	tp := treeprinter.New()
	nullStr := ""
	if !o.NullableArgs {
		nullStr = ", nullable-args=false"
	}
	child := tp.Childf(
		"FUNCTION %s%s [%s%s]",
		fn.Name, o.Signature(false /* simplify */), o.Volatility, nullStr,
	)
	child.Child(o.Body)
	return tp.String()
}
