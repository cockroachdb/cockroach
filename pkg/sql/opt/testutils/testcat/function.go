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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ tree.FunctionReferenceResolver = (*Catalog)(nil)

// ResolveFunction part of the tree.FunctionReferenceResolver interface.
func (tc *Catalog) ResolveFunction(
	ctx context.Context, name *tree.UnresolvedName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	fn, err := name.ToRoutineName()
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
) (*tree.RoutineName, *tree.Overload, error) {
	return nil, nil, errors.AssertionFailedf("ResolveFunctionByOID not supported in test catalog")
}

// ResolveProcedure is part of the tree.FunctionReferenceResolver interface.
func (tc *Catalog) ResolveProcedure(
	ctx context.Context, name *tree.UnresolvedObjectName, path tree.SearchPath,
) (*tree.Overload, error) {
	if def, ok := tc.udfs[name.String()]; ok {
		o := def.Overloads[0]
		if o.IsProcedure {
			return o.Overload, nil
		}
	}
	return nil, sqlerrors.NewProcedureUndefinedError(name)
}

// CreateRoutine handles the CREATE FUNCTION statement.
func (tc *Catalog) CreateRoutine(c *tree.CreateRoutine) {
	name := c.Name.String()
	if _, ok := tree.FunDefs[name]; ok {
		panic(fmt.Errorf("built-in function with name %q already exists", name))
	}
	if _, ok := tc.udfs[name]; ok {
		if c.Replace {
			delete(tc.udfs, name)
		} else {
			// TODO(mgartner): The test catalog should support multiple overloads
			// with the same name if their arguments are different.
			panic(fmt.Errorf("user-defined function with name %q already exists", name))
		}
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
	body, v, calledOnNullInput, language := collectFuncOptions(c.Options)

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
		Language:          language,
		IsProcedure:       c.IsProcedure,
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
	o tree.RoutineOptions,
) (body string, v volatility.V, calledOnNullInput bool, language tree.RoutineLanguage) {
	// The default volatility is VOLATILE.
	v = volatility.Volatile

	// The default leakproof option is NOT LEAKPROOF.
	leakproof := false

	// The default is CALLED ON NULL INPUT, which is equivalent to
	// CalledOnNullInput=true in function overloads.
	calledOnNullInput = true

	language = tree.RoutineLangUnknown

	for _, option := range o {
		switch t := option.(type) {
		case tree.RoutineBodyStr:
			body = strings.Trim(string(t), "\n")

		case tree.RoutineVolatility:
			switch t {
			case tree.RoutineImmutable:
				v = volatility.Immutable
			case tree.RoutineStable:
				v = volatility.Stable
			}

		case tree.RoutineLeakproof:
			leakproof = bool(t)

		case tree.RoutineNullInputBehavior:
			switch t {
			case tree.RoutineReturnsNullOnNullInput, tree.RoutineStrict:
				calledOnNullInput = false
			}

		case tree.RoutineLanguage:
			if t != tree.RoutineLangSQL && t != tree.RoutineLangPLpgSQL {
				panic(fmt.Errorf("LANGUAGE must be SQL or plpgsql"))
			}
			language = t

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

	return body, v, calledOnNullInput, language
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
