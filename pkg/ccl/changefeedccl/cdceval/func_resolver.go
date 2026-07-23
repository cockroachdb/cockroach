// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdceval

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/lib/pq/oid"
)

// cdcFunctionResolver is a function resolver specific used by CDC expression
// evaluation.
type cdcFunctionResolver struct {
	wrapped tree.FunctionReferenceResolver
}

// TODO(yevgeniy): Function resolution is complex
// (see https://github.com/cockroachdb/cockroach/issues/75101).  It would be nice if we
// could accomplish cdc goals without having to define custom resolver.
func newCDCFunctionResolver(wrapped tree.FunctionReferenceResolver) tree.FunctionReferenceResolver {
	return &cdcFunctionResolver{wrapped: wrapped}
}

// ResolveFunction implements FunctionReferenceResolver interface.
func (rs *cdcFunctionResolver) ResolveFunction(
	ctx context.Context, name tree.UnresolvedRoutineName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	fn, err := name.UnresolvedName().ToRoutineName()
	if err != nil {
		return nil, err
	}

	fnName := func() string {
		if !fn.ExplicitSchema {
			return fn.Object()
		}
		var sb strings.Builder
		sb.WriteString(fn.Schema())
		sb.WriteByte('.')
		sb.WriteString(fn.Object())
		return sb.String()
	}()

	if _, denied := functionDenyList[fnName]; denied {
		return nil, pgerror.Newf(pgcode.UndefinedFunction, "function %q unsupported by CDC", fnName)
	}

	// Check CDC function first.
	cdcFuncDef, found := cdcFunctions[fnName]
	if !found {
		// Try a bit harder
		cdcFuncDef, found = cdcFunctions[strings.ToLower(fnName)]
	}

	if found && cdcFuncDef != useDefaultBuiltin {
		return cdcFuncDef, nil
	}

	// Delegate to real resolver.
	funcDef, err := rs.wrapped.ResolveFunction(ctx, name, path)
	if err != nil {
		return nil, err
	}

	// Ensure that any overloads defined using a SQL string are supported.
	for _, overload := range funcDef.Overloads {
		if overload.HasSQLBody() {
			if err := checkOverloadSupported(fnName, overload.Overload); err != nil {
				return nil, err
			}
		}
	}
	return funcDef, nil
}

// ResolveFunctionByOID implements FunctionReferenceResolver interface.
func (rs *cdcFunctionResolver) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (*tree.RoutineName, *tree.Overload, error) {
	fnName, overload, err := rs.wrapped.ResolveFunctionByOID(ctx, oid)
	if err != nil {
		return nil, nil, err
	}
	if err := checkOverloadSupported(fnName.Object(), overload); err != nil {
		return nil, nil, err
	}
	return fnName, overload, err
}

// checkOverloadsSupported verifies function overload is supported by CDC.
func checkOverloadSupported(fnName string, overload *tree.Overload) error {
	switch overload.Class {
	case tree.AggregateClass, tree.GeneratorClass, tree.WindowClass:
		return pgerror.Newf(pgcode.UndefinedFunction, "aggregate, generator, or window function %q unsupported by CDC", fnName)
	}
	if overload.Volatility == volatility.Volatile {
		return pgerror.Newf(pgcode.UndefinedFunction, "volatile functions %q unsupported by CDC", fnName)
	}
	return nil
}
