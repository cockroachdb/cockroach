// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// CDCFunctionResolver is a function resolver specific used by CDC expression
// evaluation.
type CDCFunctionResolver struct {
}

// ResolveFunction implements FunctionReferenceResolver interface.
func (rs *CDCFunctionResolver) ResolveFunction(
	ctx context.Context, name *tree.UnresolvedName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	fn, err := name.ToFunctionName()
	if err != nil {
		return nil, err
	}

	// Check CDC function first.
	cdcFuncDef, found := cdcFunctions[fn.Object()]
	if !found {
		// Try a bit harder
		cdcFuncDef, found = cdcFunctions[strings.ToLower(fn.Object())]
	}

	if found && cdcFuncDef != useDefaultBuiltin {
		return cdcFuncDef, nil
	}

	// Resolve builtin.
	funcDef, err := tree.GetBuiltinFuncDefinition(fn, path)
	if err != nil {
		return nil, err
	}
	if funcDef == nil {
		return nil, errors.AssertionFailedf("function %s does not exist", fn.String())
	}
	return funcDef, nil
}

// ResolveFunctionByOID implements FunctionReferenceResolver interface.
func (rs *CDCFunctionResolver) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (string, *tree.Overload, error) {
	// CDC doesn't support user defined function yet, so there's no need to
	// resolve function by OID.
	return "", nil, errors.AssertionFailedf("unimplemented yet")
}
