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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// CDCFunctionResolver is a function resolver specific used by CDC expression
// evaluation.
type CDCFunctionResolver struct {
	prevRowFn *tree.ResolvedFunctionDefinition
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

	if !found {
		// Try internal cdc function.
		if funDef := rs.resolveInternalCDCFn(name); funDef != nil {
			return funDef, nil
		}
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

// resolveInternalCDCFn resolves special internal functions we install for CDC.
// Resolve functions which are used internally by CDC, but are not exposed to
// end users.
func (rs *CDCFunctionResolver) resolveInternalCDCFn(
	name *tree.UnresolvedName,
) *tree.ResolvedFunctionDefinition {
	fnName := name.Parts[0]
	switch name.NumParts {
	case 1:
	case 2:
		if name.Parts[1] != "crdb_internal" {
			return nil
		}
	default:
		return nil
	}

	switch fnName {
	case prevRowFnName.Parts[0]:
		return rs.prevRowFn
	}
	return nil
}

func (rs *CDCFunctionResolver) setPrevFuncForEventDescriptor(
	d *cdcevent.EventDescriptor,
) *tree.DTuple {
	if d == nil {
		rs.prevRowFn = nil
		return nil
	}

	tupleType := cdcPrevType(d)
	rowTuple := tree.NewDTupleWithLen(tupleType, len(tupleType.InternalType.TupleContents))
	rs.prevRowFn = makePrevRowFn(rowTuple.ResolvedType())
	return rowTuple
}
