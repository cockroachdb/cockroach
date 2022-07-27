// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cdceval

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type CDCFunctionResolver struct{}

func (rs *CDCFunctionResolver) ResolveFunction(
	ctx context.Context, name *tree.UnresolvedName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	un, err := name.ToUnresolvedObjectName(tree.NoAnnotation)
	if err != nil {
		return nil, err
	}
	fn := un.ToFunctionName()

	if fn.ExplicitSchema {
		// CDC functions don't have schema prefixes. So if given explicit schema
		// name, we only need to look at other non-CDC builtin function.
		funcDef, err := tree.GetBuiltinFuncDefinition(&fn, path)
		if err != nil {
			return nil, err
		}
		if funcDef == nil {
			return nil, errors.AssertionFailedf("function %s does not exist", fn.String())
		}
		return funcDef, nil
	}

	// Check CDC function first.
	cdcFuncDef, found := cdcFunctions[fn.Object()]
	if found {
		// The schema name is actually not important since CDC doesn't use any user
		// defined functions. And, we're sure that we always return the first
		// function definition found.
		return tree.PrefixBuiltinFunctionDefinition(cdcFuncDef, catconstants.PublicSchemaName), nil
	}

	cdcFuncDef, found = cdcFunctions[strings.ToLower(fn.Object())]
	if found {
		// The schema name is actually not important since CDC doesn't use any user
		// defined functions. And, we're sure that we always return the first
		// function definition found.
		return tree.PrefixBuiltinFunctionDefinition(cdcFuncDef, catconstants.PublicSchemaName), nil
	}

	funcDef, err := tree.GetBuiltinFuncDefinition(&fn, path)
	if err != nil {
		return nil, err
	}
	if funcDef == nil {
		return nil, errors.AssertionFailedf("function %s does not exist", fn.String())
	}
	return funcDef, nil
}

func (rs *CDCFunctionResolver) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (*tree.Overload, error) {
	return nil, errors.AssertionFailedf("unimplemented yet")
}
