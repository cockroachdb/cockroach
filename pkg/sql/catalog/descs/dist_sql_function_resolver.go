// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// DistSQLFunctionResolver is a tree.FunctionReferenceResolver that resolves
// builtin function by name while resolves user defined function by oid. Given
// the fact that expression in tables are serialized. We only serialize user
// defined function into OID references, but not builtin functions.
// TODO(chengxiong): extract resolution logics in schema_resolver.go into
// separate package so that it can be reused here.
type DistSQLFunctionResolver struct {
	g ByIDGetter
}

// NewDistSQLFunctionResolver returns a new DistSQLFunctionResolver.
func NewDistSQLFunctionResolver(descs *Collection, txn *kv.Txn) *DistSQLFunctionResolver {
	return &DistSQLFunctionResolver{
		g: descs.ByIDWithLeased(txn).Get(),
	}
}

// ResolveFunction implements tree.FunctionReferenceResolver interface.
// It only resolves builtin functions.
// TODO(chengxiong): extract resolution logics in schema_resolver.go into
// separate package so that it can be reused here.
func (d *DistSQLFunctionResolver) ResolveFunction(
	ctx context.Context, name *tree.UnresolvedName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	fn, err := name.ToFunctionName()
	if err != nil {
		return nil, err
	}
	// Get builtin and udf functions if there is any match.
	builtinDef, err := tree.GetBuiltinFuncDefinition(fn, path)
	if err != nil {
		return nil, err
	}
	if builtinDef == nil {
		return nil, errors.Wrapf(tree.ErrFunctionUndefined, "function %s not found", fn.Object())
	}
	return builtinDef, nil
}

// ResolveFunctionByOID implements tree.FunctionReferenceResolver interface.
// It only resolves user defined functions.
// TODO(chengxiong): extract resolution logics in schema_resolver.go into
// separate package so that it can be reused here.
func (d *DistSQLFunctionResolver) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (*tree.FunctionName, *tree.Overload, error) {
	if !funcdesc.IsOIDUserDefinedFunc(oid) {
		return nil, nil, errors.Wrapf(tree.ErrFunctionUndefined, "function %d not found", oid)
	}
	descID := funcdesc.UserDefinedFunctionOIDToID(oid)
	funcDesc, err := d.g.Function(ctx, descID)
	if err != nil {
		return nil, nil, err
	}
	ret, err := funcDesc.ToOverload()
	if err != nil {
		return nil, nil, err
	}
	db, err := d.g.Database(ctx, funcDesc.GetParentID())
	if err != nil {
		return nil, nil, err
	}
	sc, err := d.g.Schema(ctx, funcDesc.GetParentSchemaID())
	if err != nil {
		return nil, nil, err
	}
	fnName := tree.MakeQualifiedFunctionName(db.GetName(), sc.GetName(), funcDesc.GetName())
	return &fnName, ret, nil
}
