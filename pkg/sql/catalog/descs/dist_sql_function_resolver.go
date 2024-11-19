// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
	ctx context.Context, name tree.UnresolvedRoutineName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	fn, err := name.UnresolvedName().ToRoutineName()
	if err != nil {
		return nil, err
	}
	// Get builtin and udf functions if there is any match.
	builtinDef, err := tree.GetBuiltinFuncDefinition(fn, path)
	if err != nil {
		return nil, err
	}
	if builtinDef == nil {
		return nil, errors.Mark(
			pgerror.Newf(pgcode.UndefinedFunction, "function %s not found", fn.Object()),
			tree.ErrRoutineUndefined,
		)
	}
	return builtinDef, nil
}

// ResolveFunctionByOID implements tree.FunctionReferenceResolver interface.
// It only resolves user defined functions.
// TODO(chengxiong): extract resolution logics in schema_resolver.go into
// separate package so that it can be reused here.
func (d *DistSQLFunctionResolver) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (*tree.RoutineName, *tree.Overload, error) {
	if !funcdesc.IsOIDUserDefinedFunc(oid) {
		return nil, nil, errors.Mark(
			pgerror.Newf(pgcode.UndefinedFunction, "function %d not found", oid),
			tree.ErrRoutineUndefined,
		)
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
	fnName := tree.MakeQualifiedRoutineName(db.GetName(), sc.GetName(), funcDesc.GetName())
	return &fnName, ret, nil
}
