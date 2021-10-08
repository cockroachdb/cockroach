// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// DistSQLTypeResolverFactory is an object that constructs TypeResolver objects
// that are bound under a transaction. These TypeResolvers access descriptors
// through the descs.Collection and eventually the lease.Manager. It cannot be
// used concurrently, and neither can the constructed TypeResolvers. After the
// DistSQLTypeResolverFactory is finished being used, all descriptors need to
// be released from Descriptors. It is intended to be used to resolve type
// references during the initialization of DistSQL flows.
type DistSQLTypeResolverFactory struct {
	Descriptors *Collection
	CleanupFunc func(ctx context.Context)
}

// NewTypeResolver creates a new TypeResolver that is bound under the input
// transaction. It returns a nil resolver if the factory itself is nil.
func (df *DistSQLTypeResolverFactory) NewTypeResolver(txn *kv.Txn) DistSQLTypeResolver {
	if df == nil {
		return DistSQLTypeResolver{}
	}
	return NewDistSQLTypeResolver(df.Descriptors, txn)
}

// NewSemaContext creates a new SemaContext with a TypeResolver bound to the
// input transaction.
func (df *DistSQLTypeResolverFactory) NewSemaContext(txn *kv.Txn) *tree.SemaContext {
	semaCtx := tree.MakeSemaContext()
	semaCtx.TypeResolver = df.NewTypeResolver(txn)
	return &semaCtx
}

// DistSQLTypeResolver is a TypeResolver that accesses TypeDescriptors through
// a given descs.Collection and transaction.
type DistSQLTypeResolver struct {
	descriptors *Collection
	txn         *kv.Txn
}

// NewDistSQLTypeResolver creates a new DistSQLTypeResolver.
func NewDistSQLTypeResolver(descs *Collection, txn *kv.Txn) DistSQLTypeResolver {
	return DistSQLTypeResolver{
		descriptors: descs,
		txn:         txn,
	}
}

// ResolveType implements the tree.TypeReferenceResolver interface.
func (dt DistSQLTypeResolver) ResolveType(
	context.Context, *tree.UnresolvedObjectName,
) (*types.T, error) {
	return nil, errors.AssertionFailedf("cannot resolve types in DistSQL by name")
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
func (dt DistSQLTypeResolver) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	id, err := typedesc.UserDefinedTypeOIDToID(oid)
	if err != nil {
		return nil, err
	}
	name, desc, err := dt.GetTypeDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	return desc.MakeTypesT(ctx, &name, dt)
}

// GetTypeDescriptor implements the sqlbase.TypeDescriptorResolver interface.
func (dt DistSQLTypeResolver) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	flags := tree.CommonLookupFlags{
		Required: true,
	}
	desc, err := dt.descriptors.getDescriptorByIDMaybeSetTxnDeadline(
		ctx, dt.txn, id, flags, false, /* setTxnDeadline */
	)
	if err != nil {
		return tree.TypeName{}, nil, err
	}
	typeDesc, isType := desc.(catalog.TypeDescriptor)
	if !isType {
		return tree.TypeName{}, nil, pgerror.Newf(pgcode.WrongObjectType,
			"descriptor %d is a %s not a %s", id, desc.DescriptorType(), catalog.Type)
	}
	name := tree.MakeUnqualifiedTypeName(desc.GetName())
	return name, typeDesc, nil
}

// HydrateTypeSlice installs metadata into a slice of types.T's.
func (dt DistSQLTypeResolver) HydrateTypeSlice(ctx context.Context, typs []*types.T) error {
	for _, t := range typs {
		if t.UserDefined() {
			id, err := typedesc.GetUserDefinedTypeDescID(t)
			if err != nil {
				return err
			}
			name, desc, err := dt.GetTypeDescriptor(ctx, id)
			if err != nil {
				return err
			}
			if err := desc.HydrateTypeInfoWithName(ctx, t, &name, dt); err != nil {
				return err
			}
		}
	}
	return nil
}
