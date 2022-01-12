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
func (dt *DistSQLTypeResolver) ResolveType(
	context.Context, *tree.UnresolvedObjectName,
) (*types.T, error) {
	return nil, errors.AssertionFailedf("cannot resolve types in DistSQL by name")
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
func (dt *DistSQLTypeResolver) ResolveTypeByOID(
	ctx context.Context, oid oid.Oid,
) (*types.T, error) {
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
func (dt *DistSQLTypeResolver) GetTypeDescriptor(
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
	var typeDesc catalog.TypeDescriptor
	switch t := desc.(type) {
	case catalog.TypeDescriptor:
		// User-defined type.
		typeDesc = t
	case catalog.TableDescriptor:
		// If we find a table descriptor when we were expecting a type descriptor,
		// we return the implicitly-created type descriptor that is created for each
		// table. Make sure that we hydrate the table ahead of time, since we expect
		// that the table's types are fully hydrated below.
		t, err = dt.descriptors.hydrateTypesInTableDesc(ctx, dt.txn, t)
		if err != nil {
			return tree.TypeName{}, nil, err
		}
		typeDesc, err = typedesc.CreateImplicitRecordTypeFromTableDesc(t)
		if err != nil {
			return tree.TypeName{}, nil, err
		}
	default:
		return tree.TypeName{}, nil, pgerror.Newf(pgcode.WrongObjectType,
			"descriptor %d is a %s not a %s", id, desc.DescriptorType(), catalog.Type)
	}
	name := tree.MakeUnqualifiedTypeName(desc.GetName())
	return name, typeDesc, nil
}

// HydrateTypeSlice installs metadata into a slice of types.T's.
func (dt *DistSQLTypeResolver) HydrateTypeSlice(ctx context.Context, typs []*types.T) error {
	for _, t := range typs {
		if err := typedesc.EnsureTypeIsHydrated(ctx, t, dt); err != nil {
			return err
		}
	}
	return nil
}
