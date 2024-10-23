// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package crosscluster

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// CrossClusterTypeResolver is meant to be used to resolve types using type
// descriptors that originate from a different cluster.
type CrossClusterTypeResolver struct {
	typeIDToDesc   map[descpb.ID]*descpb.TypeDescriptor
	typeNameToDesc map[string][]*descpb.TypeDescriptor
}

var _ tree.TypeReferenceResolver = CrossClusterTypeResolver{}
var _ catalog.TypeDescriptorResolver = CrossClusterTypeResolver{}

func MakeCrossClusterTypeResolver(typeDescs []*descpb.TypeDescriptor) CrossClusterTypeResolver {
	itr := CrossClusterTypeResolver{
		typeIDToDesc:   make(map[descpb.ID]*descpb.TypeDescriptor),
		typeNameToDesc: make(map[string][]*descpb.TypeDescriptor),
	}
	for _, typeDesc := range typeDescs {
		itr.typeIDToDesc[typeDesc.GetID()] = typeDesc
		name := typeDesc.GetName()
		itr.typeNameToDesc[name] = append(itr.typeNameToDesc[name], typeDesc)
	}
	return itr
}

// ResolveType implements the tree.TypeReferenceResolver interface.
//
// We currently have an incomplete implementation of this method - namely, it
// works whenever typeDescs are provided in makeImportTypeResolver (which is the
// case when we're importing into exactly one table). In such a case, the type
// resolution can be simplified to only look up into the provided types based on
// the type's name (meaning we can avoid resolving the db and the schema names).
//
// Note that if a table happens to have multiple types with the same name (but
// different schemas), this implementation will return a "feature unsupported"
// error.
func (i CrossClusterTypeResolver) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	var descs []*descpb.TypeDescriptor
	var ok bool
	if descs, ok = i.typeNameToDesc[name.Parts[0]]; !ok || len(descs) == 0 {
		return nil, sqlerrors.NewUndefinedTypeError(name)
	}
	if len(descs) > 1 {
		return nil, pgerror.New(
			pgcode.FeatureNotSupported,
			"tables with multiple user-defined types with the same name are currently unsupported",
		)
	}
	typeDesc := typedesc.NewBuilder(descs[0]).BuildImmutableType()
	t := typeDesc.AsTypesT()
	if err := typedesc.EnsureTypeIsHydrated(ctx, t, i); err != nil {
		return nil, err
	}
	return t, nil
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
func (i CrossClusterTypeResolver) ResolveTypeByOID(
	ctx context.Context, oid oid.Oid,
) (*types.T, error) {
	return typedesc.ResolveHydratedTByOID(ctx, oid, i)
}

// GetTypeDescriptor implements the catalog.TypeDescriptorResolver interface.
func (i CrossClusterTypeResolver) GetTypeDescriptor(
	_ context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	var desc *descpb.TypeDescriptor
	var ok bool
	if desc, ok = i.typeIDToDesc[id]; !ok {
		return tree.TypeName{}, nil, errors.Newf("type descriptor could not be resolved for type id %d", id)
	}
	typeDesc := typedesc.NewBuilder(desc).BuildImmutableType()
	name := tree.MakeUnqualifiedTypeName(desc.GetName())
	return name, typeDesc, nil
}
