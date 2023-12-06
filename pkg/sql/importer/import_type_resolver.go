// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type importTypeResolver struct {
	typeIDToDesc   map[descpb.ID]*descpb.TypeDescriptor
	typeNameToDesc map[string]*descpb.TypeDescriptor
}

var _ tree.TypeReferenceResolver = importTypeResolver{}
var _ catalog.TypeDescriptorResolver = importTypeResolver{}

func makeImportTypeResolver(typeDescs []*descpb.TypeDescriptor) importTypeResolver {
	itr := importTypeResolver{
		typeIDToDesc:   make(map[descpb.ID]*descpb.TypeDescriptor),
		typeNameToDesc: make(map[string]*descpb.TypeDescriptor),
	}
	for _, typeDesc := range typeDescs {
		itr.typeIDToDesc[typeDesc.GetID()] = typeDesc
		itr.typeNameToDesc[typeDesc.GetName()] = typeDesc
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
func (i importTypeResolver) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	var desc *descpb.TypeDescriptor
	var ok bool
	if desc, ok = i.typeNameToDesc[name.Parts[0]]; !ok {
		return nil, errors.Newf("type descriptor could not be resolved for type name %s", name.Parts[0])
	}
	typeDesc := typedesc.NewBuilder(desc).BuildImmutableType()
	t := typeDesc.AsTypesT()
	if err := typedesc.EnsureTypeIsHydrated(ctx, t, i); err != nil {
		return nil, err
	}
	return t, nil
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
func (i importTypeResolver) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	return typedesc.ResolveHydratedTByOID(ctx, oid, i)
}

// GetTypeDescriptor implements the catalog.TypeDescriptorResolver interface.
func (i importTypeResolver) GetTypeDescriptor(
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
