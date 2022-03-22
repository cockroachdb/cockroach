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

func newImportTypeResolver(typeDescs []*descpb.TypeDescriptor) importTypeResolver {
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

var _ tree.TypeReferenceResolver = &importTypeResolver{}

func (i importTypeResolver) ResolveType(
	_ context.Context, _ *tree.UnresolvedObjectName,
) (*types.T, error) {
	return nil, errors.New("importTypeResolver does not implement ResolveType")
}

func (i importTypeResolver) ResolveTypeByOID(ctx context.Context, oid oid.Oid) (*types.T, error) {
	id, err := typedesc.UserDefinedTypeOIDToID(oid)
	if err != nil {
		return nil, err
	}
	name, desc, err := i.GetTypeDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	return desc.MakeTypesT(ctx, &name, i)
}

var _ catalog.TypeDescriptorResolver = &importTypeResolver{}

// GetTypeDescriptor implements the sqlbase.TypeDescriptorResolver interface.
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
