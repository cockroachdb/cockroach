// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testcat

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ tree.TypeReferenceResolver = (*Catalog)(nil)

// CreateType handles the CREATE TYPE statement.
func (tc *Catalog) CreateType(c *tree.CreateType) {
	if c.Variety != tree.Enum {
		panic("only enum types can be created")
	}
	typOid := catid.TypeIDToOID(catid.DescID(tc.nextStableID()))
	arrayOid := catid.TypeIDToOID(catid.DescID(tc.nextStableID()))
	typ := types.MakeEnum(typOid, arrayOid)

	// We don't handle fully qualified names.
	typ.TypeMeta = types.UserDefinedTypeMetadata{
		Name: &types.UserDefinedTypeName{
			Name: c.TypeName.Object(),
		},
		Version: 1,
		EnumData: &types.EnumMetadata{
			PhysicalRepresentations: enum.GenerateNEvenlySpacedBytes(len(c.EnumLabels)),
			LogicalRepresentations:  make([]string, len(c.EnumLabels)),
			IsMemberReadOnly:        make([]bool, len(c.EnumLabels)),
		},
	}
	for i := range c.EnumLabels {
		typ.TypeMeta.EnumData.LogicalRepresentations[i] = string(c.EnumLabels[i])
	}
	if tc.enumTypes == nil {
		tc.enumTypes = make(map[string]*types.T)
	}
	tc.enumTypes[c.TypeName.Object()] = typ
}

// Look for a matching implicit record type, skipping tables that don't match
// the filter functor's criteria.
func (tc *Catalog) resolveTypeImplicit(skipme func(tab *Table) bool) *types.T {
	for _, ds := range tc.testSchema.dataSources {
		if tab, ok := ds.(*Table); ok {
			if skipme(tab) {
				continue
			}
			contents := make([]*types.T, 0, tab.ColumnCount())
			labels := make([]string, 0, tab.ColumnCount())
			for i, n := 0, tab.ColumnCount(); i < n; i++ {
				col := tab.Column(i)
				if col.Kind() == cat.Ordinary && col.Visibility() == cat.Visible {
					contents = append(contents, col.DatumType())
					labels = append(labels, string(col.ColName()))
				}
			}
			return types.MakeLabeledTuple(contents, labels)
		}
	}

	return nil
}

// ResolveType is part of the cat.Catalog interface and the
// tree.TypeReferenceResolver interface.
func (tc *Catalog) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	// First look for a matching user-defined enum type.
	if typ := tc.enumTypes[name.Object()]; typ != nil {
		return typ, nil
	}

	// Otherwise look for a matching implicit record type.
	typ := tc.resolveTypeImplicit(func(tab *Table) bool {
		return tab.TabName.Object() != name.Object()
	})
	if typ == nil {
		return nil, errors.Newf("type %q does not exist", name)
	}
	return typ, nil
}

// ResolveTypeByOID is part of the cat.Catalog interface.
func (tc *Catalog) ResolveTypeByOID(ctx context.Context, typID oid.Oid) (*types.T, error) {
	// First look for a matching user-defined enum type.
	for _, typ := range tc.enumTypes {
		if typ.Oid() == typID {
			return typ, nil
		}
	}

	// Otherwise look for a matching implicit record type.
	typ := tc.resolveTypeImplicit(func(tab *Table) bool {
		return typedesc.TableIDToImplicitTypeOID(descpb.ID(tab.ID())) != typID
	})
	if typ == nil {
		return nil, errors.Newf("type %q does not exist", typID)
	}
	return typ, nil
}
