// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testcat

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
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
	typOid := oid.Oid(oidext.CockroachPredefinedOIDMax + 1 + len(tc.enumTypes)*2)
	arrayOid := typOid + 1
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

// ResolveType part of the cat.Catalog interface and the
// tree.TypeReferenceResolver interface.
func (tc *Catalog) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	// First look for a matching user-defined enum type.
	if typ := tc.enumTypes[name.Object()]; typ != nil {
		return typ, nil
	}
	// Otherwise look for a matching implicit record type.
	for _, ds := range tc.testSchema.dataSources {
		if tab, ok := ds.(*Table); ok && tab.TabName.Object() == name.Object() {
			contents := make([]*types.T, 0, tab.ColumnCount())
			labels := make([]string, 0, tab.ColumnCount())
			for i, n := 0, tab.ColumnCount(); i < n; i++ {
				col := tab.Column(i)
				if col.Kind() == cat.Ordinary {
					contents = append(contents, col.DatumType())
					labels = append(labels, string(col.ColName()))
				}
			}
			return types.MakeLabeledTuple(contents, labels), nil
		}
	}
	return nil, errors.Newf("type %q does not exist", name)
}

// ResolveTypeByOID is part of the cat.Catalog interface.
func (tc *Catalog) ResolveTypeByOID(context.Context, oid.Oid) (*types.T, error) {
	return nil, errors.Newf("ResolveTypeByOID not supported in the test catalog")
}
