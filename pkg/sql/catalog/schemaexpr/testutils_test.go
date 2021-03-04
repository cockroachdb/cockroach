// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr_test

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// testCol includes the information needed to create a column descriptor for
// testing purposes.
type testCol struct {
	name string
	typ  *types.T
}

// testTableDesc is a helper functions for creating table descriptors in a
// less verbose way.
func testTableDesc(
	name string, columns []testCol, mutationColumns []testCol,
) catalog.TableDescriptor {
	cols := make([]descpb.ColumnDescriptor, len(columns))
	for i := range columns {
		cols[i] = descpb.ColumnDescriptor{
			Name: columns[i].name,
			Type: columns[i].typ,
			// Column IDs start at 1 to mimic "real" table descriptors.
			ID: descpb.ColumnID(i + 1),
		}
	}

	muts := make([]descpb.DescriptorMutation, len(mutationColumns))
	for i := range mutationColumns {
		muts[i] = descpb.DescriptorMutation{
			Descriptor_: &descpb.DescriptorMutation_Column{
				Column: &descpb.ColumnDescriptor{
					Name: mutationColumns[i].name,
					Type: mutationColumns[i].typ,
					ID:   descpb.ColumnID(len(columns) + i + 1),
				},
			},
			Direction: descpb.DescriptorMutation_ADD,
		}
	}
	return tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name:      name,
		ID:        1,
		Columns:   cols,
		Mutations: muts,
	}).BuildImmutableTable()
}
