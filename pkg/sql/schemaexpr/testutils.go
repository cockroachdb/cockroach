// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
) sqlbase.MutableTableDescriptor {
	cols := make([]sqlbase.ColumnDescriptor, len(columns))
	for i := range columns {
		cols[i] = sqlbase.ColumnDescriptor{
			Name: columns[i].name,
			Type: columns[i].typ,
			ID:   sqlbase.ColumnID(i),
		}
	}

	muts := make([]sqlbase.DescriptorMutation, len(mutationColumns))
	for i := range mutationColumns {
		muts[i] = sqlbase.DescriptorMutation{
			Descriptor_: &sqlbase.DescriptorMutation_Column{
				Column: &sqlbase.ColumnDescriptor{
					Name: mutationColumns[i].name,
					Type: mutationColumns[i].typ,
					ID:   sqlbase.ColumnID(len(columns) + i),
				},
			},
			Direction: sqlbase.DescriptorMutation_ADD,
		}
	}

	return sqlbase.MutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			Name:      name,
			ID:        1,
			Columns:   cols,
			Mutations: muts,
		},
	}
}
