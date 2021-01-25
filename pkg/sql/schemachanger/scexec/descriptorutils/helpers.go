// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descriptorutils

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// GetIndexMutation returns a reference to a specified index add/drop mutation
// on a table.
func GetIndexMutation(
	table catalog.TableDescriptor, idxID descpb.IndexID,
) (mut *descpb.DescriptorMutation, sliceIdx int, err error) {
	mutations := table.TableDesc().Mutations
	for i := range mutations {
		mut := &mutations[i]
		idx := mut.GetIndex()
		if idx != nil && idx.ID == idxID {
			return mut, i, nil
		}
	}
	return nil, 0, errors.AssertionFailedf("mutation not found")
}

// GetColumnMutation returns a reference to a specified column add/drop mutation
// on a table.
func GetColumnMutation(
	table catalog.TableDescriptor, colID descpb.ColumnID,
) (mut *descpb.DescriptorMutation, sliceIdx int, err error) {
	mutations := table.TableDesc().Mutations
	for i := range mutations {
		mut := &mutations[i]
		col := mut.GetColumn()
		if col != nil && col.ID == colID {
			return mut, i, nil
		}
	}
	return nil, 0, errors.AssertionFailedf("mutation not found")
}
