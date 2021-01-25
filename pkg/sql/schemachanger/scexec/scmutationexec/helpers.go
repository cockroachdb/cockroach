// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/descriptorutils"
	"github.com/cockroachdb/errors"
)

type mutationSelector func(descriptor catalog.TableDescriptor) (mut *descpb.DescriptorMutation, sliceIdx int, err error)

func mutationStateChange(
	ctx context.Context,
	table *tabledesc.Mutable,
	f mutationSelector,
	exp, next descpb.DescriptorMutation_State,
) error {
	mut, _, err := f(table)
	if err != nil {
		return err
	}
	if mut.State != exp {
		return errors.AssertionFailedf("update mutation for %d from %v to %v: unexpected state: %v",
			table.GetID(), exp, mut.State, table)
	}
	mut.State = next
	return nil
}

func removeMutation(
	ctx context.Context,
	table *tabledesc.Mutable,
	f mutationSelector,
	exp descpb.DescriptorMutation_State,
) (descpb.DescriptorMutation, error) {
	mut, foundIdx, err := f(table)
	if err != nil {
		return descpb.DescriptorMutation{}, err
	}
	cpy := *mut
	if mut.State != exp {
		return descpb.DescriptorMutation{}, errors.AssertionFailedf(
			"remove mutation from %d: unexpected state: got %v, expected %v: %v",
			table.GetID(), mut.State, exp, table,
		)
	}
	table.Mutations = append(table.Mutations[:foundIdx], table.Mutations[foundIdx+1:]...)
	return cpy, nil
}

func getIndexMutation(
	idxID descpb.IndexID,
) func(table catalog.TableDescriptor) (mut *descpb.DescriptorMutation, sliceIdx int, err error) {
	return func(table catalog.TableDescriptor) (mut *descpb.DescriptorMutation, sliceIdx int, err error) {
		return descriptorutils.GetIndexMutation(table, idxID)
	}
}

func getColumnMutation(
	colID descpb.ColumnID,
) func(table catalog.TableDescriptor) (mut *descpb.DescriptorMutation, sliceIdx int, err error) {
	return func(table catalog.TableDescriptor) (mut *descpb.DescriptorMutation, sliceIdx int, err error) {
		return descriptorutils.GetColumnMutation(table, colID)
	}
}

// findFamilyOrdinalForColumnID finds a family which contains the needle column
// id and returns its index in the families slice.
func findFamilyOrdinalForColumnID(
	table catalog.TableDescriptor, needle descpb.ColumnID,
) (int, error) {
	families := table.GetFamilies()
	for i := range families {
		for _, colID := range families[i].ColumnIDs {
			if colID == needle {
				return i, nil
			}
		}
	}
	return -1, errors.Errorf("failed to find column family for column %d in table %d: %v",
		needle, table.GetID(), table)
}

// Suppress the linter.
var _ = findFamilyOrdinalForColumnID

func removeColumnFromFamily(table *tabledesc.Mutable, colID descpb.ColumnID) error {
	famIdx, err := findFamilyOrdinalForColumnID(table, colID)
	if err != nil {
		return errors.WithAssertionFailure(err)
	}
	f := &table.Families[famIdx]
	for i, id := range f.ColumnIDs {
		if id == colID {
			f.ColumnIDs = append(f.ColumnIDs[:i], f.ColumnIDs[i+1:]...)
			f.ColumnNames = append(f.ColumnNames[:i], f.ColumnNames[i+1:]...)
			break
		}
	}
	if len(f.ColumnIDs) == 0 {
		table.Families = append(table.Families[:famIdx], table.Families[famIdx+1:]...)
	}
	return nil
}

// Suppress the linter.
var _ = removeColumnFromFamily
