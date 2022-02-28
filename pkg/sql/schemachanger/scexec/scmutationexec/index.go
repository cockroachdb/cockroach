// Copyright 2022 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

func (m *visitor) MakeAddedIndexDeleteOnly(
	ctx context.Context, op scop.MakeAddedIndexDeleteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.Index.TableID)
	if err != nil {
		return err
	}
	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.Index.IndexID >= tbl.NextIndexID {
		tbl.NextIndexID = op.Index.IndexID + 1
	}
	// Resolve column names
	colNames, err := columnNamesFromIDs(tbl, op.Index.KeyColumnIDs)
	if err != nil {
		return err
	}
	storeColNames, err := columnNamesFromIDs(tbl, op.Index.StoringColumnIDs)
	if err != nil {
		return err
	}
	colDirs := make([]descpb.IndexDescriptor_Direction, len(op.Index.KeyColumnIDs))
	for i, dir := range op.Index.KeyColumnDirections {
		if dir == scpb.Index_DESC {
			colDirs[i] = descpb.IndexDescriptor_DESC
		}
	}
	// Set up the index descriptor type.
	indexType := descpb.IndexDescriptor_FORWARD
	if op.Index.IsInverted {
		indexType = descpb.IndexDescriptor_INVERTED
	}
	// Set up the encoding type.
	encodingType := descpb.PrimaryIndexEncoding
	indexVersion := descpb.PrimaryIndexWithStoredColumnsVersion
	if op.IsSecondaryIndex {
		encodingType = descpb.SecondaryIndexEncoding
	}
	// Create an index descriptor from the operation.
	idx := &descpb.IndexDescriptor{
		ID:                          op.Index.IndexID,
		Name:                        tabledesc.IndexNamePlaceholder(op.Index.IndexID),
		Unique:                      op.Index.IsUnique,
		Version:                     indexVersion,
		KeyColumnNames:              colNames,
		KeyColumnIDs:                op.Index.KeyColumnIDs,
		StoreColumnIDs:              op.Index.StoringColumnIDs,
		StoreColumnNames:            storeColNames,
		KeyColumnDirections:         colDirs,
		Type:                        indexType,
		KeySuffixColumnIDs:          op.Index.KeySuffixColumnIDs,
		CompositeColumnIDs:          op.Index.CompositeColumnIDs,
		CreatedExplicitly:           true,
		EncodingType:                encodingType,
		ConstraintID:                tbl.GetNextConstraintID(),
		UseDeletePreservingEncoding: op.IsDeletePreserving,
	}
	if op.Index.Sharding != nil {
		idx.Sharded = *op.Index.Sharding
	}
	tbl.NextConstraintID++
	return enqueueAddIndexMutation(tbl, idx)
}

func (m *visitor) SetAddedIndexPartialPredicate(
	ctx context.Context, op scop.SetAddedIndexPartialPredicate,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := FindMutation(tbl, MakeIndexIDMutationSelector(op.IndexID))
	if err != nil {
		return err
	}
	idx := mut.AsIndex().IndexDesc()
	idx.Predicate = string(op.Expr)
	return nil
}

func (m *visitor) MakeAddedIndexDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeAddedIndexDeleteAndWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
}

func (m *visitor) MakeAddedPrimaryIndexPublic(
	ctx context.Context, op scop.MakeAddedPrimaryIndexPublic,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	index, err := tbl.FindIndexWithID(op.IndexID)
	if err != nil {
		return err
	}
	indexDesc := index.IndexDescDeepCopy()
	if _, err := removeMutation(
		tbl,
		MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	); err != nil {
		return err
	}
	tbl.PrimaryIndex = indexDesc
	return nil
}

func (m *visitor) MakeAddedSecondaryIndexPublic(
	ctx context.Context, op scop.MakeAddedSecondaryIndexPublic,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}

	for idx, idxMutation := range tbl.GetMutations() {
		if idxMutation.GetIndex() != nil &&
			idxMutation.GetIndex().ID == op.IndexID {
			err := tbl.MakeMutationComplete(idxMutation)
			if err != nil {
				return err
			}
			tbl.Mutations = append(tbl.Mutations[:idx], tbl.Mutations[idx+1:]...)
			break
		}
	}
	if len(tbl.Mutations) == 0 {
		tbl.Mutations = nil
	}
	return nil
}

func (m *visitor) MakeDroppedPrimaryIndexDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeDroppedPrimaryIndexDeleteAndWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	if tbl.GetPrimaryIndexID() != op.IndexID {
		return errors.AssertionFailedf("index being dropped (%d) does not match existing primary index (%d).", op.IndexID, tbl.PrimaryIndex.ID)
	}
	desc := tbl.GetPrimaryIndex().IndexDescDeepCopy()
	return enqueueDropIndexMutation(tbl, &desc)
}

func (m *visitor) MakeDroppedNonPrimaryIndexDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeDroppedNonPrimaryIndexDeleteAndWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	for i, idx := range tbl.PublicNonPrimaryIndexes() {
		if idx.GetID() == op.IndexID {
			desc := idx.IndexDescDeepCopy()
			tbl.Indexes = append(tbl.Indexes[:i], tbl.Indexes[i+1:]...)
			return enqueueDropIndexMutation(tbl, &desc)
		}
	}
	return errors.AssertionFailedf("failed to find secondary index %d in descriptor %v", op.IndexID, tbl)
}

func (m *visitor) MakeDroppedIndexDeleteOnly(
	ctx context.Context, op scop.MakeDroppedIndexDeleteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		descpb.DescriptorMutation_DELETE_ONLY,
	)
}

func (m *visitor) RemoveDroppedIndexPartialPredicate(
	ctx context.Context, op scop.RemoveDroppedIndexPartialPredicate,
) error {
	if desc, err := m.s.GetDescriptor(ctx, op.TableID); err != nil || desc.Dropped() {
		return err
	}
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := FindMutation(tbl, MakeIndexIDMutationSelector(op.IndexID))
	if err != nil {
		return err
	}
	idx := mut.AsIndex().IndexDesc()
	idx.Predicate = ""
	return nil
}

func (m *visitor) MakeIndexAbsent(ctx context.Context, op scop.MakeIndexAbsent) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	_, err = removeMutation(
		tbl,
		MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_DELETE_ONLY,
	)
	return err
}

func (m *visitor) AddIndexPartitionInfo(ctx context.Context, op scop.AddIndexPartitionInfo) error {
	tbl, err := m.checkOutTable(ctx, op.Partitioning.TableID)
	if err != nil {
		return err
	}
	index, err := tbl.FindIndexWithID(op.Partitioning.IndexID)
	if err != nil {
		return err
	}
	index.IndexDesc().Partitioning = op.Partitioning.PartitioningDescriptor
	return nil
}

func (m *visitor) SetIndexName(ctx context.Context, op scop.SetIndexName) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	index, err := tbl.FindIndexWithID(op.IndexID)
	if err != nil {
		return err
	}
	index.IndexDesc().Name = op.Name
	return nil
}
