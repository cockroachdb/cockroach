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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func (m *visitor) MakeAbsentIndexBackfilling(
	ctx context.Context, op scop.MakeAbsentIndexBackfilling,
) error {
	return addNewIndexMutation(
		ctx, m, op.Index, op.IsSecondaryIndex, op.IsDeletePreserving,
		descpb.DescriptorMutation_BACKFILLING,
	)
}

func (m *visitor) MakeAbsentTempIndexDeleteOnly(
	ctx context.Context, op scop.MakeAbsentTempIndexDeleteOnly,
) error {
	const isDeletePreserving = true // temp indexes are always delete preserving
	return addNewIndexMutation(
		ctx, m, op.Index, op.IsSecondaryIndex, isDeletePreserving,
		descpb.DescriptorMutation_DELETE_ONLY,
	)
}

func addNewIndexMutation(
	ctx context.Context,
	m *visitor,
	opIndex scpb.Index,
	isSecondary bool,
	isDeletePreserving bool,
	state descpb.DescriptorMutation_State,
) error {
	tbl, err := m.checkOutTable(ctx, opIndex.TableID)
	if err != nil {
		return err
	}
	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if opIndex.IndexID >= tbl.NextIndexID {
		tbl.NextIndexID = opIndex.IndexID + 1
	}
	if opIndex.ConstraintID >= tbl.NextConstraintID {
		tbl.NextConstraintID = opIndex.ConstraintID + 1
	}

	// Set up the index descriptor type.
	indexType := descpb.IndexDescriptor_FORWARD
	if opIndex.IsInverted {
		indexType = descpb.IndexDescriptor_INVERTED
	}
	// Set up the encoding type.
	encodingType := catenumpb.PrimaryIndexEncoding
	indexVersion := descpb.LatestIndexDescriptorVersion
	if isSecondary {
		encodingType = catenumpb.SecondaryIndexEncoding
	}
	// Create an index descriptor from the operation.
	idx := &descpb.IndexDescriptor{
		ID:                          opIndex.IndexID,
		Name:                        tabledesc.IndexNamePlaceholder(opIndex.IndexID),
		Unique:                      opIndex.IsUnique,
		NotVisible:                  opIndex.IsNotVisible,
		Version:                     indexVersion,
		Type:                        indexType,
		CreatedExplicitly:           true,
		EncodingType:                encodingType,
		ConstraintID:                opIndex.ConstraintID,
		UseDeletePreservingEncoding: isDeletePreserving,
		StoreColumnNames:            []string{},
	}
	if isSecondary && !isDeletePreserving {
		idx.CreatedAtNanos = m.clock.ApproximateTime().UnixNano()
	}
	if opIndex.Sharding != nil {
		idx.Sharded = *opIndex.Sharding
	}
	if opIndex.GeoConfig != nil {
		idx.GeoConfig = *opIndex.GeoConfig
	}
	return enqueueAddIndexMutation(tbl, idx, state)
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

func (m *visitor) MakeBackfillingIndexDeleteOnly(
	ctx context.Context, op scop.MakeBackfillingIndexDeleteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_BACKFILLING,
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_ADD,
	)
}

func (m *visitor) MakeDeleteOnlyIndexWriteOnly(
	ctx context.Context, op scop.MakeDeleteOnlyIndexWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_WRITE_ONLY,
		descpb.DescriptorMutation_ADD,
	)
}

func (m *visitor) MakeBackfilledIndexMerging(
	ctx context.Context, op scop.MakeBackfilledIndexMerging,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_MERGING,
		descpb.DescriptorMutation_ADD,
	)
}

func (m *visitor) MakeMergedIndexWriteOnly(
	ctx context.Context, op scop.MakeMergedIndexWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_MERGING,
		descpb.DescriptorMutation_WRITE_ONLY,
		descpb.DescriptorMutation_ADD,
	)
}

func (m *visitor) MakeValidatedPrimaryIndexPublic(
	ctx context.Context, op scop.MakeValidatedPrimaryIndexPublic,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	index, err := catalog.MustFindIndexByID(tbl, op.IndexID)
	if err != nil {
		return err
	}
	indexDesc := index.IndexDescDeepCopy()
	if _, err := m.removeMutation(tbl, MakeIndexIDMutationSelector(op.IndexID), op.TargetMetadata, eventpb.CommonSQLEventDetails{
		DescriptorID:    uint32(tbl.GetID()),
		Statement:       redact.RedactableString(op.Statement),
		Tag:             op.StatementTag,
		ApplicationName: op.Authorization.AppName,
		User:            op.Authorization.UserName,
	}, descpb.DescriptorMutation_WRITE_ONLY); err != nil {
		return err
	}
	tbl.PrimaryIndex = indexDesc
	return nil
}

func (m *visitor) MakeValidatedSecondaryIndexPublic(
	ctx context.Context, op scop.MakeValidatedSecondaryIndexPublic,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}

	for idx, idxMutation := range tbl.GetMutations() {
		if idxMutation.GetIndex() != nil &&
			idxMutation.GetIndex().ID == op.IndexID {
			// If this is a rollback of a drop, we are trying to add the index back,
			// so swap the direction before making it complete.
			idxMutation.Direction = descpb.DescriptorMutation_ADD
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

func (m *visitor) MakePublicPrimaryIndexWriteOnly(
	ctx context.Context, op scop.MakePublicPrimaryIndexWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	if tbl.GetPrimaryIndexID() != op.IndexID {
		return errors.AssertionFailedf("index being dropped (%d) does not match existing primary index (%d).", op.IndexID, tbl.PrimaryIndex.ID)
	}
	desc := tbl.GetPrimaryIndex().IndexDescDeepCopy()
	tbl.TableDesc().PrimaryIndex = descpb.IndexDescriptor{} // zero-out the current primary index
	return enqueueDropIndexMutation(tbl, &desc)
}

func (m *visitor) MakePublicSecondaryIndexWriteOnly(
	ctx context.Context, op scop.MakePublicSecondaryIndexWriteOnly,
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

func (m *visitor) MakeWriteOnlyIndexDeleteOnly(
	ctx context.Context, op scop.MakeWriteOnlyIndexDeleteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	idx, err := catalog.MustFindIndexByID(tbl, op.IndexID)
	if err != nil {
		return err
	}
	// It's okay if the index is in MERGING.
	exp := descpb.DescriptorMutation_WRITE_ONLY
	if idx.Merging() {
		exp = descpb.DescriptorMutation_MERGING
	}
	return mutationStateChange(
		tbl,
		MakeIndexIDMutationSelector(op.IndexID),
		exp, descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_DROP,
	)
}

func (m *visitor) RemoveDroppedIndexPartialPredicate(
	ctx context.Context, op scop.RemoveDroppedIndexPartialPredicate,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
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
	_, err = m.removeMutation(tbl, MakeIndexIDMutationSelector(op.IndexID), op.TargetMetadata, eventpb.CommonSQLEventDetails{
		DescriptorID:    uint32(tbl.GetID()),
		Statement:       redact.RedactableString(op.Statement),
		Tag:             op.StatementTag,
		ApplicationName: op.Authorization.AppName,
		User:            op.Authorization.UserName,
	}, descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_BACKFILLING)
	return err
}

func (m *visitor) AddIndexPartitionInfo(ctx context.Context, op scop.AddIndexPartitionInfo) error {
	tbl, err := m.checkOutTable(ctx, op.Partitioning.TableID)
	if err != nil {
		return err
	}
	index, err := catalog.MustFindIndexByID(tbl, op.Partitioning.IndexID)
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
	index, err := catalog.MustFindIndexByID(tbl, op.IndexID)
	if err != nil {
		return err
	}
	index.IndexDesc().Name = op.Name
	return nil
}

func (m *visitor) AddColumnToIndex(ctx context.Context, op scop.AddColumnToIndex) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	index, err := catalog.MustFindIndexByID(tbl, op.IndexID)
	if err != nil {
		return err
	}
	column, err := catalog.MustFindColumnByID(tbl, op.ColumnID)
	if err != nil {
		return err
	}
	// Deal with the fact that we allow the columns to be unordered in how
	// we add them. We could add a rule to make sure we add the columns in
	// order, but we'd need a way to express successor or something in rel
	// rules to add those dependencies efficiently. Instead, we just don't
	// and sort here.
	indexDesc := index.IndexDesc()
	n := int(op.Ordinal + 1)
	insertIntoNames := func(s *[]string) {
		for delta := n - len(*s); delta > 0; delta-- {
			*s = append(*s, "")
		}
		(*s)[n-1] = column.GetName()
	}
	insertIntoDirections := func(s *[]catenumpb.IndexColumn_Direction) {
		for delta := n - len(*s); delta > 0; delta-- {
			*s = append(*s, 0)
		}
		(*s)[n-1] = op.Direction
	}
	insertIntoIDs := func(s *[]descpb.ColumnID) {
		for delta := n - len(*s); delta > 0; delta-- {
			*s = append(*s, 0)
		}
		(*s)[n-1] = column.GetID()
	}
	switch op.Kind {
	case scpb.IndexColumn_KEY:
		insertIntoIDs(&indexDesc.KeyColumnIDs)
		insertIntoNames(&indexDesc.KeyColumnNames)
		insertIntoDirections(&indexDesc.KeyColumnDirections)
	case scpb.IndexColumn_KEY_SUFFIX:
		insertIntoIDs(&indexDesc.KeySuffixColumnIDs)
	case scpb.IndexColumn_STORED:
		insertIntoIDs(&indexDesc.StoreColumnIDs)
		insertIntoNames(&indexDesc.StoreColumnNames)
	}
	// If this is a composite column, note that.
	if colinfo.CanHaveCompositeKeyEncoding(column.GetType()) &&
		// We don't need to track the composite column IDs for stored columns.
		op.Kind != scpb.IndexColumn_STORED {

		index.NumKeyColumns()
		var colOrdMap catalog.TableColMap
		for i := 0; i < index.NumKeyColumns(); i++ {
			colOrdMap.Set(index.GetKeyColumnID(i), i)
		}
		for i := 0; i < index.NumKeySuffixColumns(); i++ {
			colOrdMap.Set(index.GetKeyColumnID(i), i+index.NumKeyColumns())
		}
		indexDesc.CompositeColumnIDs = append(indexDesc.CompositeColumnIDs, column.GetID())
		cids := indexDesc.CompositeColumnIDs
		sort.Slice(cids, func(i, j int) bool {
			return colOrdMap.GetDefault(cids[i]) < colOrdMap.GetDefault(cids[j])
		})
	}
	return nil
}

func (m *visitor) RemoveColumnFromIndex(ctx context.Context, op scop.RemoveColumnFromIndex) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	index, err := catalog.MustFindIndexByID(tbl, op.IndexID)
	if err != nil {
		return err
	}
	column, err := catalog.MustFindColumnByID(tbl, op.ColumnID)
	if err != nil {
		return err
	}
	// Deal with the fact that we allow the columns to be unordered in how
	// we add them. We could add a rule to make sure we add the columns in
	// order, but we'd need a way to express successor or something in rel
	// rules to add those dependencies efficiently. Instead, we just don't
	// and sort here.
	idx := index.IndexDesc()
	n := int(op.Ordinal + 1)
	removeFromNames := func(s *[]string) {
		(*s)[n-1] = ""
	}
	removeFromColumnIDs := func(s *[]descpb.ColumnID) {
		(*s)[n-1] = 0
	}
	switch op.Kind {
	case scpb.IndexColumn_KEY:
		removeFromNames(&idx.KeyColumnNames)
		removeFromColumnIDs(&idx.KeyColumnIDs)
		for i := len(idx.KeyColumnIDs) - 1; i >= 0 && idx.KeyColumnIDs[i] == 0; i-- {
			idx.KeyColumnNames = idx.KeyColumnNames[:i]
			idx.KeyColumnIDs = idx.KeyColumnIDs[:i]
			idx.KeyColumnDirections = idx.KeyColumnDirections[:i]
			if idx.Type == descpb.IndexDescriptor_INVERTED && i == len(idx.KeyColumnIDs)-1 {
				idx.InvertedColumnKinds = nil
			}
		}
	case scpb.IndexColumn_KEY_SUFFIX:
		removeFromColumnIDs(&idx.KeySuffixColumnIDs)
		for i := len(idx.KeySuffixColumnIDs) - 1; i >= 0 && idx.KeySuffixColumnIDs[i] == 0; i-- {
			idx.KeySuffixColumnIDs = idx.KeySuffixColumnIDs[:i]
		}
	case scpb.IndexColumn_STORED:
		removeFromNames(&idx.StoreColumnNames)
		removeFromColumnIDs(&idx.StoreColumnIDs)
		for i := len(idx.StoreColumnIDs) - 1; i >= 0 && idx.StoreColumnIDs[i] == 0; i-- {
			idx.StoreColumnNames = idx.StoreColumnNames[:i]
			idx.StoreColumnIDs = idx.StoreColumnIDs[:i]
		}
	}
	// If this is a composite column, remove it from the list.
	if colinfo.CanHaveCompositeKeyEncoding(column.GetType()) &&
		// We don't need to track the composite column IDs for stored columns.
		op.Kind != scpb.IndexColumn_STORED {
		for i, colID := range idx.CompositeColumnIDs {
			if colID == column.GetID() {
				idx.CompositeColumnIDs = append(
					idx.CompositeColumnIDs[:i], idx.CompositeColumnIDs[i+1:]...,
				)
			}
		}
	}
	return nil
}

func (m *visitor) MaybeAddSplitForIndex(_ context.Context, op scop.MaybeAddSplitForIndex) error {
	m.s.AddIndexForMaybeSplitAndScatter(op.TableID, op.IndexID)
	return nil
}
