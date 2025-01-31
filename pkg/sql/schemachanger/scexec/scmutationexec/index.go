// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/errors"
)

func (i *immediateVisitor) MakeAbsentIndexBackfilling(
	ctx context.Context, op scop.MakeAbsentIndexBackfilling,
) error {
	return addNewIndexMutation(
		ctx, i, op.Index, op.IsSecondaryIndex, op.IsDeletePreserving,
		descpb.DescriptorMutation_BACKFILLING,
	)
}

func (i *immediateVisitor) MakeAbsentTempIndexDeleteOnly(
	ctx context.Context, op scop.MakeAbsentTempIndexDeleteOnly,
) error {
	const isDeletePreserving = true // temp indexes are always delete preserving
	return addNewIndexMutation(
		ctx, i, op.Index, op.IsSecondaryIndex, isDeletePreserving,
		descpb.DescriptorMutation_DELETE_ONLY,
	)
}

func addNewIndexMutation(
	ctx context.Context,
	i *immediateVisitor,
	opIndex scpb.Index,
	isSecondary bool,
	isDeletePreserving bool,
	state descpb.DescriptorMutation_State,
) error {
	tbl, err := i.checkOutTable(ctx, opIndex.TableID)
	if err != nil || tbl.Dropped() {
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
		Invisibility:                opIndex.Invisibility,
		Version:                     indexVersion,
		Type:                        opIndex.Type,
		CreatedExplicitly:           true,
		EncodingType:                encodingType,
		ConstraintID:                opIndex.ConstraintID,
		UseDeletePreservingEncoding: isDeletePreserving,
		StoreColumnNames:            []string{},
	}
	if isSecondary && !isDeletePreserving {
		idx.CreatedAtNanos = i.clock.ApproximateTime().UnixNano()
	}
	if opIndex.Sharding != nil {
		idx.Sharded = *opIndex.Sharding
	}
	if opIndex.GeoConfig != nil {
		idx.GeoConfig = *opIndex.GeoConfig
	}
	if opIndex.VecConfig != nil {
		idx.VecConfig = *opIndex.VecConfig
	}
	return enqueueIndexMutation(tbl, idx, state, descpb.DescriptorMutation_ADD)
}

func (i *immediateVisitor) SetAddedIndexPartialPredicate(
	ctx context.Context, op scop.SetAddedIndexPartialPredicate,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
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

func (i *immediateVisitor) MakeBackfillingIndexDeleteOnly(
	ctx context.Context, op scop.MakeBackfillingIndexDeleteOnly,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
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

func (i *immediateVisitor) MakeDeleteOnlyIndexWriteOnly(
	ctx context.Context, op scop.MakeDeleteOnlyIndexWriteOnly,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
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

func (i *immediateVisitor) MakeBackfilledIndexMerging(
	ctx context.Context, op scop.MakeBackfilledIndexMerging,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
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

func (i *immediateVisitor) MakeMergedIndexWriteOnly(
	ctx context.Context, op scop.MakeMergedIndexWriteOnly,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
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

func (i *immediateVisitor) MakeValidatedPrimaryIndexPublic(
	ctx context.Context, op scop.MakeValidatedPrimaryIndexPublic,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	index, err := catalog.MustFindIndexByID(tbl, op.IndexID)
	if err != nil {
		return err
	}
	tbl.PrimaryIndex = index.IndexDescDeepCopy()
	_, err = RemoveMutation(
		tbl,
		MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_WRITE_ONLY,
	)
	return err
}

func (i *immediateVisitor) MakeValidatedSecondaryIndexPublic(
	ctx context.Context, op scop.MakeValidatedSecondaryIndexPublic,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
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

func (i *immediateVisitor) MakePublicPrimaryIndexWriteOnly(
	ctx context.Context, op scop.MakePublicPrimaryIndexWriteOnly,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	if tbl.GetPrimaryIndexID() != op.IndexID {
		return errors.AssertionFailedf("index being dropped (%d) does not match existing primary index (%d).", op.IndexID, tbl.PrimaryIndex.ID)
	}
	desc := tbl.GetPrimaryIndex().IndexDescDeepCopy()
	tbl.TableDesc().PrimaryIndex = descpb.IndexDescriptor{} // zero-out the current primary index
	return enqueueIndexMutation(tbl, &desc, descpb.DescriptorMutation_WRITE_ONLY, descpb.DescriptorMutation_DROP)
}

func (i *immediateVisitor) MakePublicSecondaryIndexWriteOnly(
	ctx context.Context, op scop.MakePublicSecondaryIndexWriteOnly,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	for i, idx := range tbl.PublicNonPrimaryIndexes() {
		if idx.GetID() == op.IndexID {
			desc := idx.IndexDescDeepCopy()
			tbl.Indexes = append(tbl.Indexes[:i], tbl.Indexes[i+1:]...)
			return enqueueIndexMutation(tbl, &desc, descpb.DescriptorMutation_WRITE_ONLY, descpb.DescriptorMutation_DROP)
		}
	}
	return errors.AssertionFailedf("failed to find secondary index %d in descriptor %v", op.IndexID, tbl)
}

func (i *immediateVisitor) MakeWriteOnlyIndexDeleteOnly(
	ctx context.Context, op scop.MakeWriteOnlyIndexDeleteOnly,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
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

func (i *immediateVisitor) RemoveDroppedIndexPartialPredicate(
	ctx context.Context, op scop.RemoveDroppedIndexPartialPredicate,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
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

func (i *immediateVisitor) MakeIndexAbsent(ctx context.Context, op scop.MakeIndexAbsent) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	_, err = RemoveMutation(
		tbl,
		MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_BACKFILLING,
	)
	return err
}

func (i *immediateVisitor) AddIndexPartitionInfo(
	ctx context.Context, op scop.AddIndexPartitionInfo,
) error {
	tbl, err := i.checkOutTable(ctx, op.Partitioning.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	index, err := catalog.MustFindIndexByID(tbl, op.Partitioning.IndexID)
	if err != nil {
		return err
	}
	index.IndexDesc().Partitioning = op.Partitioning.PartitioningDescriptor
	return nil
}

func (i *immediateVisitor) SetIndexName(ctx context.Context, op scop.SetIndexName) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	index, err := catalog.MustFindIndexByID(tbl, op.IndexID)
	if err != nil {
		return err
	}
	index.IndexDesc().Name = op.Name
	return nil
}

func (i *immediateVisitor) AddColumnToIndex(ctx context.Context, op scop.AddColumnToIndex) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
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

		var colOrdMap catalog.TableColMap
		for i := 0; i < index.NumKeyColumns(); i++ {
			colOrdMap.Set(index.GetKeyColumnID(i), i)
		}
		for i := 0; i < index.NumKeySuffixColumns(); i++ {
			colOrdMap.Set(index.GetKeySuffixColumnID(i), i+index.NumKeyColumns())
		}
		indexDesc.CompositeColumnIDs = append(indexDesc.CompositeColumnIDs, column.GetID())
		cids := indexDesc.CompositeColumnIDs
		sort.Slice(cids, func(i, j int) bool {
			return colOrdMap.GetDefault(cids[i]) < colOrdMap.GetDefault(cids[j])
		})
	}
	// If this is an inverted column, note that.
	if indexDesc.Type == idxtype.INVERTED && op.ColumnID == indexDesc.InvertedColumnID() {
		indexDesc.InvertedColumnKinds = []catpb.InvertedIndexColumnKind{op.InvertedKind}
	}
	return nil
}

func (i *immediateVisitor) RemoveColumnFromIndex(
	ctx context.Context, op scop.RemoveColumnFromIndex,
) error {
	tbl, err := i.checkOutTable(ctx, op.TableID)
	if err != nil || tbl.Dropped() {
		return err
	}
	index, err := catalog.MustFindIndexByID(tbl, op.IndexID)
	if err != nil || index.Dropped() {
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
	switch op.Kind {
	case scpb.IndexColumn_KEY:
		if int(op.Ordinal) >= len(idx.KeyColumnNames) {
			return errors.AssertionFailedf("invalid ordinal %d for key columns %v",
				op.Ordinal, idx.KeyColumnNames)
		}
		idx.KeyColumnIDs[op.Ordinal] = 0
		idx.KeyColumnNames[op.Ordinal] = ""
		for i := len(idx.KeyColumnIDs) - 1; i >= 0 && idx.KeyColumnIDs[i] == 0; i-- {
			idx.KeyColumnNames = idx.KeyColumnNames[:i]
			idx.KeyColumnIDs = idx.KeyColumnIDs[:i]
			idx.KeyColumnDirections = idx.KeyColumnDirections[:i]
			if idx.Type == idxtype.INVERTED && i == len(idx.KeyColumnIDs)-1 {
				idx.InvertedColumnKinds = nil
			}
		}
	case scpb.IndexColumn_KEY_SUFFIX:
		if int(op.Ordinal) >= len(idx.KeySuffixColumnIDs) {
			return errors.AssertionFailedf("invalid ordinal %d for key suffix columns %v",
				op.Ordinal, idx.KeySuffixColumnIDs)
		}
		idx.KeySuffixColumnIDs[op.Ordinal] = 0
		for i := len(idx.KeySuffixColumnIDs) - 1; i >= 0 && idx.KeySuffixColumnIDs[i] == 0; i-- {
			idx.KeySuffixColumnIDs = idx.KeySuffixColumnIDs[:i]
		}
	case scpb.IndexColumn_STORED:
		if int(op.Ordinal) >= len(idx.StoreColumnNames) {
			return errors.AssertionFailedf("invalid ordinal %d for stored columns %v",
				op.Ordinal, idx.StoreColumnNames)
		}
		idx.StoreColumnIDs[op.Ordinal] = 0
		idx.StoreColumnNames[op.Ordinal] = ""
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

func (m *deferredVisitor) MaybeAddSplitForIndex(
	_ context.Context, op scop.MaybeAddSplitForIndex,
) error {
	m.AddIndexForMaybeSplitAndScatter(op.TableID, op.IndexID)
	return nil
}

func (i *immediateVisitor) AddIndexZoneConfig(_ context.Context, op scop.AddIndexZoneConfig) error {
	i.ImmediateMutationStateUpdater.UpdateSubzoneConfig(
		op.TableID, op.Subzone, op.SubzoneSpans, op.SubzoneIndexToDelete)
	return nil
}

func (i *immediateVisitor) AddPartitionZoneConfig(
	_ context.Context, op scop.AddPartitionZoneConfig,
) error {
	i.ImmediateMutationStateUpdater.UpdateSubzoneConfig(
		op.TableID, op.Subzone, op.SubzoneSpans, op.SubzoneIndexToDelete)
	return nil
}
