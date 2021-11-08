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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/descriptorutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// CatalogReader describes catalog read operations as required by the mutation
// visitor.
type CatalogReader interface {
	// MustReadImmutableDescriptor reads a descriptor from the catalog by ID.
	MustReadImmutableDescriptor(ctx context.Context, id descpb.ID) (catalog.Descriptor, error)

	// AddSyntheticDescriptor adds a synthetic descriptor to the reader state.
	// Subsequent calls to MustReadImmutableDescriptor for this ID will return
	// this synthetic descriptor instead of what it would have otherwise returned.
	AddSyntheticDescriptor(desc catalog.Descriptor)

	// RemoveSyntheticDescriptor undoes the effects of AddSyntheticDescriptor.
	RemoveSyntheticDescriptor(id descpb.ID)

	// AddPartitioning adds partitioning information on an index descriptor.
	AddPartitioning(
		tableDesc *tabledesc.Mutable,
		indexDesc *descpb.IndexDescriptor,
		partitionFields []string,
		listPartition []*scpb.ListPartition,
		rangePartition []*scpb.RangePartitions,
		allowedNewColumnNames []tree.Name,
		allowImplicitPartitioning bool,
	) error
}

// MutationVisitorStateUpdater is the interface for updating the visitor state.
type MutationVisitorStateUpdater interface {

	// CheckOutDescriptor reads a descriptor from the catalog by ID and marks it
	// as undergoing a change.
	CheckOutDescriptor(ctx context.Context, id descpb.ID) (catalog.MutableDescriptor, error)

	// AddDrainedName marks a namespace entry as being drained.
	AddDrainedName(id descpb.ID, nameInfo descpb.NameInfo)

	// AddNewGCJobForDescriptor enqueues a GC job for the given descriptor.
	AddNewGCJobForDescriptor(descriptor catalog.Descriptor)
}

// NewMutationVisitor creates a new scop.MutationVisitor.
func NewMutationVisitor(cr CatalogReader, s MutationVisitorStateUpdater) scop.MutationVisitor {
	return &visitor{
		cr: cr,
		s:  s,
	}
}

type visitor struct {
	cr CatalogReader
	s  MutationVisitorStateUpdater
}

func (m *visitor) checkOutTable(ctx context.Context, id descpb.ID) (*tabledesc.Mutable, error) {
	desc, err := m.s.CheckOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*tabledesc.Mutable)
	if !ok {
		return nil, catalog.WrapTableDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

func (m *visitor) checkOutDatabase(ctx context.Context, id descpb.ID) (*dbdesc.Mutable, error) {
	desc, err := m.s.CheckOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*dbdesc.Mutable)
	if !ok {
		return nil, catalog.WrapDatabaseDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

// Stop the linter from complaining.
var _ = ((*visitor)(nil)).checkOutDatabase

func (m *visitor) checkOutSchema(ctx context.Context, id descpb.ID) (*schemadesc.Mutable, error) {
	desc, err := m.s.CheckOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*schemadesc.Mutable)
	if !ok {
		return nil, catalog.WrapSchemaDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

// Stop the linter from complaining.
var _ = ((*visitor)(nil)).checkOutSchema

func (m *visitor) checkOutType(ctx context.Context, id descpb.ID) (*typedesc.Mutable, error) {
	desc, err := m.s.CheckOutDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	mut, ok := desc.(*typedesc.Mutable)
	if !ok {
		return nil, catalog.WrapTypeDescRefErr(id, catalog.NewDescriptorTypeError(desc))
	}
	return mut, nil
}

func (m *visitor) MakeAddedColumnDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeAddedColumnDeleteAndWriteOnly,
) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		ctx,
		table,
		descriptorutils.MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
}

func (m *visitor) UpdateRelationDeps(ctx context.Context, op scop.UpdateRelationDeps) error {
	// TODO(fqazi): Only implemented for sequences.
	tableDesc, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	// Determine all the dependencies for this descriptor.
	dependedOnBy := make([]descpb.TableDescriptor_Reference, len(tableDesc.DependedOnBy))
	addDependency := func(dep descpb.TableDescriptor_Reference) {
		for _, existingDep := range dependedOnBy {
			if dep.Equal(existingDep) {
				return
			}
			dependedOnBy = append(dependedOnBy, dep)
		}
	}
	for _, col := range tableDesc.Columns {
		sequenceRefByID := true
		// Parse the default expression to determine
		// if all references are by ID.
		if col.DefaultExpr != nil && len(col.UsesSequenceIds) > 0 {
			expr, err := parser.ParseExpr(*col.DefaultExpr)
			if err != nil {
				return err
			}
			usedSequences, err := seqexpr.GetUsedSequences(expr)
			if err != nil {
				return err
			}
			if len(usedSequences) > 0 {
				sequenceRefByID = usedSequences[0].IsByID()
			}
		}
		for _, seqID := range col.UsesSequenceIds {
			addDependency(descpb.TableDescriptor_Reference{
				ID:        seqID,
				ColumnIDs: []descpb.ColumnID{col.ID},
				ByID:      sequenceRefByID,
			})
		}
	}
	tableDesc.DependedOnBy = dependedOnBy
	return nil
}

func (m *visitor) RemoveColumnDefaultExpression(
	ctx context.Context, op scop.RemoveColumnDefaultExpression,
) error {
	// Remove the descriptors namespaces as the last stage
	tableDesc, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	column, err := tableDesc.FindColumnWithID(op.ColumnID)
	if err != nil {
		return err
	}

	// Clean up the default expression and the sequence ID's
	column.ColumnDesc().DefaultExpr = nil
	column.ColumnDesc().UsesSequenceIds = nil
	return nil
}

func (m *visitor) AddTypeBackRef(ctx context.Context, op scop.AddTypeBackRef) error {
	mutDesc, err := m.checkOutType(ctx, op.TypeID)
	if err != nil {
		return err
	}
	mutDesc.AddReferencingDescriptorID(op.DescID)
	// Sanity: Validate that a back reference exists by now.
	desc, err := m.cr.MustReadImmutableDescriptor(ctx, op.DescID)
	if err != nil {
		return err
	}
	refDescs, err := desc.GetReferencedDescIDs()
	if err != nil {
		return err
	}
	if !refDescs.Contains(op.TypeID) {
		return errors.AssertionFailedf("Back reference for type %d is missing inside descriptor %d.",
			op.TypeID, op.DescID)
	}
	return nil
}

func (m *visitor) RemoveRelationDependedOnBy(
	ctx context.Context, op scop.RemoveRelationDependedOnBy,
) error {
	// Remove the descriptors namespaces as the last stage
	tableDesc, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	for depIdx, dependedOnBy := range tableDesc.DependedOnBy {
		if dependedOnBy.ID == op.DependedOnBy {
			tableDesc.DependedOnBy = append(tableDesc.DependedOnBy[:depIdx], tableDesc.DependedOnBy[depIdx+1:]...)
			break
		}
	}
	return nil
}

func (m *visitor) RemoveSequenceOwnedBy(ctx context.Context, op scop.RemoveSequenceOwnedBy) error {
	mutDesc, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	mutDesc.GetSequenceOpts().SequenceOwner.OwnerTableID = descpb.InvalidID
	mutDesc.GetSequenceOpts().SequenceOwner.OwnerColumnID = 0
	return nil
}

func (m *visitor) RemoveTypeBackRef(ctx context.Context, op scop.RemoveTypeBackRef) error {
	mutDesc, err := m.checkOutType(ctx, op.TypeID)
	if err != nil {
		return err
	}
	mutDesc.RemoveReferencingDescriptorID(op.DescID)
	return nil
}

func (m *visitor) CreateGcJobForDescriptor(
	ctx context.Context, op scop.CreateGcJobForDescriptor,
) error {
	desc, err := m.cr.MustReadImmutableDescriptor(ctx, op.DescID)
	if err != nil {
		return err
	}
	m.s.AddNewGCJobForDescriptor(desc)
	return nil
}

func (m *visitor) MarkDescriptorAsDropped(
	ctx context.Context, op scop.MarkDescriptorAsDropped,
) error {
	// Before we can mutate the descriptor, get rid of any synthetic descriptor.
	m.cr.RemoveSyntheticDescriptor(op.DescID)
	desc, err := m.s.CheckOutDescriptor(ctx, op.DescID)
	if err != nil {
		return err
	}
	desc.SetDropped()
	return nil
}

func (m *visitor) MarkDescriptorAsDroppedSynthetically(
	ctx context.Context, op scop.MarkDescriptorAsDroppedSynthetically,
) error {
	desc, err := m.cr.MustReadImmutableDescriptor(ctx, op.DescID)
	if err != nil {
		return err
	}
	mut := desc.NewBuilder().BuildCreatedMutable()
	mut.SetDropped()
	m.cr.AddSyntheticDescriptor(mut)
	return nil
}

func (m *visitor) DrainDescriptorName(ctx context.Context, op scop.DrainDescriptorName) error {
	descriptor, err := m.cr.MustReadImmutableDescriptor(ctx, op.TableID)
	if err != nil {
		return err
	}
	// Queue up names for draining.
	nameDetails := descpb.NameInfo{
		ParentID:       descriptor.GetParentID(),
		ParentSchemaID: descriptor.GetParentSchemaID(),
		Name:           descriptor.GetName()}
	m.s.AddDrainedName(descriptor.GetID(), nameDetails)
	return nil
}

func (m *visitor) MakeColumnPublic(ctx context.Context, op scop.MakeColumnPublic) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := removeMutation(
		ctx,
		table,
		descriptorutils.MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
	if err != nil {
		return err
	}
	// TODO(ajwerner): Should the op just have the column descriptor? What's the
	// type hydration status here? Cloning is going to blow away hydration. Is
	// that okay?
	table.Columns = append(table.Columns,
		*(protoutil.Clone(mut.GetColumn())).(*descpb.ColumnDescriptor))
	return nil
}

func (m *visitor) MakeDroppedNonPrimaryIndexDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeDroppedNonPrimaryIndexDeleteAndWriteOnly,
) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	var idx descpb.IndexDescriptor
	for i := range table.Indexes {
		if table.Indexes[i].ID != op.IndexID {
			continue
		}
		idx = table.Indexes[i]
		table.Indexes = append(table.Indexes[:i], table.Indexes[i+1:]...)
		break
	}
	if idx.ID == 0 {
		return errors.AssertionFailedf("failed to find index %d in descriptor %v",
			op.IndexID, table)
	}
	return table.AddIndexMutation(&idx, descpb.DescriptorMutation_DROP)
}

func (m *visitor) MakeDroppedColumnDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeDroppedColumnDeleteAndWriteOnly,
) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	var col descpb.ColumnDescriptor
	for i := range table.Columns {
		if table.Columns[i].ID != op.ColumnID {
			continue
		}
		col = table.Columns[i]
		table.Columns = append(table.Columns[:i], table.Columns[i+1:]...)
		break
	}
	if col.ID == 0 {
		return errors.AssertionFailedf("failed to find column %d in %v", col.ID, table)
	}
	table.AddColumnMutation(&col, descpb.DescriptorMutation_DROP)
	return nil
}

func (m *visitor) MakeDroppedColumnDeleteOnly(
	ctx context.Context, op scop.MakeDroppedColumnDeleteOnly,
) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		ctx,
		table,
		descriptorutils.MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		descpb.DescriptorMutation_DELETE_ONLY,
	)
}

func (m *visitor) MakeColumnAbsent(ctx context.Context, op scop.MakeColumnAbsent) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := removeMutation(
		ctx,
		table,
		descriptorutils.MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_ONLY,
	)
	if err != nil {
		return err
	}
	col := mut.GetColumn()
	table.RemoveColumnFromFamilyAndPrimaryIndex(col.ID)
	return nil
}

func (m *visitor) MakeAddedIndexDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeAddedIndexDeleteAndWriteOnly,
) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		ctx,
		table,
		descriptorutils.MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
}

func (m *visitor) MakeAddedColumnDeleteOnly(
	ctx context.Context, op scop.MakeAddedColumnDeleteOnly,
) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.Column.ID >= table.NextColumnID {
		table.NextColumnID = op.Column.ID + 1
	}
	if !op.Column.IsComputed() ||
		!op.Column.Virtual {
		var foundFamily bool
		for i := range table.Families {
			fam := &table.Families[i]
			if foundFamily = fam.ID == op.FamilyID; foundFamily {
				fam.ColumnIDs = append(fam.ColumnIDs, op.Column.ID)
				fam.ColumnNames = append(fam.ColumnNames, op.Column.Name)
				break
			}
		}
		// Only create column families for non-computed columns
		if !foundFamily {
			table.Families = append(table.Families, descpb.ColumnFamilyDescriptor{
				Name:        op.FamilyName,
				ID:          op.FamilyID,
				ColumnNames: []string{op.Column.Name},
				ColumnIDs:   []descpb.ColumnID{op.Column.ID},
			})
			sort.Slice(table.Families, func(i, j int) bool {
				return table.Families[i].ID < table.Families[j].ID
			})
			if table.NextFamilyID <= op.FamilyID {
				table.NextFamilyID = op.FamilyID + 1
			}
		}
	}
	table.AddColumnMutation(&op.Column, descpb.DescriptorMutation_ADD)
	return nil
}

func (m *visitor) MakeDroppedIndexDeleteOnly(
	ctx context.Context, op scop.MakeDroppedIndexDeleteOnly,
) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		ctx,
		table,
		descriptorutils.MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		descpb.DescriptorMutation_DELETE_ONLY,
	)
}

func (m *visitor) MakeDroppedPrimaryIndexDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeDroppedPrimaryIndexDeleteAndWriteOnly,
) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	if table.PrimaryIndex.ID != op.IndexID {
		return errors.AssertionFailedf("index being dropped (%d) does not match existing primary index (%d).", op.IndexID, table.PrimaryIndex.ID)
	}
	idx := protoutil.Clone(&table.PrimaryIndex).(*descpb.IndexDescriptor)
	return table.AddIndexMutation(idx, descpb.DescriptorMutation_DROP)
}

func (m *visitor) MakeAddedIndexDeleteOnly(
	ctx context.Context, op scop.MakeAddedIndexDeleteOnly,
) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.IndexID >= table.NextIndexID {
		table.NextIndexID = op.IndexID + 1
	}
	// Resolve column names
	colNames := make([]string, 0, len(op.KeyColumnIDs))
	for _, colID := range op.KeyColumnIDs {
		column, err := table.FindColumnWithID(colID)
		if err != nil {
			return err
		}
		colNames = append(colNames, column.GetName())
	}
	storeColNames := make([]string, 0, len(op.StoreColumnIDs))
	for _, colID := range op.StoreColumnIDs {
		column, err := table.FindColumnWithID(colID)
		if err != nil {
			return err
		}
		storeColNames = append(storeColNames, column.GetName())
	}
	// Setup the index descriptor type.
	indexType := descpb.IndexDescriptor_FORWARD
	if op.Inverted {
		indexType = descpb.IndexDescriptor_INVERTED
	}
	// Setup the encoding type.
	encodingType := descpb.PrimaryIndexEncoding
	indexVersion := descpb.PrimaryIndexWithStoredColumnsVersion
	if op.SecondaryIndex {
		encodingType = descpb.SecondaryIndexEncoding
		indexVersion = descpb.StrictIndexColumnIDGuaranteesVersion
	}
	// Create an index descriptor from the the operation.
	idx := &descpb.IndexDescriptor{
		Name:                op.IndexName,
		ID:                  op.IndexID,
		Unique:              op.Unique,
		Version:             indexVersion,
		KeyColumnNames:      colNames,
		KeyColumnIDs:        op.KeyColumnIDs,
		StoreColumnIDs:      op.StoreColumnIDs,
		StoreColumnNames:    storeColNames,
		KeyColumnDirections: op.KeyColumnDirections,
		Type:                indexType,
		KeySuffixColumnIDs:  op.KeySuffixColumnIDs,
		CompositeColumnIDs:  op.CompositeColumnIDs,
		CreatedExplicitly:   true,
		EncodingType:        encodingType,
	}
	if idx.Name == "" {
		name, err := tabledesc.BuildIndexName(table, idx)
		if err != nil {
			return err
		}
		idx.Name = name
	}
	if op.ShardedDescriptor != nil {
		idx.Sharded = *op.ShardedDescriptor
	}
	return table.AddIndexMutation(idx, descpb.DescriptorMutation_ADD)
}

func (m *visitor) AddCheckConstraint(ctx context.Context, op scop.AddCheckConstraint) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	ck := &descpb.TableDescriptor_CheckConstraint{
		Expr:      op.Expr,
		Name:      op.Name,
		ColumnIDs: op.ColumnIDs,
		Hidden:    op.Hidden,
	}
	if op.Unvalidated {
		ck.Validity = descpb.ConstraintValidity_Unvalidated
	} else {
		ck.Validity = descpb.ConstraintValidity_Validating
	}
	table.Checks = append(table.Checks, ck)
	return nil
}

func (m *visitor) MakeAddedSecondaryIndexPublic(
	ctx context.Context, op scop.MakeAddedSecondaryIndexPublic,
) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}

	for idx, idxMutation := range table.GetMutations() {
		if idxMutation.GetIndex() != nil &&
			idxMutation.GetIndex().ID == op.IndexID {
			err := table.MakeMutationComplete(idxMutation)
			if err != nil {
				return err
			}
			table.Mutations = append(table.Mutations[:idx], table.Mutations[idx+1:]...)
			break
		}
	}
	return nil
}

func (m *visitor) MakeAddedPrimaryIndexPublic(
	ctx context.Context, op scop.MakeAddedPrimaryIndexPublic,
) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	index, err := table.FindIndexWithID(op.IndexID)
	if err != nil {
		return err
	}
	indexDesc := index.IndexDescDeepCopy()
	if _, err := removeMutation(
		ctx,
		table,
		descriptorutils.MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	); err != nil {
		return err
	}
	table.PrimaryIndex = indexDesc
	return nil
}

func (m *visitor) MakeIndexAbsent(ctx context.Context, op scop.MakeIndexAbsent) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	_, err = removeMutation(ctx,
		table,
		descriptorutils.MakeIndexIDMutationSelector(op.IndexID),
		descpb.DescriptorMutation_DELETE_ONLY,
	)
	return err
}

func (m *visitor) AddColumnFamily(ctx context.Context, op scop.AddColumnFamily) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	table.AddFamily(op.Family)
	if op.Family.ID >= table.NextFamilyID {
		table.NextFamilyID = op.Family.ID + 1
	}
	return nil
}

func (m *visitor) DropForeignKeyRef(ctx context.Context, op scop.DropForeignKeyRef) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	fks := table.TableDesc().OutboundFKs
	if !op.Outbound {
		fks = table.TableDesc().InboundFKs
	}
	newFks := make([]descpb.ForeignKeyConstraint, 0, len(fks)-1)
	for _, fk := range fks {
		if op.Outbound && (fk.OriginTableID != op.TableID ||
			op.Name != fk.Name) {
			newFks = append(newFks, fk)
		} else if !op.Outbound && (fk.ReferencedTableID != op.TableID ||
			op.Name != fk.Name) {
			newFks = append(newFks, fk)
		}
	}
	if op.Outbound {
		table.TableDesc().OutboundFKs = newFks
	} else {
		table.TableDesc().InboundFKs = newFks
	}
	return nil
}

func (m *visitor) AddIndexPartitionInfo(ctx context.Context, op scop.AddIndexPartitionInfo) error {
	table, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	index, err := table.FindIndexWithID(op.IndexID)
	if err != nil {
		return err
	}
	return m.cr.AddPartitioning(table, index.IndexDesc(), op.PartitionFields, op.ListPartitions, op.RangePartitions, nil, true)
}

func (m *visitor) NoOpInfo(_ context.Context, _ scop.NoOpInfo) error {
	return nil
}

var _ scop.MutationVisitor = (*visitor)(nil)
