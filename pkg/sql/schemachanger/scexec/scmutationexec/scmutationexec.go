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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/descriptorutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/sequence"
	"github.com/cockroachdb/errors"
)

// DescriptorReader encapsulates logic used to retrieve
// and track modified descriptors.
type DescriptorReader interface {
	GetMutableTypeByID(ctx context.Context, id descpb.ID) (*typedesc.Mutable, error)
	GetImmutableDatabaseByID(ctx context.Context, id descpb.ID) (catalog.DatabaseDescriptor, error)
	GetMutableTableByID(ctx context.Context, id descpb.ID) (*tabledesc.Mutable, error)
	GetAnyDescriptorByID(ctx context.Context, id descpb.ID) (catalog.MutableDescriptor, error)
	AddDrainedName(id descpb.ID, nameInfo descpb.NameInfo)
}

// NamespaceWriter encapsulates operations used to manipulate
// namespaces.
type NamespaceWriter interface {
	AddDrainedName(id descpb.ID, nameInfo descpb.NameInfo)
}

// CommentWriter encapsulates operations used to manipulate
// object comments.
type CommentWriter interface {
	RemoveObjectComments(ctx context.Context, id descpb.ID) error
}

// Catalog encapsulates the logic to modify descriptors,
// namespaces, comments to help support schema changer mutations.
type Catalog interface {
	DescriptorReader
	NamespaceWriter
	CommentWriter
}

// MutationJobs encapsulates the logic to create different types
// of jobs.
type MutationJobs interface {
	AddNewGCJobForDescriptor(descriptor catalog.Descriptor)
}

// NewMutationVisitor creates a new scop.MutationVisitor.
func NewMutationVisitor(catalog Catalog, jobs MutationJobs) scop.MutationVisitor {
	return &visitor{catalog: catalog, jobs: jobs}
}

type visitor struct {
	catalog Catalog
	jobs    MutationJobs
}

func (m *visitor) MakeAddedColumnDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeAddedColumnDeleteAndWriteOnly,
) error {
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	tableDesc, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
			usedSequences, err := sequence.GetUsedSequences(expr)
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
	tableDesc, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	mutDesc, err := m.catalog.GetMutableTypeByID(ctx, op.TypeID)
	if err != nil {
		return err
	}
	mutDesc.AddReferencingDescriptorID(op.DescID)
	// Sanity: Validate that a back reference exists by now.
	desc, err := m.catalog.GetAnyDescriptorByID(ctx, op.DescID)
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
	tableDesc, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	mutDesc, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	mutDesc.GetSequenceOpts().SequenceOwner.OwnerTableID = descpb.InvalidID
	mutDesc.GetSequenceOpts().SequenceOwner.OwnerColumnID = 0
	return nil
}

func (m *visitor) RemoveTypeBackRef(ctx context.Context, op scop.RemoveTypeBackRef) error {
	mutDesc, err := m.catalog.GetMutableTypeByID(ctx, op.TypeID)
	if err != nil {
		return err
	}
	mutDesc.RemoveReferencingDescriptorID(op.DescID)
	return nil
}

func (m *visitor) CreateGcJobForDescriptor(
	ctx context.Context, op scop.CreateGcJobForDescriptor,
) error {
	desc, err := m.catalog.GetAnyDescriptorByID(ctx, op.DescID)
	if err != nil {
		return err
	}
	m.jobs.AddNewGCJobForDescriptor(desc)
	return nil
}

func (m *visitor) MarkDescriptorAsDropped(
	ctx context.Context, op scop.MarkDescriptorAsDropped,
) error {
	descriptor, err := m.catalog.GetAnyDescriptorByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	// Mark table as dropped
	descriptor.SetDropped()
	return nil
}

func (m *visitor) DrainDescriptorName(ctx context.Context, op scop.DrainDescriptorName) error {
	descriptor, err := m.catalog.GetAnyDescriptorByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	// Queue up names for draining.
	nameDetails := descpb.NameInfo{
		ParentID:       descriptor.GetParentID(),
		ParentSchemaID: descriptor.GetParentSchemaID(),
		Name:           descriptor.GetName()}
	m.catalog.AddDrainedName(descriptor.GetID(), nameDetails)
	return nil
}

func (m *visitor) MakeColumnPublic(ctx context.Context, op scop.MakeColumnPublic) error {
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}

	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.Column.ID >= table.NextColumnID {
		table.NextColumnID = op.Column.ID + 1
	}
	var foundFamily bool
	for i := range table.Families {
		fam := &table.Families[i]
		if foundFamily = fam.ID == op.FamilyID; foundFamily {
			fam.ColumnIDs = append(fam.ColumnIDs, op.Column.ID)
			fam.ColumnNames = append(fam.ColumnNames, op.Column.Name)
			break
		}
	}
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
	table.AddColumnMutation(&op.Column, descpb.DescriptorMutation_ADD)
	return nil
}

func (m *visitor) MakeDroppedIndexDeleteOnly(
	ctx context.Context, op scop.MakeDroppedIndexDeleteOnly,
) error {
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}

	// NOTE: There is no ordering guarantee between operations which might
	// touch the primary index. Remove it if it has not already been overwritten.
	if table.PrimaryIndex.ID == op.Index.ID {
		table.PrimaryIndex = descpb.IndexDescriptor{}
	}

	idx := protoutil.Clone(&op.Index).(*descpb.IndexDescriptor)
	return table.AddIndexMutation(idx, descpb.DescriptorMutation_DROP)
}

func (m *visitor) MakeAddedIndexDeleteOnly(
	ctx context.Context, op scop.MakeAddedIndexDeleteOnly,
) error {

	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}

	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.Index.ID >= table.NextIndexID {
		table.NextIndexID = op.Index.ID + 1
	}
	// Make some adjustments to the index descriptor so that it behaves correctly
	// as a secondary index while being added.
	idx := protoutil.Clone(&op.Index).(*descpb.IndexDescriptor)
	return table.AddIndexMutation(idx, descpb.DescriptorMutation_ADD)
}

func (m *visitor) AddCheckConstraint(ctx context.Context, op scop.AddCheckConstraint) error {
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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

func (m *visitor) MakeAddedPrimaryIndexPublic(
	ctx context.Context, op scop.MakeAddedPrimaryIndexPublic,
) error {
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
	if err != nil {
		return err
	}
	if _, err := removeMutation(
		ctx,
		table,
		descriptorutils.MakeIndexIDMutationSelector(op.Index.ID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	); err != nil {
		return err
	}
	table.PrimaryIndex = *(protoutil.Clone(&op.Index)).(*descpb.IndexDescriptor)
	return nil
}

func (m *visitor) MakeIndexAbsent(ctx context.Context, op scop.MakeIndexAbsent) error {
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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
	table, err := m.catalog.GetMutableTableByID(ctx, op.TableID)
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

var _ scop.MutationVisitor = (*visitor)(nil)
