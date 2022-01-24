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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// CatalogReader describes catalog read operations as required by the mutation
// visitor.
type CatalogReader interface {
	// MustReadImmutableDescriptor reads a descriptor from the catalog by ID.
	MustReadImmutableDescriptor(ctx context.Context, id descpb.ID) (catalog.Descriptor, error)

	// GetFullyQualifiedName gets the fully qualified name from a descriptor ID.
	GetFullyQualifiedName(ctx context.Context, id descpb.ID) (string, error)

	// AddSyntheticDescriptor adds a synthetic descriptor to the reader state.
	// Subsequent calls to MustReadImmutableDescriptor for this ID will return
	// this synthetic descriptor instead of what it would have otherwise returned.
	AddSyntheticDescriptor(desc catalog.Descriptor)

	// RemoveSyntheticDescriptor undoes the effects of AddSyntheticDescriptor.
	RemoveSyntheticDescriptor(id descpb.ID)
}

// Partitioner is the interface for adding partitioning to a table descriptor.
type Partitioner interface {
	AddPartitioning(
		ctx context.Context,
		tbl *tabledesc.Mutable,
		index catalog.Index,
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

	// DeleteDescriptor adds a descriptor for deletion.
	DeleteDescriptor(id descpb.ID)

	// DeleteComment removes comments for a descriptor
	DeleteComment(id descpb.ID, subID int, commentType keys.CommentType)

	// DeleteConstraintComment removes comments for a descriptor
	DeleteConstraintComment(
		ctx context.Context,
		tbl catalog.TableDescriptor,
		constraintID descpb.ConstraintID,
	) error

	// AddNewGCJobForTable enqueues a GC job for the given table.
	AddNewGCJobForTable(descriptor catalog.TableDescriptor)

	// AddNewGCJobForDatabase enqueues a GC job for the given database.
	AddNewGCJobForDatabase(descriptor catalog.DatabaseDescriptor)

	// AddNewGCJobForIndex enqueues a GC job for the given table index.
	AddNewGCJobForIndex(tbl catalog.TableDescriptor, index catalog.Index)

	// AddNewSchemaChangerJob adds a schema changer job.
	AddNewSchemaChangerJob(jobID jobspb.JobID, targetState scpb.TargetState, current []scpb.Status) error

	// UpdateSchemaChangerJob will update the progress and payload of the
	// schema changer job.
	UpdateSchemaChangerJob(jobID jobspb.JobID, current []scpb.Status, isNonCancelable bool) error

	// EnqueueEvent will enqueue an event to be written to the event log.
	EnqueueEvent(id descpb.ID, metadata scpb.TargetMetadata, details eventpb.CommonSQLEventDetails, event eventpb.EventPayload) error
}

// NewMutationVisitor creates a new scop.MutationVisitor.
func NewMutationVisitor(
	cr CatalogReader, s MutationVisitorStateUpdater, p Partitioner,
) scop.MutationVisitor {
	return &visitor{
		cr: cr,
		s:  s,
		p:  p,
	}
}

type visitor struct {
	cr CatalogReader
	s  MutationVisitorStateUpdater
	p  Partitioner
}

func (m *visitor) RemoveJobReference(ctx context.Context, reference scop.RemoveJobReference) error {
	return m.swapSchemaChangeJobID(ctx, reference.DescriptorID, reference.JobID, 0)
}

func (m *visitor) AddJobReference(ctx context.Context, reference scop.AddJobReference) error {
	return m.swapSchemaChangeJobID(ctx, reference.DescriptorID, 0, reference.JobID)
}

func (m *visitor) swapSchemaChangeJobID(
	ctx context.Context, descID descpb.ID, exp, to jobspb.JobID,
) error {
	// TODO(ajwerner): Support all of the descriptor types. We need to write this
	// to avoid concurrency.
	d, err := m.cr.MustReadImmutableDescriptor(ctx, descID)

	// If we're clearing the status, we might have already deleted the
	// descriptor. Permit that by detecting the prior deletion and
	// short-circuiting.
	//
	// TODO(ajwerner): Ideally we'd model the clearing of the job dependency as
	// an operation which has to happen before deleting the descriptor. If that
	// were the case, this error would become unexpected.
	if errors.Is(err, catalog.ErrDescriptorNotFound) && to == 0 {
		return nil
	}
	if err != nil {
		return err
	}
	// Short-circuit writing an update if this isn't a table because we'd have
	// no field to touch.
	if _, isTable := d.(catalog.TableDescriptor); !isTable {
		return nil
	}

	tbl, err := m.s.CheckOutDescriptor(ctx, descID)
	if err != nil {
		return err
	}
	mut, ok := tbl.(*tabledesc.Mutable)
	if !ok {
		return nil
	}
	if jobspb.JobID(mut.NewSchemaChangeJobID) != exp {
		return errors.AssertionFailedf(
			"unexpected schema change job ID %d on table %d, expected %d",
			mut.NewSchemaChangeJobID, descID, exp,
		)
	}
	mut.NewSchemaChangeJobID = int64(to)
	return nil
}

func (m *visitor) CreateDeclarativeSchemaChangerJob(
	ctx context.Context, job scop.CreateDeclarativeSchemaChangerJob,
) error {
	return m.s.AddNewSchemaChangerJob(job.JobID, job.TargetState, job.Current)
}

func (m *visitor) UpdateSchemaChangerJob(
	ctx context.Context, progress scop.UpdateSchemaChangerJob,
) error {
	return m.s.UpdateSchemaChangerJob(progress.JobID, progress.Current, progress.IsNonCancelable)
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

func (m *visitor) NotImplemented(_ context.Context, op scop.NotImplemented) error {
	return nil
}

func (m *visitor) MakeAddedColumnDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeAddedColumnDeleteAndWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_ONLY,
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
}

func (m *visitor) UpdateRelationDeps(ctx context.Context, op scop.UpdateRelationDeps) error {
	// TODO(fqazi): Only implemented for sequences.
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	// Determine all the dependencies for this descriptor.
	dependedOnBy := make([]descpb.TableDescriptor_Reference, len(tbl.DependedOnBy))
	addDependency := func(dep descpb.TableDescriptor_Reference) {
		for _, existingDep := range dependedOnBy {
			if dep.Equal(existingDep) {
				return
			}
			dependedOnBy = append(dependedOnBy, dep)
		}
	}
	for _, col := range tbl.Columns {
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
	tbl.DependedOnBy = dependedOnBy
	return nil
}

func (m *visitor) RemoveColumnDefaultExpression(
	ctx context.Context, op scop.RemoveColumnDefaultExpression,
) error {
	// Remove the descriptors namespaces as the last stage
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	column, err := tbl.FindColumnWithID(op.ColumnID)
	if err != nil {
		return err
	}

	// Clean up the default expression and the sequence ID's
	column.ColumnDesc().DefaultExpr = nil
	column.ColumnDesc().UsesSequenceIds = nil
	return nil
}

func (m *visitor) AddTypeBackRef(ctx context.Context, op scop.AddTypeBackRef) error {
	typ, err := m.checkOutType(ctx, op.TypeID)
	if err != nil {
		return err
	}
	typ.AddReferencingDescriptorID(op.DescID)
	// Sanity: Validate that a back reference exists by now.
	desc, err := m.cr.MustReadImmutableDescriptor(ctx, op.DescID)
	if err != nil {
		return err
	}
	refDescIDs, err := desc.GetReferencedDescIDs()
	if err != nil {
		return err
	}
	if !refDescIDs.Contains(op.TypeID) {
		return errors.AssertionFailedf("Back reference for type %d is missing inside descriptor %d.",
			op.TypeID, op.DescID)
	}
	return nil
}

func (m *visitor) RemoveRelationDependedOnBy(
	ctx context.Context, op scop.RemoveRelationDependedOnBy,
) error {
	// Clean up dependencies for the relationship.
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	depDesc, err := m.checkOutTable(ctx, op.DependedOnBy)
	if err != nil {
		return err
	}
	for depIdx, dependsOnBy := range tbl.DependedOnBy {
		if dependsOnBy.ID == op.DependedOnBy {
			tbl.DependedOnBy = append(tbl.DependedOnBy[:depIdx], tbl.DependedOnBy[depIdx+1:]...)
			break
		}
	}
	// Intentionally set empty slices to nil, so that for our data driven tests
	// these fields are omitted in the output.
	if len(tbl.DependedOnBy) == 0 {
		tbl.DependedOnBy = nil
	}
	for depIdx, dependsOn := range depDesc.DependsOn {
		if dependsOn == op.TableID {
			depDesc.DependsOn = append(depDesc.DependsOn[:depIdx], depDesc.DependsOn[depIdx+1:]...)
			break
		}
	}
	// Intentionally set empty slices to nil, so that for our data driven tests
	// these fields are omitted in the output.
	if len(depDesc.DependsOn) == 0 {
		depDesc.DependsOn = nil
	}
	return nil
}

func removeOwnedByFromColumn(col *descpb.ColumnDescriptor, seqID descpb.ID) (found bool) {
	for idx := range col.OwnsSequenceIds {
		if col.OwnsSequenceIds[idx] == seqID {
			col.OwnsSequenceIds = append(
				col.OwnsSequenceIds[:idx],
				col.OwnsSequenceIds[idx+1:]...)
			return true
		}
	}
	return false
}

func (m *visitor) RemoveSequenceOwnedBy(ctx context.Context, op scop.RemoveSequenceOwnedBy) error {
	tbl, err := m.checkOutTable(ctx, op.SequenceID)
	if err != nil {
		return err
	}
	// Clean up the ownership inside the owning table first.
	sequenceOwner := &tbl.GetSequenceOpts().SequenceOwner
	ownedByTbl, err := m.checkOutTable(ctx, sequenceOwner.OwnerTableID)
	if err != nil {
		return err
	}
	col, err := ownedByTbl.FindColumnWithID(sequenceOwner.OwnerColumnID)
	if err != nil {
		return err
	}
	if found := removeOwnedByFromColumn(col.ColumnDesc(), op.SequenceID); !found {
		return errors.AssertionFailedf("unable to find sequence (%d) owned by"+
			" inside table (%d) and column (%d)",
			op.SequenceID,
			sequenceOwner.OwnerTableID,
			sequenceOwner.OwnerColumnID)
	}
	// Next, clean the ownership on the sequence.
	tbl.GetSequenceOpts().SequenceOwner.OwnerTableID = descpb.InvalidID
	tbl.GetSequenceOpts().SequenceOwner.OwnerColumnID = 0
	return nil
}

func (m *visitor) RemoveTypeBackRef(ctx context.Context, op scop.RemoveTypeBackRef) error {
	typ, err := m.checkOutType(ctx, op.TypeID)
	if err != nil {
		return err
	}
	typ.RemoveReferencingDescriptorID(op.DescID)
	return nil
}

func (m *visitor) CreateGcJobForTable(ctx context.Context, op scop.CreateGcJobForTable) error {
	desc, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	m.s.AddNewGCJobForTable(desc)
	return nil
}

func (m *visitor) CreateGcJobForDatabase(
	ctx context.Context, op scop.CreateGcJobForDatabase,
) error {
	desc, err := m.checkOutDatabase(ctx, op.DatabaseID)
	if err != nil {
		return err
	}
	m.s.AddNewGCJobForDatabase(desc)
	return nil
}

func (m *visitor) CreateGcJobForIndex(ctx context.Context, op scop.CreateGcJobForIndex) error {
	desc, err := m.cr.MustReadImmutableDescriptor(ctx, op.TableID)
	if err != nil {
		return err
	}
	tbl, err := catalog.AsTableDescriptor(desc)
	if err != nil {
		return err
	}
	idx, err := tbl.FindIndexWithID(op.IndexID)
	if err != nil {
		return errors.AssertionFailedf("table %q (%d): could not find index %d", tbl.GetName(), tbl.GetID(), op.IndexID)
	}
	m.s.AddNewGCJobForIndex(tbl, idx)
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
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := removeMutation(
		tbl,
		MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
	)
	if err != nil {
		return err
	}
	// TODO(ajwerner): Should the op just have the column descriptor? What's the
	// type hydration status here? Cloning is going to blow away hydration. Is
	// that okay?
	tbl.Columns = append(tbl.Columns,
		*(protoutil.Clone(mut.GetColumn())).(*descpb.ColumnDescriptor))
	return nil
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

func (m *visitor) MakeDroppedColumnDeleteAndWriteOnly(
	ctx context.Context, op scop.MakeDroppedColumnDeleteAndWriteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	for i, col := range tbl.PublicColumns() {
		if col.GetID() == op.ColumnID {
			desc := col.ColumnDescDeepCopy()
			tbl.Columns = append(tbl.Columns[:i], tbl.Columns[i+1:]...)
			return enqueueDropColumnMutation(tbl, &desc)
		}
	}
	return errors.AssertionFailedf("failed to find column %d in %v", op.ColumnID, tbl)
}

func (m *visitor) MakeDroppedColumnDeleteOnly(
	ctx context.Context, op scop.MakeDroppedColumnDeleteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	return mutationStateChange(
		tbl,
		MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		descpb.DescriptorMutation_DELETE_ONLY,
	)
}

func (m *visitor) MakeColumnAbsent(ctx context.Context, op scop.MakeColumnAbsent) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	mut, err := removeMutation(
		tbl,
		MakeColumnIDMutationSelector(op.ColumnID),
		descpb.DescriptorMutation_DELETE_ONLY,
	)
	if err != nil {
		return err
	}
	col := mut.GetColumn()
	tbl.RemoveColumnFromFamilyAndPrimaryIndex(col.ID)
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

func (m *visitor) SetColumnName(ctx context.Context, op scop.SetColumnName) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	col, err := tbl.FindColumnWithID(op.ColumnID)
	if err != nil {
		return errors.AssertionFailedf("column %d not found in table %q (%d)", op.ColumnID, tbl.GetName(), tbl.GetID())
	}
	return tabledesc.RenameColumnInTable(tbl, col, tree.Name(op.Name), nil /* isShardColumnRenameable */)
}

func (m *visitor) MakeAddedColumnDeleteOnly(
	ctx context.Context, op scop.MakeAddedColumnDeleteOnly,
) error {
	name := tabledesc.ColumnNamePlaceholder(op.ColumnID)
	emptyStrToNil := func(v string) *string {
		if v == "" {
			return nil
		}
		return &v
	}
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}

	if op.ComputerExpr == "" ||
		!op.Virtual {
		foundFamily := false
		for i := range tbl.Families {
			fam := &tbl.Families[i]
			if foundFamily = fam.ID == op.FamilyID; foundFamily {
				fam.ColumnIDs = append(fam.ColumnIDs, op.ColumnID)
				fam.ColumnNames = append(fam.ColumnNames, name)
				break
			}
		}
		if !foundFamily {
			tbl.Families = append(tbl.Families, descpb.ColumnFamilyDescriptor{
				Name:        op.FamilyName,
				ID:          op.FamilyID,
				ColumnNames: []string{name},
				ColumnIDs:   []descpb.ColumnID{op.ColumnID},
			})
			sort.Slice(tbl.Families, func(i, j int) bool {
				return tbl.Families[i].ID < tbl.Families[j].ID
			})
			if tbl.NextFamilyID <= op.FamilyID {
				tbl.NextFamilyID = op.FamilyID + 1
			}
		}
	}

	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.ColumnID >= tbl.NextColumnID {
		tbl.NextColumnID = op.ColumnID + 1
	}

	return enqueueAddColumnMutation(tbl, &descpb.ColumnDescriptor{
		ID:                                op.ColumnID,
		Name:                              name,
		Type:                              op.ColumnType,
		Nullable:                          op.Nullable,
		DefaultExpr:                       emptyStrToNil(op.DefaultExpr),
		OnUpdateExpr:                      emptyStrToNil(op.OnUpdateExpr),
		Hidden:                            op.Hidden,
		Inaccessible:                      op.Inaccessible,
		GeneratedAsIdentityType:           op.GeneratedAsIdentityType,
		GeneratedAsIdentitySequenceOption: emptyStrToNil(op.GeneratedAsIdentitySequenceOption),
		UsesSequenceIds:                   op.UsesSequenceIds,
		ComputeExpr:                       emptyStrToNil(op.ComputerExpr),
		PGAttributeNum:                    op.PgAttributeNum,
		SystemColumnKind:                  op.SystemColumnKind,
		Virtual:                           op.Virtual,
	})
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

func (m *visitor) MakeAddedIndexDeleteOnly(
	ctx context.Context, op scop.MakeAddedIndexDeleteOnly,
) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	// TODO(ajwerner): deal with ordering the indexes or sanity checking this
	// or what-not.
	if op.IndexID >= tbl.NextIndexID {
		tbl.NextIndexID = op.IndexID + 1
	}
	// Resolve column names
	colNames, err := columnNamesFromIDs(tbl, op.KeyColumnIDs)
	if err != nil {
		return err
	}
	storeColNames, err := columnNamesFromIDs(tbl, op.StoreColumnIDs)
	if err != nil {
		return err
	}
	// Set up the index descriptor type.
	indexType := descpb.IndexDescriptor_FORWARD
	if op.Inverted {
		indexType = descpb.IndexDescriptor_INVERTED
	}
	// Set up the encoding type.
	encodingType := descpb.PrimaryIndexEncoding
	indexVersion := descpb.LatestPrimaryIndexDescriptorVersion
	if op.SecondaryIndex {
		encodingType = descpb.SecondaryIndexEncoding
		indexVersion = descpb.LatestNonPrimaryIndexDescriptorVersion
	}
	// Create an index descriptor from the operation.
	idx := &descpb.IndexDescriptor{
		ID:                  op.IndexID,
		Name:                tabledesc.IndexNamePlaceholder(op.IndexID),
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
	if op.ShardedDescriptor != nil {
		idx.Sharded = *op.ShardedDescriptor
	}
	return enqueueAddIndexMutation(tbl, idx)
}

func (m *visitor) AddCheckConstraint(ctx context.Context, op scop.AddCheckConstraint) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
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
	tbl.Checks = append(tbl.Checks, ck)
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

func (m *visitor) AddColumnFamily(ctx context.Context, op scop.AddColumnFamily) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	tbl.AddFamily(op.Family)
	if op.Family.ID >= tbl.NextFamilyID {
		tbl.NextFamilyID = op.Family.ID + 1
	}
	return nil
}

func (m *visitor) DropForeignKeyRef(ctx context.Context, op scop.DropForeignKeyRef) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	fks := tbl.TableDesc().OutboundFKs
	if !op.Outbound {
		fks = tbl.TableDesc().InboundFKs
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
		tbl.TableDesc().OutboundFKs = newFks
	} else {
		tbl.TableDesc().InboundFKs = newFks
	}
	return nil
}

func (m *visitor) LogEvent(ctx context.Context, op scop.LogEvent) error {
	descID := screl.GetDescID(op.Element.Element())
	fullName, err := m.cr.GetFullyQualifiedName(ctx, descID)
	if err != nil {
		return err
	}
	event, err := asEventPayload(ctx, fullName, op.Element.Element(), op.TargetStatus, m)
	if err != nil {
		return err
	}
	details := eventpb.CommonSQLEventDetails{
		ApplicationName: op.Authorization.AppName,
		User:            op.Authorization.UserName,
		Statement:       redact.RedactableString(op.Statement),
		Tag:             op.StatementTag,
	}
	return m.s.EnqueueEvent(descID, op.TargetMetadata, details, event)
}

func asEventPayload(
	ctx context.Context, fullName string, e scpb.Element, targetStatus scpb.Status, m *visitor,
) (eventpb.EventPayload, error) {
	if targetStatus == scpb.Status_ABSENT {
		switch e.(type) {
		case *scpb.Table:
			return &eventpb.DropTable{TableName: fullName}, nil
		case *scpb.View:
			return &eventpb.DropView{ViewName: fullName}, nil
		case *scpb.Sequence:
			return &eventpb.DropSequence{SequenceName: fullName}, nil
		case *scpb.Database:
			return &eventpb.DropDatabase{DatabaseName: fullName}, nil
		case *scpb.Schema:
			return &eventpb.DropSchema{SchemaName: fullName}, nil
		case *scpb.Type:
			return &eventpb.DropType{TypeName: fullName}, nil
		}
	}
	switch e := e.(type) {
	case *scpb.Column:
		tbl, err := m.checkOutTable(ctx, e.TableID)
		if err != nil {
			return nil, err
		}
		mutation, err := FindMutation(tbl, MakeColumnIDMutationSelector(e.ColumnID))
		if err != nil {
			return nil, err
		}
		return &eventpb.AlterTable{
			TableName:  fullName,
			MutationID: uint32(mutation.MutationID()),
		}, nil
	case *scpb.SecondaryIndex:
		tbl, err := m.checkOutTable(ctx, e.TableID)
		if err != nil {
			return nil, err
		}
		mutation, err := FindMutation(tbl, MakeIndexIDMutationSelector(e.IndexID))
		if err != nil {
			return nil, err
		}
		switch targetStatus {
		case scpb.Status_PUBLIC:
			return &eventpb.AlterTable{
				TableName:  fullName,
				MutationID: uint32(mutation.MutationID()),
			}, nil
		case scpb.Status_ABSENT:
			return &eventpb.DropIndex{
				TableName:  fullName,
				IndexName:  mutation.AsIndex().GetName(),
				MutationID: uint32(mutation.MutationID()),
			}, nil
		default:
			return nil, errors.AssertionFailedf("unknown target status %s", targetStatus)
		}
	}
	return nil, errors.AssertionFailedf("unknown %s element type %T", targetStatus.String(), e)
}

func (m *visitor) AddIndexPartitionInfo(ctx context.Context, op scop.AddIndexPartitionInfo) error {
	tbl, err := m.checkOutTable(ctx, op.TableID)
	if err != nil {
		return err
	}
	index, err := tbl.FindIndexWithID(op.IndexID)
	if err != nil {
		return err
	}
	return m.p.AddPartitioning(
		ctx,
		tbl,
		index,
		op.PartitionFields,
		op.ListPartitions,
		op.RangePartitions,
		nil,  /* allowedNewColumnNames */
		true, /* allowImplicitPartitioning */
	)
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

func (m *visitor) DeleteDescriptor(_ context.Context, op scop.DeleteDescriptor) error {
	m.s.DeleteDescriptor(op.DescriptorID)
	return nil
}

func (m *visitor) DeleteDatabaseSchemaEntry(
	ctx context.Context, op scop.DeleteDatabaseSchemaEntry,
) error {
	db, err := m.checkOutDatabase(ctx, op.DatabaseID)
	if err != nil {
		return err
	}
	sc, err := m.cr.MustReadImmutableDescriptor(ctx, op.SchemaID)
	if err != nil {
		return err
	}
	delete(db.Schemas, sc.GetName())
	return nil
}

func (m *visitor) RemoveTableComment(_ context.Context, op scop.RemoveTableComment) error {
	m.s.DeleteComment(op.TableID, 0, keys.TableCommentType)
	return nil
}

func (m *visitor) RemoveDatabaseComment(_ context.Context, op scop.RemoveDatabaseComment) error {
	m.s.DeleteComment(op.DatabaseID, 0, keys.DatabaseCommentType)
	return nil
}

func (m *visitor) RemoveSchemaComment(_ context.Context, op scop.RemoveSchemaComment) error {
	m.s.DeleteComment(op.SchemaID, 0, keys.SchemaCommentType)
	return nil
}

func (m *visitor) RemoveIndexComment(_ context.Context, op scop.RemoveIndexComment) error {
	m.s.DeleteComment(op.TableID, int(op.IndexID), keys.IndexCommentType)
	return nil
}

func (m *visitor) RemoveColumnComment(_ context.Context, op scop.RemoveColumnComment) error {
	m.s.DeleteComment(op.TableID, int(op.ColumnID), keys.ColumnCommentType)
	return nil
}

func (m *visitor) RemoveConstraintComment(
	ctx context.Context, op scop.RemoveConstraintComment,
) error {
	tbl, err := m.cr.MustReadImmutableDescriptor(ctx, op.TableID)
	if err != nil {
		return err
	}
	return m.s.DeleteConstraintComment(ctx, tbl.(catalog.TableDescriptor), op.ConstraintID)
}

var _ scop.MutationVisitor = (*visitor)(nil)
