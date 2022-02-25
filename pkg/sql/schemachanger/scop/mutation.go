// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scop

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

//go:generate go run ./generate_visitor.go scop Mutation mutation.go mutation_visitor_generated.go

type mutationOp struct{ baseOp }

// Make sure baseOp is used for linter.
var _ = mutationOp{baseOp: baseOp{}}

func (mutationOp) Type() Type { return MutationType }

// NotImplemented is a placeholder for operations which haven't been defined yet.
// TODO(postamar): remove all of these
type NotImplemented struct {
	mutationOp
	ElementType string
}

// MakeAddedIndexDeleteOnly adds a non-existent primary index to the
// table.
type MakeAddedIndexDeleteOnly struct {
	mutationOp
	Index              scpb.Index
	IsSecondaryIndex   bool
	IsDeletePreserving bool
}

// SetAddedIndexPartialPredicate sets a partial predicate expression in an added
// secondary index.
type SetAddedIndexPartialPredicate struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
	Expr    catpb.Expression
}

// MakeAddedIndexDeleteAndWriteOnly transitions an index addition mutation from
// DELETE_ONLY to DELETE_AND_WRITE_ONLY.
type MakeAddedIndexDeleteAndWriteOnly struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeAddedSecondaryIndexPublic moves a new primary index from its mutation to
// public.
type MakeAddedSecondaryIndexPublic struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeAddedPrimaryIndexPublic moves a new primary index from its mutation to
// public.
type MakeAddedPrimaryIndexPublic struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeDroppedPrimaryIndexDeleteAndWriteOnly moves a dropped primary index from
// public to DELETE_AND_WRITE_ONLY.
type MakeDroppedPrimaryIndexDeleteAndWriteOnly struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// CreateGcJobForTable creates a GC job for a given table, when necessary.
type CreateGcJobForTable struct {
	mutationOp
	TableID descpb.ID
}

// CreateGcJobForDatabase creates a GC job for a given database.
type CreateGcJobForDatabase struct {
	mutationOp
	DatabaseID descpb.ID
}

// CreateGcJobForIndex creates a GC job for a given table index.
type CreateGcJobForIndex struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MarkDescriptorAsDroppedSynthetically marks a descriptor as dropped within
// a transaction by injecting a synthetic descriptor.
type MarkDescriptorAsDroppedSynthetically struct {
	mutationOp
	DescID descpb.ID
}

// MarkDescriptorAsDropped marks a descriptor as dropped.
type MarkDescriptorAsDropped struct {
	mutationOp
	DescID descpb.ID
}

// DrainDescriptorName marks a descriptor as dropped.
type DrainDescriptorName struct {
	mutationOp
	Namespace scpb.Namespace
}

// MakeAddedColumnDeleteAndWriteOnly transitions a column addition mutation from
// DELETE_ONLY to DELETE_AND_WRITE_ONLY.
type MakeAddedColumnDeleteAndWriteOnly struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakeDroppedNonPrimaryIndexDeleteAndWriteOnly moves a dropped secondary index
// from public to DELETE_AND_WRITE_ONLY.
type MakeDroppedNonPrimaryIndexDeleteAndWriteOnly struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeDroppedIndexDeleteOnly transitions an index drop mutation from
// DELETE_AND_WRITE_ONLY to DELETE_ONLY.
type MakeDroppedIndexDeleteOnly struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// RemoveDroppedIndexPartialPredicate removes a partial predicate expression in
// a dropped secondary index.
type RemoveDroppedIndexPartialPredicate struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeIndexAbsent removes a dropped index mutation in DELETE_ONLY from the
// table.
type MakeIndexAbsent struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeAddedColumnDeleteOnly adds a new column in the DELETE_ONLY state.
type MakeAddedColumnDeleteOnly struct {
	mutationOp
	Column scpb.Column
}

// SetAddedColumnType sets the type of a new column.
type SetAddedColumnType struct {
	mutationOp
	ColumnType scpb.ColumnType
}

// MakeColumnPublic moves a new column from its mutation to public.
type MakeColumnPublic struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakeDroppedColumnDeleteAndWriteOnly moves a dropped column from public to
// DELETE_AND_WRITE_ONLY.
type MakeDroppedColumnDeleteAndWriteOnly struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakeDroppedColumnDeleteOnly transitions a column drop mutation from
// DELETE_AND_WRITE_ONLY to DELETE_ONLY.
type MakeDroppedColumnDeleteOnly struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// RemoveDroppedColumnType unsets a column drop mutation's type and computed
// expr.
type RemoveDroppedColumnType struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakeColumnAbsent removes a dropped column mutation in DELETE_ONLY from the
// table.
type MakeColumnAbsent struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// RemoveOwnerBackReferenceInSequence removes a sequence ownership
// back-reference from a sequence.
type RemoveOwnerBackReferenceInSequence struct {
	mutationOp
	SequenceID descpb.ID
}

// RemoveSequenceOwner removes a sequence ownership reference from the owning
// table column.
type RemoveSequenceOwner struct {
	mutationOp
	TableID         descpb.ID
	ColumnID        descpb.ColumnID
	OwnedSequenceID descpb.ID
}

// RemoveCheckConstraint removes a check constraint from a table.
type RemoveCheckConstraint struct {
	mutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// RemoveForeignKeyConstraint removes a foreign key from the origin table.
type RemoveForeignKeyConstraint struct {
	mutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// RemoveForeignKeyBackReference removes a foreign key back-reference from the
// referenced table.
type RemoveForeignKeyBackReference struct {
	mutationOp
	ReferencedTableID  descpb.ID
	OriginTableID      descpb.ID
	OriginConstraintID descpb.ConstraintID
}

// RemoveSchemaParent removes the schema - parent database relationship.
type RemoveSchemaParent struct {
	mutationOp
	Parent scpb.SchemaParent
}

// AddIndexPartitionInfo adds a partitioning descriptor to an existing index.
type AddIndexPartitionInfo struct {
	mutationOp
	Partitioning scpb.IndexPartitioning
}

// LogEvent logs an event for a given descriptor.
type LogEvent struct {
	mutationOp
	TargetMetadata scpb.TargetMetadata
	Authorization  scpb.Authorization
	Statement      string
	StatementTag   string
	Element        scpb.ElementProto
	TargetStatus   scpb.Status
}

// AddColumnFamily adds a new column family to the table.
type AddColumnFamily struct {
	mutationOp
	TableID  descpb.ID
	FamilyID descpb.FamilyID
	Name     string
}

// AddColumnDefaultExpression adds a DEFAULT expression to a column.
type AddColumnDefaultExpression struct {
	mutationOp
	Default scpb.ColumnDefaultExpression
}

// RemoveColumnDefaultExpression removes a DEFAULT expression from a column.
type RemoveColumnDefaultExpression struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// AddColumnOnUpdateExpression adds an ON UPDATE expression to a column.
type AddColumnOnUpdateExpression struct {
	mutationOp
	OnUpdate scpb.ColumnOnUpdateExpression
}

// RemoveColumnOnUpdateExpression removes an ON UPDATE expression from a column.
type RemoveColumnOnUpdateExpression struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// UpdateTableBackReferencesInTypes updates back references to a table
// in the specified types.
type UpdateTableBackReferencesInTypes struct {
	mutationOp
	TypeIDs               []descpb.ID
	BackReferencedTableID descpb.ID
}

// RemoveBackReferenceInTypes removes back references to a descriptor in the
// specified types. It is a special case of the previous op for use with views
// and multi-region elements, where a forward reference to the type is being
// removed and is known to be unique to the descriptor.
type RemoveBackReferenceInTypes struct {
	mutationOp
	BackReferencedDescID descpb.ID
	TypeIDs              []descpb.ID
}

// UpdateBackReferencesInSequences updates back references to a table expression
// (in a column or a check constraint) in the specified sequences.
type UpdateBackReferencesInSequences struct {
	mutationOp
	BackReferencedTableID  descpb.ID
	BackReferencedColumnID descpb.ColumnID
	SequenceIDs            []descpb.ID
}

// RemoveViewBackReferencesInRelations removes back references to a view in
// the specified tables, views or sequences.
type RemoveViewBackReferencesInRelations struct {
	mutationOp
	BackReferencedViewID descpb.ID
	RelationIDs          []descpb.ID
}

// SetColumnName renames a column.
type SetColumnName struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
	Name     string
}

// SetIndexName renames an index.
type SetIndexName struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
	Name    string
}

// DeleteDescriptor deletes a descriptor.
type DeleteDescriptor struct {
	mutationOp
	DescriptorID descpb.ID
}

// RemoveJobStateFromDescriptor removes the reference to a job from the
// descriptor and clears the pending targets.
type RemoveJobStateFromDescriptor struct {
	mutationOp
	DescriptorID descpb.ID
	JobID        jobspb.JobID
}

// SetJobStateOnDescriptor adds the reference to a job to the descriptor.
type SetJobStateOnDescriptor struct {
	mutationOp
	DescriptorID descpb.ID
	// Initialize indicates whether this op ought to be setting the JobID and
	// state for the first time for this job or whether it's an update. In true
	// case, the expectation is that the job and state are currently unset. In
	// the false case, the expectation is that they are set and match the values
	// in the op.
	Initialize bool
	State      scpb.DescriptorState
}

// UpdateSchemaChangerJob may update the job's cancelable status.
type UpdateSchemaChangerJob struct {
	mutationOp
	IsNonCancelable bool
	JobID           jobspb.JobID

	// TODO(ajwerner): Plumb and set RunningStatus.
}

// CreateSchemaChangerJob constructs the job for the
// declarative schema changer post-commit phases.
type CreateSchemaChangerJob struct {
	mutationOp
	JobID         jobspb.JobID
	Authorization scpb.Authorization
	Statements    []scpb.Statement
	DescriptorIDs []descpb.ID
}

// RemoveTableComment is used to delete a comment associated with a table.
type RemoveTableComment struct {
	mutationOp
	TableID descpb.ID
}

// RemoveDatabaseComment is used to delete a comment associated with a database.
type RemoveDatabaseComment struct {
	mutationOp
	DatabaseID descpb.ID
}

// RemoveSchemaComment is used to delete a comment associated with a schema.
type RemoveSchemaComment struct {
	mutationOp
	SchemaID descpb.ID
}

// RemoveIndexComment is used to delete a comment associated with an index.
type RemoveIndexComment struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// RemoveColumnComment is used to delete a comment associated with a column.
type RemoveColumnComment struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// RemoveConstraintComment is used to delete a comment associated with a
// constraint.
type RemoveConstraintComment struct {
	mutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// RemoveDatabaseRoleSettings is used to delete a role setting for a database.
type RemoveDatabaseRoleSettings struct {
	mutationOp
	DatabaseID descpb.ID
}

// DeleteSchedule is used to delete a schedule ID from the database.
type DeleteSchedule struct {
	mutationOp
	ScheduleID int64
}
