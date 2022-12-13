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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catenumpb"
)

//go:generate go run ./generate_visitor.go scop Mutation mutation.go mutation_visitor_generated.go

type mutationOp struct{ baseOp }

// EventBase basic fields needed for anything event log related
type EventBase struct {
	TargetMetadata scpb.TargetMetadata
	Authorization  scpb.Authorization
	Statement      string
	StatementTag   string
	Element        scpb.ElementProto
}

// Make sure baseOp is used for linter.
var _ = mutationOp{baseOp: baseOp{}}

func (mutationOp) Type() Type { return MutationType }

// NotImplemented is a placeholder for operations which haven't been defined yet.
// TODO(postamar): remove all of these
type NotImplemented struct {
	mutationOp
	ElementType string
}

// MakeAbsentTempIndexDeleteOnly adds a non-existent index to the
// table in the DELETE_ONLY state.
type MakeAbsentTempIndexDeleteOnly struct {
	mutationOp
	Index            scpb.Index
	IsSecondaryIndex bool
}

// MakeAbsentIndexBackfilling adds a non-existent index to the
// table in the BACKFILLING state.
type MakeAbsentIndexBackfilling struct {
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

// MakeDeleteOnlyIndexWriteOnly transitions an index addition mutation from
// DELETE_ONLY to WRITE_ONLY.
type MakeDeleteOnlyIndexWriteOnly struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeBackfilledIndexMerging transitions an index addition mutation from
// DELETE_ONLY to MERGING.
type MakeBackfilledIndexMerging struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeMergedIndexWriteOnly transitions an index addition mutation from
// MERGING to WRITE_ONLY.
type MakeMergedIndexWriteOnly struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeBackfillingIndexDeleteOnly transitions an index addition mutation from
// BACKFILLING to DELETE_ONLY.
type MakeBackfillingIndexDeleteOnly struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeValidatedSecondaryIndexPublic moves a new primary index from its mutation to
// public.
type MakeValidatedSecondaryIndexPublic struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeValidatedPrimaryIndexPublic moves a new primary index from its mutation to
// public.
type MakeValidatedPrimaryIndexPublic struct {
	mutationOp
	EventBase
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakePublicPrimaryIndexWriteOnly zeros out the table's
// primary index and adds an index drop mutation in WRITE_ONLY
// for it.
type MakePublicPrimaryIndexWriteOnly struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// CreateGCJobForTable creates a GC job for a given table, when necessary.
type CreateGCJobForTable struct {
	mutationOp
	TableID    descpb.ID
	DatabaseID descpb.ID
	StatementForDropJob
}

// CreateGCJobForDatabase creates a GC job for a given database.
type CreateGCJobForDatabase struct {
	mutationOp
	DatabaseID descpb.ID
	StatementForDropJob
}

// CreateGCJobForIndex creates a GC job for a given table index.
type CreateGCJobForIndex struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
	StatementForDropJob
}

// MarkDescriptorAsPublic changes the descriptor's state to PUBLIC.
type MarkDescriptorAsPublic struct {
	mutationOp
	DescriptorID descpb.ID
}

// MarkDescriptorAsSyntheticallyDropped changes the descriptor's state to
// DROPPED, but records that status only synthetically.
type MarkDescriptorAsSyntheticallyDropped struct {
	mutationOp
	DescriptorID descpb.ID
}

// MarkDescriptorAsDropped changes the descriptor's state to DROPPED.
type MarkDescriptorAsDropped struct {
	mutationOp
	DescriptorID descpb.ID
}

// DrainDescriptorName marks a descriptor's name as to-be-drained from
// the system.Namespace table.
type DrainDescriptorName struct {
	mutationOp
	Namespace scpb.Namespace
}

// MakeDeleteOnlyColumnWriteOnly transitions a column addition mutation from
// DELETE_ONLY to WRITE_ONLY.
type MakeDeleteOnlyColumnWriteOnly struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakePublicSecondaryIndexWriteOnly zeros out the secondary
// index and adds an index drop mutation in WRITE_ONLY
// for it.
type MakePublicSecondaryIndexWriteOnly struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeWriteOnlyIndexDeleteOnly transitions an index drop mutation from
// WRITE_ONLY to DELETE_ONLY.
type MakeWriteOnlyIndexDeleteOnly struct {
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
	EventBase
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeAbsentColumnDeleteOnly adds a new column in the DELETE_ONLY state.
type MakeAbsentColumnDeleteOnly struct {
	mutationOp
	Column scpb.Column
}

// SetAddedColumnType sets the type of a new column.
type SetAddedColumnType struct {
	mutationOp
	ColumnType scpb.ColumnType
}

// MakeWriteOnlyColumnPublic moves a new column from its mutation to public.
type MakeWriteOnlyColumnPublic struct {
	mutationOp
	EventBase
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakePublicColumnWriteOnly zeros out the column and adds
// a column drop mutation in WRITE_ONLY for it.
type MakePublicColumnWriteOnly struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakeWriteOnlyColumnDeleteOnly transitions a column drop mutation from
// WRITE_ONLY to DELETE_ONLY.
type MakeWriteOnlyColumnDeleteOnly struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// RemoveDroppedColumnType unsets a column type and computed expr.
type RemoveDroppedColumnType struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakeDeleteOnlyColumnAbsent removes a dropped column mutation in DELETE_ONLY from the
// table.
type MakeDeleteOnlyColumnAbsent struct {
	mutationOp
	EventBase
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

// MakeAbsentCheckConstraintWriteOnly adds a non-existent check constraint
// to the table in the WRITE_ONLY state.
type MakeAbsentCheckConstraintWriteOnly struct {
	mutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
	ColumnIDs    []descpb.ColumnID
	scpb.Expression
	FromHashShardedColumn bool
}

// MakePublicCheckConstraintValidated moves a public
// check constraint to VALIDATED.
type MakePublicCheckConstraintValidated struct {
	mutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// MakeValidatedCheckConstraintPublic moves a new, validated check
// constraint from mutation to public.
type MakeValidatedCheckConstraintPublic struct {
	mutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// MakeAbsentForeignKeyConstraintWriteOnly adds a non-existent foreign key
// constraint to the table in the WRITE_ONLY state.
type MakeAbsentForeignKeyConstraintWriteOnly struct {
	mutationOp
	TableID                 descpb.ID
	ConstraintID            descpb.ConstraintID
	ColumnIDs               []descpb.ColumnID
	ReferencedTableID       descpb.ID
	ReferencedColumnIDs     []descpb.ColumnID
	OnUpdateAction          catenumpb.ForeignKeyAction
	OnDeleteAction          catenumpb.ForeignKeyAction
	CompositeKeyMatchMethod catenumpb.Match
}

// MakeValidatedForeignKeyConstraintPublic moves a new, validated foreign key
// constraint from mutation to public.
type MakeValidatedForeignKeyConstraintPublic struct {
	mutationOp
	TableID           descpb.ID
	ConstraintID      descpb.ConstraintID
	ReferencedTableID descpb.ID
}

// MakePublicForeignKeyConstraintValidated moves a public
// check constraint to VALIDATED.
type MakePublicForeignKeyConstraintValidated struct {
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
	EventBase
	Element      scpb.ElementProto
	TargetStatus scpb.Status
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
	BackReferencedDescriptorID descpb.ID
	TypeIDs                    []descpb.ID
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

// SetConstraintName renames a constraint.
type SetConstraintName struct {
	mutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
	Name         string
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
	IsNonCancelable       bool
	JobID                 jobspb.JobID
	RunningStatus         string
	DescriptorIDsToRemove []descpb.ID
}

// CreateSchemaChangerJob constructs the job for the
// declarative schema changer post-commit phases.
type CreateSchemaChangerJob struct {
	mutationOp
	JobID         jobspb.JobID
	Authorization scpb.Authorization
	Statements    []scpb.Statement
	DescriptorIDs []descpb.ID

	// NonCancelable maps to the job's property, but in the schema changer can
	// be thought of as !Revertible.
	NonCancelable bool
	RunningStatus string
}

// UpsertTableComment is used to add a comment to a table.
type UpsertTableComment struct {
	mutationOp
	TableID descpb.ID
	Comment string
}

// RemoveAllTableComments is used to delete all comments associated with a
// table when dropping a table.
type RemoveAllTableComments struct {
	mutationOp
	TableID descpb.ID
}

// RemoveTableComment is used to delete a comment associated with a table.
type RemoveTableComment struct {
	mutationOp
	TableID descpb.ID
}

// UpsertDatabaseComment is used to add a comment to a database.
type UpsertDatabaseComment struct {
	mutationOp
	DatabaseID descpb.ID
	Comment    string
}

// RemoveDatabaseComment is used to delete a comment associated with a database.
type RemoveDatabaseComment struct {
	mutationOp
	DatabaseID descpb.ID
}

// UpsertSchemaComment is used to add a comment to a schema.
type UpsertSchemaComment struct {
	mutationOp
	SchemaID descpb.ID
	Comment  string
}

// RemoveSchemaComment is used to delete a comment associated with a schema.
type RemoveSchemaComment struct {
	mutationOp
	SchemaID descpb.ID
}

// UpsertIndexComment is used to add a comment to an index.
type UpsertIndexComment struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
	Comment string
}

// RemoveIndexComment is used to delete a comment associated with an index.
type RemoveIndexComment struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// UpsertColumnComment is used to add a comment to a column.
type UpsertColumnComment struct {
	mutationOp
	TableID        descpb.ID
	ColumnID       descpb.ColumnID
	PGAttributeNum descpb.PGAttributeNum
	Comment        string
}

// RemoveColumnComment is used to delete a comment associated with a column.
type RemoveColumnComment struct {
	mutationOp
	TableID        descpb.ID
	ColumnID       descpb.ColumnID
	PgAttributeNum descpb.PGAttributeNum
}

// UpsertConstraintComment is used to add a comment to a constraint.
type UpsertConstraintComment struct {
	mutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
	Comment      string
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

// RemoveUserPrivileges is used to revoke a user's privileges.
type RemoveUserPrivileges struct {
	mutationOp
	DescriptorID descpb.ID
	User         string
}

// DeleteSchedule is used to delete a schedule ID from the database.
type DeleteSchedule struct {
	mutationOp
	ScheduleID int64
}

// RefreshStats is used to queue a table for stats refresh.
type RefreshStats struct {
	mutationOp
	TableID descpb.ID
}

// AddColumnToIndex mutates an index to add a column to it.
// The column should already exist on the table and so should
// the index.
type AddColumnToIndex struct {
	mutationOp
	TableID      descpb.ID
	ColumnID     descpb.ColumnID
	IndexID      descpb.IndexID
	Kind         scpb.IndexColumn_Kind
	Direction    catpb.IndexColumn_Direction
	Ordinal      uint32
	InvertedKind catpb.InvertedIndexColumnKind
}

// RemoveColumnFromIndex mutates an index to removed a column from it.
// The column should already exist on the table and so should the index.
type RemoveColumnFromIndex struct {
	mutationOp
	TableID      descpb.ID
	ColumnID     descpb.ColumnID
	IndexID      descpb.IndexID
	Kind         scpb.IndexColumn_Kind
	Ordinal      uint32
	InvertedKind catpb.InvertedIndexColumnKind
}
