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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
)

//go:generate go run ./generate_visitor.go scop ImmediateMutation immediate_mutation.go immediate_mutation_visitor_generated.go

type immediateMutationOp struct{ baseOp }

// Make sure baseOp is used for linter.
var _ = immediateMutationOp{baseOp: baseOp{}}

func (immediateMutationOp) Type() Type { return MutationType }

// NotImplemented is a placeholder for operations which haven't been defined yet.
// TODO(postamar): remove all of these
type NotImplemented struct {
	immediateMutationOp
	ElementType string
}

// UndoAllInTxnImmediateMutationOpSideEffects undoes the side effects of all
// immediate mutation ops which have already executed. This is used to reset
// the state at pre-commit time to re-plan the schema change while taking
// revertibility into account.
type UndoAllInTxnImmediateMutationOpSideEffects struct {
	immediateMutationOp
}

// MakeAbsentTempIndexDeleteOnly adds a non-existent index to the
// table in the DELETE_ONLY state.
type MakeAbsentTempIndexDeleteOnly struct {
	immediateMutationOp
	Index            scpb.Index
	IsSecondaryIndex bool
}

// MakeAbsentIndexBackfilling adds a non-existent index to the
// table in the BACKFILLING state.
type MakeAbsentIndexBackfilling struct {
	immediateMutationOp
	Index              scpb.Index
	IsSecondaryIndex   bool
	IsDeletePreserving bool
}

// SetAddedIndexPartialPredicate sets a partial predicate expression in an added
// secondary index.
type SetAddedIndexPartialPredicate struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
	Expr    catpb.Expression
}

// MakeDeleteOnlyIndexWriteOnly transitions an index addition mutation from
// DELETE_ONLY to WRITE_ONLY.
type MakeDeleteOnlyIndexWriteOnly struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeBackfilledIndexMerging transitions an index addition mutation from
// DELETE_ONLY to MERGING.
type MakeBackfilledIndexMerging struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeMergedIndexWriteOnly transitions an index addition mutation from
// MERGING to WRITE_ONLY.
type MakeMergedIndexWriteOnly struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeBackfillingIndexDeleteOnly transitions an index addition mutation from
// BACKFILLING to DELETE_ONLY.
type MakeBackfillingIndexDeleteOnly struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeValidatedSecondaryIndexPublic moves a new primary index from its mutation to
// public.
type MakeValidatedSecondaryIndexPublic struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeValidatedPrimaryIndexPublic moves a new primary index from its mutation to
// public.
type MakeValidatedPrimaryIndexPublic struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakePublicPrimaryIndexWriteOnly zeros out the table's
// primary index and adds an index drop mutation in WRITE_ONLY
// for it.
type MakePublicPrimaryIndexWriteOnly struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MarkDescriptorAsPublic changes the descriptor's state to PUBLIC.
type MarkDescriptorAsPublic struct {
	immediateMutationOp
	DescriptorID descpb.ID
}

// MarkDescriptorAsDropped changes the descriptor's state to DROPPED.
type MarkDescriptorAsDropped struct {
	immediateMutationOp
	DescriptorID descpb.ID
}

// DrainDescriptorName marks a descriptor's name as to-be-drained from
// the system.Namespace table.
type DrainDescriptorName struct {
	immediateMutationOp
	Namespace scpb.Namespace
}

// MakeDeleteOnlyColumnWriteOnly transitions a column addition mutation from
// DELETE_ONLY to WRITE_ONLY.
type MakeDeleteOnlyColumnWriteOnly struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakePublicSecondaryIndexWriteOnly zeros out the secondary
// index and adds an index drop mutation in WRITE_ONLY
// for it.
type MakePublicSecondaryIndexWriteOnly struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeWriteOnlyIndexDeleteOnly transitions an index drop mutation from
// WRITE_ONLY to DELETE_ONLY.
type MakeWriteOnlyIndexDeleteOnly struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// RemoveDroppedIndexPartialPredicate removes a partial predicate expression in
// a dropped secondary index.
type RemoveDroppedIndexPartialPredicate struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeIndexAbsent removes a dropped index mutation in DELETE_ONLY from the
// table.
type MakeIndexAbsent struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeAbsentColumnDeleteOnly adds a new column in the DELETE_ONLY state.
type MakeAbsentColumnDeleteOnly struct {
	immediateMutationOp
	Column scpb.Column
}

// SetAddedColumnType sets the type of a new column.
type SetAddedColumnType struct {
	immediateMutationOp
	ColumnType scpb.ColumnType
}

// MakeWriteOnlyColumnPublic moves a new column from its mutation to public.
type MakeWriteOnlyColumnPublic struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakePublicColumnWriteOnly zeros out the column and adds
// a column drop mutation in WRITE_ONLY for it.
type MakePublicColumnWriteOnly struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakeWriteOnlyColumnDeleteOnly transitions a column drop mutation from
// WRITE_ONLY to DELETE_ONLY.
type MakeWriteOnlyColumnDeleteOnly struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// RemoveDroppedColumnType unsets a column type and computed expr.
type RemoveDroppedColumnType struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakeDeleteOnlyColumnAbsent removes a dropped column mutation in DELETE_ONLY from the
// table.
type MakeDeleteOnlyColumnAbsent struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// RemoveOwnerBackReferenceInSequence removes a sequence ownership
// back-reference from a sequence.
type RemoveOwnerBackReferenceInSequence struct {
	immediateMutationOp
	SequenceID descpb.ID
}

// RemoveSequenceOwner removes a sequence ownership reference from the owning
// table column.
type RemoveSequenceOwner struct {
	immediateMutationOp
	TableID         descpb.ID
	ColumnID        descpb.ColumnID
	OwnedSequenceID descpb.ID
}

// RemoveCheckConstraint removes a check constraint from a table.
type RemoveCheckConstraint struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// RemoveColumnNotNull removes a NOT NULL, disguised as a CHECK constraint, from a table.
type RemoveColumnNotNull struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// AddCheckConstraint adds a non-existent check constraint
// to the table.
type AddCheckConstraint struct {
	immediateMutationOp
	TableID               descpb.ID
	ConstraintID          descpb.ConstraintID
	ColumnIDs             []descpb.ColumnID
	CheckExpr             catpb.Expression
	FromHashShardedColumn bool
	Validity              descpb.ConstraintValidity
}

// MakeAbsentColumnNotNullWriteOnly adds a non-existent NOT NULL constraint,
// disguised as a CHECK constraint, to the table in the WRITE_ONLY state.
type MakeAbsentColumnNotNullWriteOnly struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakePublicCheckConstraintValidated moves a public
// check constraint to VALIDATED.
type MakePublicCheckConstraintValidated struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// MakePublicColumnNotNullValidated moves a PUBLIC
// column not null to VALIDATED.
type MakePublicColumnNotNullValidated struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// MakeValidatedCheckConstraintPublic moves a new, validated check
// constraint from mutation to public.
type MakeValidatedCheckConstraintPublic struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// MakeValidatedColumnNotNullPublic moves a new, validated NOT NULL, disguised
// as a CHECK constraint, from mutation to public.
type MakeValidatedColumnNotNullPublic struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// AddForeignKeyConstraint adds a non-existent foreign key
// constraint to the table.
type AddForeignKeyConstraint struct {
	immediateMutationOp
	TableID                 descpb.ID
	ConstraintID            descpb.ConstraintID
	ColumnIDs               []descpb.ColumnID
	ReferencedTableID       descpb.ID
	ReferencedColumnIDs     []descpb.ColumnID
	OnUpdateAction          semenumpb.ForeignKeyAction
	OnDeleteAction          semenumpb.ForeignKeyAction
	CompositeKeyMatchMethod semenumpb.Match
	Validity                descpb.ConstraintValidity
}

// MakeValidatedForeignKeyConstraintPublic moves a new, validated foreign key
// constraint from mutation to public.
type MakeValidatedForeignKeyConstraintPublic struct {
	immediateMutationOp
	TableID           descpb.ID
	ConstraintID      descpb.ConstraintID
	ReferencedTableID descpb.ID
}

// MakePublicForeignKeyConstraintValidated moves a public
// check constraint to VALIDATED.
type MakePublicForeignKeyConstraintValidated struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// RemoveForeignKeyConstraint removes a foreign key from the origin table.
type RemoveForeignKeyConstraint struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// RemoveForeignKeyBackReference removes a foreign key back-reference from the
// referenced table.
type RemoveForeignKeyBackReference struct {
	immediateMutationOp
	ReferencedTableID  descpb.ID
	OriginTableID      descpb.ID
	OriginConstraintID descpb.ConstraintID
}

// AddUniqueWithoutIndexConstraint adds a non-existent
// unique_without_index constraint to the table.
type AddUniqueWithoutIndexConstraint struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
	ColumnIDs    []descpb.ColumnID
	PartialExpr  catpb.Expression
	Validity     descpb.ConstraintValidity
}

// MakeValidatedUniqueWithoutIndexConstraintPublic moves a new, validated unique_without_index
// constraint from mutation to public.
type MakeValidatedUniqueWithoutIndexConstraintPublic struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// MakePublicUniqueWithoutIndexConstraintValidated moves a public
// unique_without_index constraint to VALIDATED.
type MakePublicUniqueWithoutIndexConstraintValidated struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// RemoveUniqueWithoutIndexConstraint removes a unique_without_index from the origin table.
type RemoveUniqueWithoutIndexConstraint struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// RemoveSchemaParent removes the schema - parent database relationship.
type RemoveSchemaParent struct {
	immediateMutationOp
	Parent scpb.SchemaParent
}

// AddIndexPartitionInfo adds a partitioning descriptor to an existing index.
type AddIndexPartitionInfo struct {
	immediateMutationOp
	Partitioning scpb.IndexPartitioning
}

// AddColumnFamily adds a new column family to the table.
type AddColumnFamily struct {
	immediateMutationOp
	TableID  descpb.ID
	FamilyID descpb.FamilyID
	Name     string
}

// AddColumnDefaultExpression adds a DEFAULT expression to a column.
type AddColumnDefaultExpression struct {
	immediateMutationOp
	Default scpb.ColumnDefaultExpression
}

// RemoveColumnDefaultExpression removes a DEFAULT expression from a column.
type RemoveColumnDefaultExpression struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// AddColumnOnUpdateExpression adds an ON UPDATE expression to a column.
type AddColumnOnUpdateExpression struct {
	immediateMutationOp
	OnUpdate scpb.ColumnOnUpdateExpression
}

// RemoveColumnOnUpdateExpression removes an ON UPDATE expression from a column.
type RemoveColumnOnUpdateExpression struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// UpdateTableBackReferencesInTypes updates back references to a table
// in the specified types.
type UpdateTableBackReferencesInTypes struct {
	immediateMutationOp
	TypeIDs               []descpb.ID
	BackReferencedTableID descpb.ID
}

// UpdateTypeBackReferencesInTypes updates back references to a type
// in the specified types.
type UpdateTypeBackReferencesInTypes struct {
	immediateMutationOp
	TypeIDs              []descpb.ID
	BackReferencedTypeID descpb.ID
}

// RemoveBackReferenceInTypes removes back references to a descriptor in the
// specified types. It is a special case of the previous op for use with views
// and multi-region elements, where a forward reference to the type is being
// removed and is known to be unique to the descriptor.
type RemoveBackReferenceInTypes struct {
	immediateMutationOp
	BackReferencedDescriptorID descpb.ID
	TypeIDs                    []descpb.ID
}

// UpdateTableBackReferencesInSequences updates back references to a table expression
// (in a column or a check constraint) in the specified sequences.
type UpdateTableBackReferencesInSequences struct {
	immediateMutationOp
	BackReferencedTableID  descpb.ID
	BackReferencedColumnID descpb.ColumnID
	SequenceIDs            []descpb.ID
}

// RemoveBackReferencesInRelations removes back references to a view in
// the specified tables, views or sequences.
type RemoveBackReferencesInRelations struct {
	immediateMutationOp
	BackReferencedID descpb.ID
	RelationIDs      []descpb.ID
}

// SetColumnName renames a column.
type SetColumnName struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
	Name     string
}

// SetIndexName renames an index.
type SetIndexName struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
	Name    string
}

// SetConstraintName renames a constraint.
type SetConstraintName struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
	Name         string
}

// DeleteDescriptor deletes a descriptor.
type DeleteDescriptor struct {
	immediateMutationOp
	DescriptorID descpb.ID
}

// RemoveUserPrivileges is used to revoke a user's privileges.
type RemoveUserPrivileges struct {
	immediateMutationOp
	DescriptorID descpb.ID
	User         string
}

// RemoveJobStateFromDescriptor removes the reference to a job from the
// descriptor and clears the pending targets.
type RemoveJobStateFromDescriptor struct {
	immediateMutationOp
	DescriptorID descpb.ID
	JobID        jobspb.JobID
}

// SetJobStateOnDescriptor adds the reference to a job to the descriptor.
type SetJobStateOnDescriptor struct {
	immediateMutationOp
	DescriptorID descpb.ID
	// Initialize indicates whether this op ought to be setting the JobID and
	// state for the first time for this job or whether it's an update. In true
	// case, the expectation is that the job and state are currently unset. In
	// the false case, the expectation is that they are set and match the values
	// in the op.
	Initialize bool
	State      scpb.DescriptorState
}

// UpsertTableComment is used to add a comment to a table.
type UpsertTableComment struct {
	immediateMutationOp
	TableID descpb.ID
	Comment string
}

// RemoveTableComment is used to delete a comment associated with a table.
type RemoveTableComment struct {
	immediateMutationOp
	TableID descpb.ID
}

// UpsertDatabaseComment is used to add a comment to a database.
type UpsertDatabaseComment struct {
	immediateMutationOp
	DatabaseID descpb.ID
	Comment    string
}

// RemoveDatabaseComment is used to delete a comment associated with a database.
type RemoveDatabaseComment struct {
	immediateMutationOp
	DatabaseID descpb.ID
}

// UpsertSchemaComment is used to add a comment to a schema.
type UpsertSchemaComment struct {
	immediateMutationOp
	SchemaID descpb.ID
	Comment  string
}

// RemoveSchemaComment is used to delete a comment associated with a schema.
type RemoveSchemaComment struct {
	immediateMutationOp
	SchemaID descpb.ID
}

// UpsertIndexComment is used to add a comment to an index.
type UpsertIndexComment struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
	Comment string
}

// RemoveIndexComment is used to delete a comment associated with an index.
type RemoveIndexComment struct {
	immediateMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// UpsertColumnComment is used to add a comment to a column.
type UpsertColumnComment struct {
	immediateMutationOp
	TableID        descpb.ID
	ColumnID       descpb.ColumnID
	PGAttributeNum descpb.PGAttributeNum
	Comment        string
}

// RemoveColumnComment is used to delete a comment associated with a column.
type RemoveColumnComment struct {
	immediateMutationOp
	TableID        descpb.ID
	ColumnID       descpb.ColumnID
	PgAttributeNum descpb.PGAttributeNum
}

// UpsertConstraintComment is used to add a comment to a constraint.
type UpsertConstraintComment struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
	Comment      string
}

// RemoveConstraintComment is used to delete a comment associated with a
// constraint.
type RemoveConstraintComment struct {
	immediateMutationOp
	TableID      descpb.ID
	ConstraintID descpb.ConstraintID
}

// AddColumnToIndex mutates an index to add a column to it.
// The column should already exist on the table and so should
// the index.
type AddColumnToIndex struct {
	immediateMutationOp
	TableID      descpb.ID
	ColumnID     descpb.ColumnID
	IndexID      descpb.IndexID
	Kind         scpb.IndexColumn_Kind
	Direction    catenumpb.IndexColumn_Direction
	Ordinal      uint32
	InvertedKind catpb.InvertedIndexColumnKind
}

// RemoveColumnFromIndex mutates an index to removed a column from it.
// The column should already exist on the table and so should the index.
type RemoveColumnFromIndex struct {
	immediateMutationOp
	TableID      descpb.ID
	ColumnID     descpb.ColumnID
	IndexID      descpb.IndexID
	Kind         scpb.IndexColumn_Kind
	Ordinal      uint32
	InvertedKind catpb.InvertedIndexColumnKind
}

type RemoveObjectParent struct {
	immediateMutationOp
	ObjectID       descpb.ID
	ParentSchemaID descpb.ID
}

type CreateFunctionDescriptor struct {
	immediateMutationOp
	Function scpb.Function
}

type SetFunctionName struct {
	immediateMutationOp
	FunctionID descpb.ID
	Name       string
}

type SetFunctionVolatility struct {
	immediateMutationOp
	FunctionID descpb.ID
	Volatility catpb.Function_Volatility
}

type SetFunctionLeakProof struct {
	immediateMutationOp
	FunctionID descpb.ID
	LeakProof  bool
}

type SetFunctionNullInputBehavior struct {
	immediateMutationOp
	FunctionID        descpb.ID
	NullInputBehavior catpb.Function_NullInputBehavior
}

type SetFunctionBody struct {
	immediateMutationOp
	Body scpb.FunctionBody
}

type SetFunctionParamDefaultExpr struct {
	immediateMutationOp
	Expr scpb.FunctionParamDefaultExpression
}

type UpdateFunctionTypeReferences struct {
	immediateMutationOp
	FunctionID descpb.ID
	TypeIDs    []descpb.ID
}

type UpdateFunctionRelationReferences struct {
	immediateMutationOp
	FunctionID      descpb.ID
	TableReferences []scpb.FunctionBody_TableReference
	ViewReferences  []scpb.FunctionBody_ViewReference
	SequenceIDs     []descpb.ID
}

type SetObjectParentID struct {
	immediateMutationOp
	ObjParent scpb.ObjectParent
}

type UpdateUserPrivileges struct {
	immediateMutationOp
	Privileges scpb.UserPrivileges
}

type UpdateOwner struct {
	immediateMutationOp
	Owner scpb.Owner
}
