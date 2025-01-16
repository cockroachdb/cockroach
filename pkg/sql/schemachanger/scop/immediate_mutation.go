// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scop

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
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

// NotImplementedForPublicObjects is a placeholder for operations which haven't defined,
// these are okay when dropping objects.
type NotImplementedForPublicObjects struct {
	immediateMutationOp
	ElementType string
	DescID      catid.DescID
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

type InsertTemporarySchema struct {
	immediateMutationOp
	DescriptorID descpb.ID
}

type InsertTemporarySchemaParent struct {
	immediateMutationOp
	SchemaID   descpb.ID
	DatabaseID descpb.ID
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

// AddDescriptorName marks a descriptor's name as to-be-added to the
// system.Namespace table.
type AddDescriptorName struct {
	immediateMutationOp
	Namespace scpb.Namespace
}

// SetNameInDescriptor sets the name field of the descriptor.
type SetNameInDescriptor struct {
	immediateMutationOp
	DescriptorID descpb.ID
	Name         string
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

// UpsertColumnType sets the type of a new column.
type UpsertColumnType struct {
	immediateMutationOp
	ColumnType scpb.ColumnType
}

// AddColumnComputeExpression will add a new compute expression to a column.
type AddColumnComputeExpression struct {
	immediateMutationOp
	ComputeExpression scpb.ColumnComputeExpression
}

// RemoveColumnComputeExpression will remove the compute expression from a column.
type RemoveColumnComputeExpression struct {
	immediateMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
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

// AddOwnerBackReferenceInSequence adds a sequence ownership
// back-reference from a sequence.
type AddOwnerBackReferenceInSequence struct {
	immediateMutationOp
	SequenceID descpb.ID
	TableID    descpb.ID
	ColumnID   descpb.ColumnID
}

// AddSequenceOwner adds a sequence ownership reference from the owning
// table column.
type AddSequenceOwner struct {
	immediateMutationOp
	TableID         descpb.ID
	ColumnID        descpb.ColumnID
	OwnedSequenceID descpb.ID
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

// AddSchemaParent adds the schema - parent database relationship.
// Namely, it updates schema's parentID and parentDatabase's schemas.
type AddSchemaParent struct {
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

// AssertColumnFamilyIsRemoved asserts that a column family is removed, which
// is used as a validation to make sure that the family the element reaches
// absent. The column family cleaned up with the last ColumnType element
// referencing it.
type AssertColumnFamilyIsRemoved struct {
	immediateMutationOp
	TableID  descpb.ID
	FamilyID descpb.FamilyID
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

// AddTrigger adds a trigger to a table.
type AddTrigger struct {
	immediateMutationOp
	Trigger scpb.Trigger
}

// SetTriggerName sets the name of a trigger.
type SetTriggerName struct {
	immediateMutationOp
	Name scpb.TriggerName
}

// SetTriggerEnabled sets the "enabled" status of a trigger, which indicates
// whether it will be executed in response to a triggering event.
type SetTriggerEnabled struct {
	immediateMutationOp
	Enabled scpb.TriggerEnabled
}

// SetTriggerTiming sets the timing of a trigger, which indicates when it
// executes in relation to the triggering event.
type SetTriggerTiming struct {
	immediateMutationOp
	Timing scpb.TriggerTiming
}

// SetTriggerEvents sets the events for a trigger, which indicate the operations
// that fire the trigger.
type SetTriggerEvents struct {
	immediateMutationOp
	Events scpb.TriggerEvents
}

// SetTriggerTransition sets the transition alias(es) of a trigger.
type SetTriggerTransition struct {
	immediateMutationOp
	Transition scpb.TriggerTransition
}

// SetTriggerWhen sets the WHEN condition of a trigger.
type SetTriggerWhen struct {
	immediateMutationOp
	When scpb.TriggerWhen
}

// SetTriggerFunctionCall sets the trigger-function call for a trigger.
type SetTriggerFunctionCall struct {
	immediateMutationOp
	FunctionCall scpb.TriggerFunctionCall
}

// SetTriggerForwardReferences sets the forward references to relations, types,
// and routines for a trigger.
type SetTriggerForwardReferences struct {
	immediateMutationOp
	Deps scpb.TriggerDeps
}

// RemoveTrigger is used to delete a trigger associated with a table.
type RemoveTrigger struct {
	immediateMutationOp
	Trigger scpb.Trigger
}

// AddPolicy adds a policy to a table.
type AddPolicy struct {
	immediateMutationOp
	Policy scpb.Policy
}

// RemovePolicy removes a policy from a table.
type RemovePolicy struct {
	immediateMutationOp
	Policy scpb.Policy
}

// SetPolicyName sets the name of a policy.
type SetPolicyName struct {
	immediateMutationOp
	TableID  descpb.ID
	PolicyID descpb.PolicyID
	Name     string
}

// AddPolicyRole adds a new role to a policy.
type AddPolicyRole struct {
	immediateMutationOp
	Role scpb.PolicyRole
}

// RemovePolicyRole removes an existing role from a policy.
type RemovePolicyRole struct {
	immediateMutationOp
	Role scpb.PolicyRole
}

// SetPolicyUsingExpression will set a new USING expression for a policy.
type SetPolicyUsingExpression struct {
	immediateMutationOp
	TableID  descpb.ID
	PolicyID descpb.PolicyID
	Expr     string
}

// SetPolicyWithCheckExpression will set a new WITH CHECK expression for a policy.
type SetPolicyWithCheckExpression struct {
	immediateMutationOp
	TableID  descpb.ID
	PolicyID descpb.PolicyID
	Expr     string
}

// SetPolicyForwardReferences sets new forward references to relations, types,
// and routines for the expressions in a policy.
type SetPolicyForwardReferences struct {
	immediateMutationOp
	Deps scpb.PolicyDeps
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

type RemoveBackReferenceInFunctions struct {
	immediateMutationOp

	BackReferencedDescriptorID descpb.ID
	FunctionIDs                []descpb.ID
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

// AddTableConstraintBackReferencesInFunctions adds back references to CHECK
// constraint to referenced functions.
type AddTableConstraintBackReferencesInFunctions struct {
	immediateMutationOp
	BackReferencedTableID      descpb.ID
	BackReferencedConstraintID descpb.ConstraintID
	FunctionIDs                []descpb.ID
}

// RemoveTableConstraintBackReferencesFromFunctions removes back references to
// CHECK constraint from referenced functions.
type RemoveTableConstraintBackReferencesFromFunctions struct {
	immediateMutationOp
	BackReferencedTableID      descpb.ID
	BackReferencedConstraintID descpb.ConstraintID
	FunctionIDs                []descpb.ID
}

// AddTableColumnBackReferencesInFunctions adds back-references to columns
// from referenced functions.
type AddTableColumnBackReferencesInFunctions struct {
	immediateMutationOp
	BackReferencedTableID  descpb.ID
	BackReferencedColumnID descpb.ColumnID
	FunctionIDs            []descpb.ID
}

// RemoveTableColumnBackReferencesInFunctions removes back-references to columns
// from referenced functions.
type RemoveTableColumnBackReferencesInFunctions struct {
	immediateMutationOp
	BackReferencedTableID  descpb.ID
	BackReferencedColumnID descpb.ColumnID
	FunctionIDs            []descpb.ID
}

// AddTriggerBackReferencesInRoutines adds back references to a trigger from
// referenced functions.
type AddTriggerBackReferencesInRoutines struct {
	immediateMutationOp
	BackReferencedTableID   descpb.ID
	BackReferencedTriggerID descpb.TriggerID
	RoutineIDs              []descpb.ID
}

// RemoveTriggerBackReferencesInRoutines removes back-references to a trigger
// from referenced functions.
type RemoveTriggerBackReferencesInRoutines struct {
	immediateMutationOp
	BackReferencedTableID   descpb.ID
	BackReferencedTriggerID descpb.TriggerID
	RoutineIDs              []descpb.ID
}

// AddPolicyBackReferenceInFunctions adds back references to a policy from
// referenced functions.
type AddPolicyBackReferenceInFunctions struct {
	immediateMutationOp
	BackReferencedTableID  descpb.ID
	BackReferencedPolicyID descpb.PolicyID
	FunctionIDs            []descpb.ID
}

// RemovePolicyBackReferenceInFunctions removes back-references to a policy
// from referenced functions.
type RemovePolicyBackReferenceInFunctions struct {
	immediateMutationOp
	BackReferencedTableID  descpb.ID
	BackReferencedPolicyID descpb.PolicyID
	FunctionIDs            []descpb.ID
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

// UpsertTypeComment is used to add a comment to a Type.
type UpsertTypeComment struct {
	immediateMutationOp
	TypeID  descpb.ID
	Comment string
}

// RemoveTypeComment is used to delete a comment associated with a Type.
type RemoveTypeComment struct {
	immediateMutationOp
	TypeID descpb.ID
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

type SetFunctionSecurity struct {
	immediateMutationOp
	FunctionID descpb.ID
	Security   catpb.Function_Security
}

type UpdateFunctionTypeReferences struct {
	immediateMutationOp
	FunctionID descpb.ID
	TypeIDs    []descpb.ID
}

type UpdateFunctionRelationReferences struct {
	immediateMutationOp
	FunctionID         descpb.ID
	TableReferences    []scpb.FunctionBody_TableReference
	ViewReferences     []scpb.FunctionBody_ViewReference
	SequenceIDs        []descpb.ID
	FunctionReferences []descpb.ID
}

type UpdateTableBackReferencesInRelations struct {
	immediateMutationOp
	TableID     descpb.ID
	RelationIDs []descpb.ID
}

type SetObjectParentID struct {
	immediateMutationOp
	ObjParent scpb.SchemaChild
}

type UpdateUserPrivileges struct {
	immediateMutationOp
	Privileges scpb.UserPrivileges
}

type UpdateOwner struct {
	immediateMutationOp
	Owner scpb.Owner
}

type CreateSchemaDescriptor struct {
	immediateMutationOp
	SchemaID descpb.ID
}

type CreateSequenceDescriptor struct {
	immediateMutationOp
	SequenceID descpb.ID
	Temporary  bool
}

type SetSequenceOptions struct {
	immediateMutationOp
	SequenceID descpb.ID
	Key        string
	Value      string
}

type InitSequence struct {
	immediateMutationOp
	SequenceID     descpb.ID
	RestartWith    int64
	UseRestartWith bool
}

type CreateDatabaseDescriptor struct {
	immediateMutationOp
	DatabaseID descpb.ID
}

// AddNamedRangeZoneConfig adds a zone config to a named range.
type AddNamedRangeZoneConfig struct {
	immediateMutationOp
	RangeName  zonepb.NamedZone
	ZoneConfig zonepb.ZoneConfig
}

// DiscardNamedRangeZoneConfig discards a zone config from a named range.
type DiscardNamedRangeZoneConfig struct {
	immediateMutationOp
	RangeName zonepb.NamedZone
}

// AddDatabaseZoneConfig adds a zone config to a database.
type AddDatabaseZoneConfig struct {
	immediateMutationOp
	DatabaseID descpb.ID
	ZoneConfig zonepb.ZoneConfig
}

// DiscardZoneConfig discards the zone config for the given ID. For table IDs,
// we use DiscardTableZoneConfig as some extra work is needed for subzones.
type DiscardZoneConfig struct {
	immediateMutationOp
	DescID descpb.ID
}

// DiscardTableZoneConfig discards the zone config for the given table ID. If
// the table has subzones, we mark the table's zone config as a subzone
// placeholder.
type DiscardTableZoneConfig struct {
	immediateMutationOp
	TableID    descpb.ID
	ZoneConfig zonepb.ZoneConfig
}

// DiscardSubzoneConfig discards the subzone config for the given descriptor ID.
// If this is the only subzone for the table, we delete the entry from
// system.zones.
type DiscardSubzoneConfig struct {
	immediateMutationOp
	TableID              descpb.ID
	Subzone              zonepb.Subzone
	SubzoneSpans         []zonepb.SubzoneSpan
	SubzoneIndexToDelete int32
}

// AddTableZoneConfig adds a zone config to a table.
type AddTableZoneConfig struct {
	immediateMutationOp
	TableID    descpb.ID
	ZoneConfig zonepb.ZoneConfig
}

// AddIndexZoneConfig adds a zone config to an index.
type AddIndexZoneConfig struct {
	immediateMutationOp
	TableID              descpb.ID
	Subzone              zonepb.Subzone
	SubzoneSpans         []zonepb.SubzoneSpan
	SubzoneIndexToDelete int32
}

// AddPartitionZoneConfig adds a zone config to a partition.
type AddPartitionZoneConfig struct {
	immediateMutationOp
	TableID              descpb.ID
	Subzone              zonepb.Subzone
	SubzoneSpans         []zonepb.SubzoneSpan
	SubzoneIndexToDelete int32
}
