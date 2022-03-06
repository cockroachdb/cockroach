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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	TableID descpb.ID

	// Various components needed to build an
	// index descriptor.
	PrimaryIndex        descpb.IndexID
	IndexID             descpb.IndexID
	Unique              bool
	KeyColumnIDs        []descpb.ColumnID
	KeyColumnDirections []descpb.IndexDescriptor_Direction
	KeySuffixColumnIDs  []descpb.ColumnID
	StoreColumnIDs      []descpb.ColumnID
	CompositeColumnIDs  []descpb.ColumnID
	ShardedDescriptor   *catpb.ShardedDescriptor
	Inverted            bool
	Concurrently        bool
	SecondaryIndex      bool
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
	TableID descpb.ID
}

// RemoveColumnDefaultExpression removes the default expression on a given table column.
type RemoveColumnDefaultExpression struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// RemoveColumnSequenceReferences strips the depended-on-by references
// from references to usage of the specified sequences by the specified column.
type RemoveColumnSequenceReferences struct {
	mutationOp
	TableID     descpb.ID
	ColumnID    descpb.ColumnID
	SequenceIDs []descpb.ID
}

// AddTypeBackRef adds a type back references from a relation.
type AddTypeBackRef struct {
	mutationOp
	DescID descpb.ID
	TypeID descpb.ID
}

// RemoveRelationDependedOnBy removes a depended on by reference from a given relation.
type RemoveRelationDependedOnBy struct {
	mutationOp
	TableID      descpb.ID
	DependedOnBy descpb.ID
}

// RemoveTypeBackRef removes type back references from a relation.
type RemoveTypeBackRef struct {
	mutationOp
	DescID descpb.ID
	TypeID descpb.ID
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
	ColumnID                          descpb.ColumnID
	TableID                           descpb.ID
	FamilyID                          descpb.FamilyID
	FamilyName                        string
	ColumnType                        *types.T
	Nullable                          bool
	DefaultExpr                       string
	OnUpdateExpr                      string
	Hidden                            bool
	Inaccessible                      bool
	GeneratedAsIdentityType           catpb.GeneratedAsIdentityType
	GeneratedAsIdentitySequenceOption string
	UsesSequenceIDs                   []descpb.ID
	ComputerExpr                      string
	PgAttributeNum                    uint32
	SystemColumnKind                  catpb.SystemColumnKind
	Virtual                           bool
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

// MakeColumnAbsent removes a dropped column mutation in DELETE_ONLY from the
// table.
type MakeColumnAbsent struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

// AddCheckConstraint adds a check constraint in the unvalidated state.
type AddCheckConstraint struct {
	mutationOp
	TableID     descpb.ID
	Name        string
	Expr        string
	ColumnIDs   descpb.ColumnIDs
	Unvalidated bool
	Hidden      bool
}

// AddColumnFamily adds a column family with the provided descriptor.
//
// TODO(ajwerner): Decide whether this should happen explicitly or should be a
// side-effect of adding a column. My hunch is the latter.
type AddColumnFamily struct {
	mutationOp
	TableID descpb.ID
	Family  descpb.ColumnFamilyDescriptor
}

// DropForeignKeyRef drops a foreign key reference with
// support for outbound/inbound keys.
type DropForeignKeyRef struct {
	mutationOp
	TableID  descpb.ID
	Name     string
	Outbound bool
}

// RemoveSequenceOwnedBy removes a sequence owned by
// reference.
type RemoveSequenceOwnedBy struct {
	mutationOp
	SequenceID descpb.ID
}

// AddIndexPartitionInfo adds partitoning information into
// an index
type AddIndexPartitionInfo struct {
	mutationOp
	TableID      descpb.ID
	IndexID      descpb.IndexID
	Partitioning catpb.PartitioningDescriptor
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

// SetColumnName makes a column only to allocate
// the name and ID.
type SetColumnName struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
	Name     string
}

// SetIndexName makes an index name only to allocate
// the name and ID.
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

// DeleteDatabaseSchemaEntry deletes an entry for a schema.
type DeleteDatabaseSchemaEntry struct {
	mutationOp
	DatabaseID descpb.ID
	SchemaID   descpb.ID
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
