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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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
	IndexName           string
	Unique              bool
	KeyColumnIDs        []descpb.ColumnID
	KeyColumnDirections []descpb.IndexDescriptor_Direction
	KeySuffixColumnIDs  []descpb.ColumnID
	StoreColumnIDs      []descpb.ColumnID
	CompositeColumnIDs  []descpb.ColumnID
	ShardedDescriptor   *descpb.ShardedDescriptor
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

// CreateGcJobForDescriptor creates a GC job for a given descriptor.
type CreateGcJobForDescriptor struct {
	mutationOp
	DescID descpb.ID
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

// UpdateRelationDeps updates dependencies for a relation.
type UpdateRelationDeps struct {
	mutationOp
	TableID descpb.ID
}

// RemoveColumnDefaultExpression removes the default expression on a given table column.
type RemoveColumnDefaultExpression struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
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
	GeneratedAsIdentityType           descpb.GeneratedAsIdentityType
	GeneratedAsIdentitySequenceOption string
	UsesSequenceIds                   []descpb.ID
	ComputerExpr                      string
	PgAttributeNum                    uint32
	SystemColumnKind                  descpb.SystemColumnKind
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
	TableID descpb.ID
}

// AddIndexPartitionInfo adds partitoning information into
// an index
type AddIndexPartitionInfo struct {
	mutationOp
	TableID         descpb.ID
	IndexID         descpb.IndexID
	PartitionFields []string
	ListPartitions  []*scpb.ListPartition
	RangePartitions []*scpb.RangePartitions
}

// LogEvent logs an event for a given descriptor.
type LogEvent struct {
	mutationOp
	DescID    descpb.ID
	Metadata  scpb.ElementMetadata
	Element   *scpb.ElementProto
	Direction scpb.Target_Direction
}

// SetColumnName makes a column only to allocate
// the name and ID.
type SetColumnName struct {
	mutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
	Name     string
}

// SetIndexName makes a index name only to allocate
// the name and ID.
type SetIndexName struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
	Name    string
}

// RemoveJobReference removes the reference to a job from the descriptor.
type RemoveJobReference struct {
	mutationOp
	DescriptorID descpb.ID
	JobID        jobspb.JobID
}

// AddJobReference adds the reference to a job to the descriptor.
type AddJobReference struct {
	mutationOp
	DescriptorID descpb.ID
	JobID        jobspb.JobID
}

// CreateDeclarativeSchemaChangerJob constructs the job for the
// declarative schema changer post-commit phases.
type CreateDeclarativeSchemaChangerJob struct {
	mutationOp
	Record jobs.Record
}

// UpdateSchemaChangeJobProgress is used to update the progress of the schema
// change job.
type UpdateSchemaChangeJobProgress struct {
	mutationOp
	JobID    jobspb.JobID
	Statuses []scpb.Status
}
