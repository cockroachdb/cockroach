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

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

//go:generate bash ./generate_visitor.sh scop Mutation mutation.go mutation_visitor.go

type descriptorMutationOp struct{ baseOp }

func (descriptorMutationOp) Type() Type { return DescriptorMutationType }

// MakeAddedIndexDeleteOnly adds a non-existent primary index to the
// table.
type MakeAddedIndexDeleteOnly struct {
	descriptorMutationOp
	TableID descpb.ID

	// Index represents the index as it should appear in the mutation.
	Index descpb.IndexDescriptor
}

// MakeAddedIndexDeleteAndWriteOnly transitions an index addition mutation from
// DELETE_ONLY to DELETE_AND_WRITE_ONLY.
type MakeAddedIndexDeleteAndWriteOnly struct {
	descriptorMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeAddedPrimaryIndexPublic moves a new primary index from its mutation to
// public.
type MakeAddedPrimaryIndexPublic struct {
	descriptorMutationOp
	TableID descpb.ID
	Index   descpb.IndexDescriptor
}

// MakeDroppedPrimaryIndexDeleteAndWriteOnly moves a dropped primary index from
// public to DELETE_AND_WRITE_ONLY.
type MakeDroppedPrimaryIndexDeleteAndWriteOnly struct {
	descriptorMutationOp
	TableID descpb.ID

	// Index is the descriptor as it should be added as part of the mutation. The
	// primary index of a table has a slightly different encoding than that of
	// a secondary index. The value here sets it as it should be when adding
	// the mutation, including the stored columns.
	Index descpb.IndexDescriptor
}

type MakeAddedColumnDescriptorDeleteAndWriteOnly struct {
	descriptorMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

type MakeDroppedNonPrimaryIndexDeleteAndWriteOnly struct {
	descriptorMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

type MakeDroppedIndexDeleteOnly struct {
	descriptorMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

type MakeIndexAbsent struct {
	descriptorMutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

type MakeAddedColumnDescriptorDeleteOnly struct {
	descriptorMutationOp
	TableID      descpb.ID
	ColumnFamily descpb.FamilyID
	Column       descpb.ColumnDescriptor
}

type MakeDroppedColumnDescriptorDeleteAndWriteOnly struct {
	descriptorMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

type MakeColumnDescriptorPublic struct {
	descriptorMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

type MakeDroppedColumnDeleteAndWriteOnly struct {
	descriptorMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

type MakeDroppedColumnDeleteOnly struct {
	descriptorMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

type MakeColumnAbsent struct {
	descriptorMutationOp
	TableID  descpb.ID
	ColumnID descpb.ColumnID
}

type AddCheckConstraint struct {
	descriptorMutationOp
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
	descriptorMutationOp
	TableID descpb.ID
	Family  descpb.ColumnFamilyDescriptor
}
