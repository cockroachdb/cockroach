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

//go:generate go run ./generate_visitor.go scop Mutation mutation.go mutation_visitor_generated.go

type mutationOp struct{ baseOp }

func (mutationOp) Type() Type { return MutationType }

// MakeAddedIndexDeleteOnly adds a non-existent primary index to the
// table.
type MakeAddedIndexDeleteOnly struct {
	mutationOp
	TableID descpb.ID

	// Index represents the index as it should appear in the mutation.
	Index descpb.IndexDescriptor
}

// MakeAddedIndexDeleteAndWriteOnly transitions an index addition mutation from
// DELETE_ONLY to DELETE_AND_WRITE_ONLY.
type MakeAddedIndexDeleteAndWriteOnly struct {
	mutationOp
	TableID descpb.ID
	IndexID descpb.IndexID
}

// MakeAddedPrimaryIndexPublic moves a new primary index from its mutation to
// public.
type MakeAddedPrimaryIndexPublic struct {
	mutationOp
	TableID descpb.ID
	Index   descpb.IndexDescriptor
}

// MakeDroppedPrimaryIndexDeleteAndWriteOnly moves a dropped primary index from
// public to DELETE_AND_WRITE_ONLY.
type MakeDroppedPrimaryIndexDeleteAndWriteOnly struct {
	mutationOp
	TableID descpb.ID

	// Index is the descriptor as it should be added as part of the mutation. The
	// primary index of a table has a slightly different encoding than that of
	// a secondary index. The value here sets it as it should be when adding
	// the mutation, including the stored columns.
	Index descpb.IndexDescriptor
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
	TableID    descpb.ID
	FamilyID   descpb.FamilyID
	FamilyName string
	Column     descpb.ColumnDescriptor
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
