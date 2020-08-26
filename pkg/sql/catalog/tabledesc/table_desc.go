// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package tabledesc provides concrete implementations of catalog.TableDesc.
package tabledesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var _ catalog.TableDescriptor = (*Immutable)(nil)
var _ catalog.TableDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)

// Immutable is a custom type for TableDescriptors
// It holds precomputed values and the underlying TableDescriptor
// should be const.
type Immutable struct {
	descpb.TableDescriptor

	// publicAndNonPublicCols is a list of public and non-public columns.
	// It is partitioned by the state of the column: public, write-only, delete-only
	publicAndNonPublicCols []descpb.ColumnDescriptor

	// publicAndNonPublicCols is a list of public and non-public indexes.
	// It is partitioned by the state of the index: public, write-only, delete-only
	publicAndNonPublicIndexes []descpb.IndexDescriptor

	writeOnlyColCount   int
	writeOnlyIndexCount int

	allChecks []descpb.TableDescriptor_CheckConstraint

	// partialIndexOrds contains the ordinal of each partial index.
	partialIndexOrds util.FastIntSet

	// ReadableColumns is a list of columns (including those undergoing a schema change)
	// which can be scanned. Columns in the process of a schema change
	// are all set to nullable while column backfilling is still in
	// progress, as mutation columns may have NULL values.
	ReadableColumns []descpb.ColumnDescriptor

	// columnsWithUDTs is a set of indexes into publicAndNonPublicCols containing
	// indexes of columns that contain user defined types.
	columnsWithUDTs []int

	postDeserializationChanges PostDeserializationTableDescriptorChanges

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool

	// TODO (lucy): populate these and use them
	// inboundFKs  []*ForeignKeyConstraint
	// outboundFKs []*ForeignKeyConstraint
}

// NameResolutionResult implements the tree.NameResolutionResult interface.
func (*Immutable) NameResolutionResult() {}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// DescriptorProto prepares desc for serialization.
func (desc *Immutable) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Table{Table: &desc.TableDescriptor},
	}
}

// GetPrimaryIndexID returns the ID of the primary index.
func (desc *Immutable) GetPrimaryIndexID() descpb.IndexID {
	return desc.PrimaryIndex.ID
}

// GetPublicNonPrimaryIndexes returns the public non-primary indexes of the descriptor.
func (desc *Immutable) GetPublicNonPrimaryIndexes() []descpb.IndexDescriptor {
	return desc.GetIndexes()
}

// IsTemporary returns true if this is a temporary table.
func (desc *Immutable) IsTemporary() bool {
	return desc.GetTemporary()
}

// GetPublicColumns return the public columns in the descriptor.
func (desc *Immutable) GetPublicColumns() []descpb.ColumnDescriptor {
	return desc.Columns
}

// GetColumnAtIdx returns the column at the specified index.
func (desc *Immutable) GetColumnAtIdx(idx int) *descpb.ColumnDescriptor {
	return &desc.Columns[idx]
}

// ImmutableCopy implements the MutableDescriptor interface.
func (desc *Mutable) ImmutableCopy() catalog.Descriptor {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	imm := NewImmutable(*protoutil.Clone(desc.TableDesc()).(*descpb.TableDescriptor))
	imm.isUncommittedVersion = desc.IsUncommittedVersion()
	return imm
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Mutable) IsUncommittedVersion() bool {
	return desc.IsNew() || desc.GetVersion() != desc.ClusterVersion.GetVersion()
}

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *Mutable) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}
