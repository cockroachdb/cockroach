// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var _ TableDescriptor = (*ImmutableTableDescriptor)(nil)
var _ TableDescriptor = (*MutableTableDescriptor)(nil)

// IndexOpts configures the behavior of TableDescriptor.ForeachIndex.
type IndexOpts struct {
	// NonPhysicalPrimaryIndex should be included.
	NonPhysicalPrimaryIndex bool
	// DropMutations should be included.
	DropMutations bool
	// AddMutations should be included.
	AddMutations bool
}

// TableDescriptor is an interface around the table descriptor types.
//
// TODO(ajwerner): This interface likely belongs in a catalog/tabledesc package
// or perhaps in the catalog package directly. It's not clear how expansive this
// interface should be. Perhaps very.
type TableDescriptor interface {
	Descriptor

	TableDesc() *descpb.TableDescriptor

	GetState() descpb.TableDescriptor_State
	GetSequenceOpts() *descpb.TableDescriptor_SequenceOpts
	GetViewQuery() string
	GetLease() *descpb.TableDescriptor_SchemaChangeLease
	GetDropTime() int64
	GetFormatVersion() descpb.FormatVersion

	GetPrimaryIndexID() descpb.IndexID
	GetPrimaryIndex() *descpb.IndexDescriptor
	PrimaryIndexSpan(codec keys.SQLCodec) roachpb.Span
	GetPublicNonPrimaryIndexes() []descpb.IndexDescriptor
	ForeachIndex(opts IndexOpts, f func(idxDesc *descpb.IndexDescriptor, isPrimary bool) error) error
	AllNonDropIndexes() []*descpb.IndexDescriptor
	ForeachNonDropIndex(f func(idxDesc *descpb.IndexDescriptor) error) error
	IndexSpan(codec keys.SQLCodec, id descpb.IndexID) roachpb.Span
	FindIndexByID(id descpb.IndexID) (*descpb.IndexDescriptor, error)
	FindIndexByName(name string) (_ *descpb.IndexDescriptor, dropped bool, _ error)
	FindIndexesWithPartition(name string) []*descpb.IndexDescriptor
	GetIndexMutationCapabilities(id descpb.IndexID) (isMutation, isWriteOnly bool)
	KeysPerRow(id descpb.IndexID) (int, error)
	PartialIndexOrds() util.FastIntSet
	DeletableIndexes() []descpb.IndexDescriptor

	HasPrimaryKey() bool
	PrimaryKeyString() string

	GetPublicColumns() []descpb.ColumnDescriptor
	ForeachPublicColumn(f func(col *descpb.ColumnDescriptor) error) error
	NamesForColumnIDs(ids descpb.ColumnIDs) ([]string, error)
	FindColumnByName(name tree.Name) (*descpb.ColumnDescriptor, bool, error)
	FindActiveColumnByID(id descpb.ColumnID) (*descpb.ColumnDescriptor, error)
	FindColumnByID(id descpb.ColumnID) (*descpb.ColumnDescriptor, error)
	ColumnIdxMap() map[descpb.ColumnID]int
	GetColumnAtIdx(idx int) *descpb.ColumnDescriptor
	AllNonDropColumns() []descpb.ColumnDescriptor
	VisibleColumns() []descpb.ColumnDescriptor
	ColumnsWithMutations(includeMutations bool) []descpb.ColumnDescriptor
	ColumnIdxMapWithMutations(includeMutations bool) map[descpb.ColumnID]int
	DeletableColumns() []descpb.ColumnDescriptor

	GetFamilies() []descpb.ColumnFamilyDescriptor
	NumFamilies() int
	FindFamilyByID(id descpb.FamilyID) (*descpb.ColumnFamilyDescriptor, error)
	ForeachFamily(f func(family *descpb.ColumnFamilyDescriptor) error) error

	IsTable() bool
	IsView() bool
	MaterializedView() bool
	IsSequence() bool
	IsTemporary() bool
	IsVirtualTable() bool
	IsPhysicalTable() bool
	IsInterleaved() bool

	GetMutationJobs() []descpb.TableDescriptor_MutationJob

	GetReplacementOf() descpb.TableDescriptor_Replacement
	GetAllReferencedTypeIDs(
		getType func(descpb.ID) (TypeDescriptor, error),
	) (descpb.IDs, error)

	Validate(ctx context.Context, pg protoGetter, codec keys.SQLCodec) error

	ForeachDependedOnBy(f func(dep *descpb.TableDescriptor_Reference) error) error
	GetDependsOn() []descpb.ID
	GetConstraintInfoWithLookup(fn TableLookupFn) (map[string]descpb.ConstraintDetail, error)
	ForeachOutboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error
	GetChecks() []*descpb.TableDescriptor_CheckConstraint
	AllActiveAndInactiveChecks() []*descpb.TableDescriptor_CheckConstraint
	ActiveChecks() []descpb.TableDescriptor_CheckConstraint
	ForeachInboundFK(f func(fk *descpb.ForeignKeyConstraint) error) error
}

// Immutable implements the MutableDescriptor interface.
func (desc *MutableTableDescriptor) Immutable() Descriptor {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	return NewImmutableTableDescriptor(*protoutil.Clone(desc.TableDesc()).(*descpb.TableDescriptor))
}

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *MutableTableDescriptor) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}
