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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var _ TableDescriptor = (*ImmutableTableDescriptor)(nil)
var _ TableDescriptor = (*MutableTableDescriptor)(nil)

// TableDescriptor is an interface around the table descriptor types.
//
// TODO(ajwerner): This interface likely belongs in a catalog/tabledesc package
// or perhaps in the catalog package directly. It's not clear how expansive this
// interface should be. Perhaps very.
type TableDescriptor interface {
	Descriptor

	TableDesc() *descpb.TableDescriptor

	GetState() descpb.TableDescriptor_State

	HasPrimaryKey() bool
	GetPrimaryIndex() *descpb.IndexDescriptor
	GetIndexes() []descpb.IndexDescriptor
	ForeachNonDropIndex(f func(idxDesc *descpb.IndexDescriptor) error) error
	IndexSpan(codec keys.SQLCodec, id descpb.IndexID) roachpb.Span
	IsInterleaved() bool
	FindIndexByID(id descpb.IndexID) (*descpb.IndexDescriptor, error)
	FindIndexByName(name string) (_ *descpb.IndexDescriptor, dropped bool, _ error)
	FindIndexesWithPartition(name string) []*descpb.IndexDescriptor
	AllNonDropIndexes() []*descpb.IndexDescriptor

	FindColumnByName(name tree.Name) (*descpb.ColumnDescriptor, bool, error)
	FindActiveColumnByID(id descpb.ColumnID) (*descpb.ColumnDescriptor, error)
	FindColumnByID(id descpb.ColumnID) (*descpb.ColumnDescriptor, error)
	NamesForColumnIDs(ids descpb.ColumnIDs) ([]string, error)

	ColumnIdxMap() map[descpb.ColumnID]int
	GetPublicColumns() []descpb.ColumnDescriptor
	GetColumnAtIdx(idx int) *descpb.ColumnDescriptor
	AllNonDropColumns() []descpb.ColumnDescriptor

	GetFamilies() []descpb.ColumnFamilyDescriptor

	IsTable() bool
	IsView() bool
	MaterializedView() bool
	IsSequence() bool

	GetMutationJobs() []descpb.TableDescriptor_MutationJob
	GetDependedOnBy() []descpb.TableDescriptor_Reference

	GetReplacementOf() descpb.TableDescriptor_Replacement
	GetAllReferencedTypeIDs(
		getType func(descpb.ID) (TypeDescriptor, error),
	) (descpb.IDs, error)

	Validate(ctx context.Context, txn *kv.Txn, codec keys.SQLCodec) error
	IsVirtualTable() bool
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
