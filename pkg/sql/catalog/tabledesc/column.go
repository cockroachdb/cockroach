// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

var _ catalog.Column = (*column)(nil)

// column implements the catalog.Column interface by wrapping the protobuf
// column descriptor along with some metadata from its parent table
// descriptor.
type column struct {
	maybeMutation
	desc    *descpb.ColumnDescriptor
	ordinal int
}

// ColumnDesc returns the underlying protobuf descriptor.
// Ideally, this method should be called as rarely as possible.
func (w column) ColumnDesc() *descpb.ColumnDescriptor {
	return w.desc
}

// ColumnDescDeepCopy returns a deep copy of the underlying protobuf descriptor.
func (w column) ColumnDescDeepCopy() descpb.ColumnDescriptor {
	return *protoutil.Clone(w.desc).(*descpb.ColumnDescriptor)
}

// DeepCopy returns a deep copy of the receiver.
func (w column) DeepCopy() catalog.Column {
	desc := w.ColumnDescDeepCopy()
	return &column{
		maybeMutation: w.maybeMutation,
		desc:          &desc,
		ordinal:       w.ordinal,
	}
}

// Ordinal returns the ordinal of the column in its parent TableDescriptor.
// The ordinal is defined as follows:
// - [:len(desc.Columns)] is the range of public columns,
// - [len(desc.Columns):] is the range of non-public columns.
func (w column) Ordinal() int {
	return w.ordinal
}

// Public returns true iff the column is active, i.e. readable.
func (w column) Public() bool {
	return !w.IsMutation() && !w.IsSystemColumn()
}

// GetID returns the column ID.
func (w column) GetID() descpb.ColumnID {
	return w.desc.ID
}

// GetName returns the column name as a string.
func (w column) GetName() string {
	return w.desc.Name
}

// ColName returns the column name as a tree.Name.
func (w column) ColName() tree.Name {
	return w.desc.ColName()
}

// HasType returns true iff the column type is set.
func (w column) HasType() bool {
	return w.desc.Type != nil
}

// GetType returns the column type.
func (w column) GetType() *types.T {
	return w.desc.Type
}

// IsNullable returns true iff the column allows NULL values.
func (w column) IsNullable() bool {
	return w.desc.Nullable
}

// HasDefault returns true iff the column has a default expression set.
func (w column) HasDefault() bool {
	return w.desc.HasDefault()
}

// GetDefaultExpr returns the column default expression if it exists,
// empty string otherwise.
func (w column) GetDefaultExpr() string {
	if !w.HasDefault() {
		return ""
	}
	return *w.desc.DefaultExpr
}

// IsComputed returns true iff the column is a computed column.
func (w column) IsComputed() bool {
	return w.desc.IsComputed()
}

// GetComputeExpr returns the column computed expression if it exists,
// empty string otherwise.
func (w column) GetComputeExpr() string {
	if !w.IsComputed() {
		return ""
	}
	return *w.desc.ComputeExpr
}

// IsHidden returns true iff the column is not visible.
func (w column) IsHidden() bool {
	return w.desc.Hidden
}

// IsInaccessible returns true iff the column is inaccessible.
func (w column) IsInaccessible() bool {
	return w.desc.Inaccessible
}

// IsExpressionIndexColumn returns true iff the column is an an inaccessible
// virtual computed column that represents an expression in an expression index.
func (w column) IsExpressionIndexColumn() bool {
	return w.IsInaccessible() && w.IsVirtual()
}

// NumUsesSequences returns the number of sequences used by this column.
func (w column) NumUsesSequences() int {
	return len(w.desc.UsesSequenceIds)
}

// GetUsesSequenceID returns the ID of a sequence used by this column.
func (w column) GetUsesSequenceID(usesSequenceOrdinal int) descpb.ID {
	return w.desc.UsesSequenceIds[usesSequenceOrdinal]
}

// NumOwnsSequences returns the number of sequences owned by this column.
func (w column) NumOwnsSequences() int {
	return len(w.desc.OwnsSequenceIds)
}

// GetOwnsSequenceID returns the ID of a sequence owned by this column.
func (w column) GetOwnsSequenceID(ownsSequenceOrdinal int) descpb.ID {
	return w.desc.OwnsSequenceIds[ownsSequenceOrdinal]
}

// IsVirtual returns true iff the column is a virtual column.
func (w column) IsVirtual() bool {
	return w.desc.Virtual
}

// CheckCanBeInboundFKRef returns whether the given column can be on the
// referenced (target) side of a foreign key relation.
func (w column) CheckCanBeInboundFKRef() error {
	return w.desc.CheckCanBeInboundFKRef()
}

// CheckCanBeOutboundFKRef returns whether the given column can be on the
// referencing (origin) side of a foreign key relation.
func (w column) CheckCanBeOutboundFKRef() error {
	return w.desc.CheckCanBeOutboundFKRef()
}

// GetPGAttributeNum returns the PGAttributeNum of the column descriptor
// if the PGAttributeNum is set (non-zero). Returns the ID of the
// column descriptor if the PGAttributeNum is not set.
func (w column) GetPGAttributeNum() uint32 {
	return w.desc.GetPGAttributeNum()
}

// IsSystemColumn returns true iff the column is a system column.
func (w column) IsSystemColumn() bool {
	return w.desc.SystemColumnKind != descpb.SystemColumnKind_NONE
}

// columnCache contains precomputed slices of catalog.Column interfaces.
type columnCache struct {
	all        []catalog.Column
	public     []catalog.Column
	writable   []catalog.Column
	deletable  []catalog.Column
	nonDrop    []catalog.Column
	visible    []catalog.Column
	accessible []catalog.Column
	readable   []catalog.Column
	withUDTs   []catalog.Column
	system     []catalog.Column
}

// newColumnCache returns a fresh fully-populated columnCache struct for the
// TableDescriptor.
func newColumnCache(desc *descpb.TableDescriptor, mutations *mutationCache) *columnCache {
	c := columnCache{}
	// Build a slice of structs to back the public and system interfaces in c.all.
	// This is better than allocating memory once per struct.
	numPublic := len(desc.Columns)
	backingStructs := make([]column, numPublic, numPublic+len(colinfo.AllSystemColumnDescs))
	for i := range desc.Columns {
		backingStructs[i] = column{desc: &desc.Columns[i], ordinal: i}
	}
	numMutations := len(mutations.columns)
	numDeletable := numPublic + numMutations
	for i := range colinfo.AllSystemColumnDescs {
		col := column{
			desc:    &colinfo.AllSystemColumnDescs[i],
			ordinal: numDeletable + i,
		}
		backingStructs = append(backingStructs, col)
	}
	// Populate the c.all slice with Column interfaces.
	c.all = make([]catalog.Column, 0, numDeletable+len(colinfo.AllSystemColumnDescs))
	for i := range backingStructs[:numPublic] {
		c.all = append(c.all, &backingStructs[i])
	}
	for _, m := range mutations.columns {
		c.all = append(c.all, m.AsColumn())
	}
	for i := range backingStructs[numPublic:] {
		c.all = append(c.all, &backingStructs[numPublic+i])
	}
	// Populate the remaining fields.
	c.deletable = c.all[:numDeletable]
	c.system = c.all[numDeletable:]
	c.public = c.all[:numPublic]
	if numMutations == 0 {
		c.readable = c.public
		c.writable = c.public
		c.nonDrop = c.public
	} else {
		readableDescs := make([]descpb.ColumnDescriptor, 0, numMutations)
		readableBackingStructs := make([]column, 0, numMutations)
		for _, col := range c.deletable {
			if !col.DeleteOnly() {
				lazyAllocAppendColumn(&c.writable, col, numDeletable)
			}
			if !col.Dropped() {
				lazyAllocAppendColumn(&c.nonDrop, col, numDeletable)
			}
			if !col.Public() && !col.IsNullable() {
				j := len(readableDescs)
				readableDescs = append(readableDescs, *col.ColumnDesc())
				readableDescs[j].Nullable = true
				readableBackingStructs = append(readableBackingStructs, *col.(*column))
				readableBackingStructs[j].desc = &readableDescs[j]
				col = &readableBackingStructs[j]
			}
			lazyAllocAppendColumn(&c.readable, col, numDeletable)
		}
	}
	for _, col := range c.deletable {
		if col.Public() && !col.IsHidden() && !col.IsInaccessible() {
			lazyAllocAppendColumn(&c.visible, col, numPublic)
		}
		if col.Public() && !col.IsInaccessible() {
			lazyAllocAppendColumn(&c.accessible, col, numPublic)
		}
		if col.HasType() && col.GetType().UserDefined() {
			lazyAllocAppendColumn(&c.withUDTs, col, numDeletable)
		}
	}
	return &c
}

func lazyAllocAppendColumn(slice *[]catalog.Column, col catalog.Column, cap int) {
	if *slice == nil {
		*slice = make([]catalog.Column, 0, cap)
	}
	*slice = append(*slice, col)
}
