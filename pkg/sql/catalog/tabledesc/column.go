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
	desc              *descpb.ColumnDescriptor
	ordinal           int
	mutationID        descpb.MutationID
	mutationDirection descpb.DescriptorMutation_Direction
	mutationState     descpb.DescriptorMutation_State
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

// Ordinal returns the ordinal of the column in its parent TableDescriptor.
// The ordinal is defined as follows:
// - [:len(desc.Columns)] is the range of public columns,
// - [len(desc.Columns):] is the range of non-public columns.
func (w column) Ordinal() int {
	return w.ordinal
}

// Public returns true iff the column is active, i.e. readable.
func (w column) Public() bool {
	return w.mutationState == descpb.DescriptorMutation_UNKNOWN
}

// Adding returns true iff the column is an add mutation in the table
//descriptor.
func (w column) Adding() bool {
	return w.mutationDirection == descpb.DescriptorMutation_ADD
}

// Dropped returns true iff the column is a drop mutation in the table
// descriptor.
func (w column) Dropped() bool {
	return w.mutationDirection == descpb.DescriptorMutation_DROP
}

// WriteAndDeleteOnly returns true iff the column is a mutation in the
// delete-and-write-only state.
func (w column) WriteAndDeleteOnly() bool {
	return w.mutationState == descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
}

// DeleteOnly returns true iff the column is a mutation in the delete-only
// state.
func (w column) DeleteOnly() bool {
	return w.mutationState == descpb.DescriptorMutation_DELETE_ONLY
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

// columnCache contains precomputed slices of catalog.Column interfaces.
type columnCache struct {
	all      []catalog.Column
	public   []catalog.Column
	writable []catalog.Column
	nonDrop  []catalog.Column
	visible  []catalog.Column
	readable []catalog.Column
	withUDTs []catalog.Column
}

// newColumnCache returns a fresh fully-populated columnCache struct for the
// TableDescriptor.
func newColumnCache(desc *descpb.TableDescriptor) *columnCache {
	c := columnCache{}
	// Build a slice of structs to back the interfaces in c.all.
	// This is better than allocating memory once per struct.
	backingStructs := make([]column, len(desc.Columns), len(desc.Columns)+len(desc.Mutations))
	for i := range desc.Columns {
		backingStructs[i] = column{desc: &desc.Columns[i], ordinal: i}
	}
	for _, m := range desc.Mutations {
		if colDesc := m.GetColumn(); colDesc != nil {
			col := column{
				desc:              colDesc,
				ordinal:           len(backingStructs),
				mutationID:        m.MutationID,
				mutationState:     m.State,
				mutationDirection: m.Direction,
			}
			backingStructs = append(backingStructs, col)
		}
	}

	// Populate the c.all slice with column interfaces.
	c.all = make([]catalog.Column, len(backingStructs))
	for i := range backingStructs {
		c.all[i] = &backingStructs[i]
	}
	// Populate the remaining fields.
	c.public = c.all[:len(desc.Columns)]
	if len(c.public) == len(c.all) {
		c.readable = c.public
		c.writable = c.public
		c.nonDrop = c.public
	} else {
		readableDescs := make([]descpb.ColumnDescriptor, 0, len(c.all)-len(c.public))
		readableBackingStructs := make([]column, 0, cap(readableDescs))
		for i, col := range c.all {
			if !col.DeleteOnly() {
				lazyAllocAppendColumn(&c.writable, col, len(c.all))
			}
			if !col.Dropped() {
				lazyAllocAppendColumn(&c.nonDrop, col, len(c.all))
			}
			if !col.Public() && !col.IsNullable() {
				j := len(readableDescs)
				readableDescs = append(readableDescs, *col.ColumnDesc())
				readableDescs[j].Nullable = true
				readableBackingStructs = append(readableBackingStructs, backingStructs[i])
				readableBackingStructs[j].desc = &readableDescs[j]
				col = &readableBackingStructs[j]
			}
			lazyAllocAppendColumn(&c.readable, col, len(c.all))
		}
	}
	for _, col := range c.all {
		if col.Public() && !col.IsHidden() {
			lazyAllocAppendColumn(&c.visible, col, len(c.public))
		}
		if col.HasType() && col.GetType().UserDefined() {
			lazyAllocAppendColumn(&c.withUDTs, col, len(c.all))
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
