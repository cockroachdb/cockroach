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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

var _ catalog.TableElementMaybeMutation = maybeMutation{}
var _ catalog.TableElementMaybeMutation = constraint{}
var _ catalog.TableElementMaybeMutation = primaryKeySwap{}
var _ catalog.TableElementMaybeMutation = computedColumnSwap{}
var _ catalog.TableElementMaybeMutation = materializedViewRefresh{}
var _ catalog.Mutation = mutation{}

// maybeMutation implements the catalog.TableElementMaybeMutation interface
// and is embedded in table element interface implementations column and index
// as well as mutation.
type maybeMutation struct {
	mutationID         descpb.MutationID
	mutationDirection  descpb.DescriptorMutation_Direction
	mutationState      descpb.DescriptorMutation_State
	mutationIsRollback bool
}

// IsMutation returns true iff this table element is in a mutation.
func (mm maybeMutation) IsMutation() bool {
	return mm.mutationState != descpb.DescriptorMutation_UNKNOWN
}

// IsRollback returns true iff the table element is in a rollback mutation.
func (mm maybeMutation) IsRollback() bool {
	return mm.mutationIsRollback
}

// MutationID returns the table element's mutationID if applicable,
// descpb.InvalidMutationID otherwise.
func (mm maybeMutation) MutationID() descpb.MutationID {
	return mm.mutationID
}

// WriteAndDeleteOnly returns true iff the table element is in a mutation in
// the delete-and-write-only state.
func (mm maybeMutation) WriteAndDeleteOnly() bool {
	return mm.mutationState == descpb.DescriptorMutation_WRITE_ONLY
}

// DeleteOnly returns true iff the table element is in a mutation in the
// delete-only state.
func (mm maybeMutation) DeleteOnly() bool {
	return mm.mutationState == descpb.DescriptorMutation_DELETE_ONLY
}

// Backfilling returns true iff the table element is a mutation in the
// backfilling state.
func (mm maybeMutation) Backfilling() bool {
	return mm.mutationState == descpb.DescriptorMutation_BACKFILLING
}

// Merging returns true iff the table element is a mutation in the
// merging state.
func (mm maybeMutation) Merging() bool {
	return mm.mutationState == descpb.DescriptorMutation_MERGING
}

// Adding returns true iff the table element is in an add mutation.
func (mm maybeMutation) Adding() bool {
	return mm.mutationDirection == descpb.DescriptorMutation_ADD
}

// Dropped returns true iff the table element is in a drop mutation.
func (mm maybeMutation) Dropped() bool {
	return mm.mutationDirection == descpb.DescriptorMutation_DROP
}

// constraint implements the catalog.Constraint interface by wrapping
// the protobuf descriptor (*descpb.ConstraintToUpdate) along with
// some metadata if this constraint is a mutation.
// N.B. This struct is intended for non-index-backed-constraints.
type constraint struct {
	maybeMutation
	desc *descpb.ConstraintToUpdate
}

// GetName implements the catalog.Constraint interface.
func (c constraint) GetName() string {
	return c.desc.Name
}

// NotNullColumnID implements the catalog.Constraint interface.
func (c constraint) NotNullColumnID() descpb.ColumnID {
	return c.desc.NotNullColumn
}

// AsCheck implements the catalog.Constraint interface.
func (c constraint) AsCheck() *descpb.TableDescriptor_CheckConstraint {
	switch c.desc.ConstraintType {
	case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
		return &c.desc.Check
	}
	return nil
}

// AsForeignKey implements the catalog.Constraint interface.
func (c constraint) AsForeignKey() *descpb.ForeignKeyConstraint {
	switch c.desc.ConstraintType {
	case descpb.ConstraintToUpdate_FOREIGN_KEY:
		return &c.desc.ForeignKey
	}
	return nil
}

// AsUniqueWithoutIndex implements the catalog.Constraint interface.
func (c constraint) AsUniqueWithoutIndex() *descpb.UniqueWithoutIndexConstraint {
	switch c.desc.ConstraintType {
	case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
		return &c.desc.UniqueWithoutIndexConstraint
	}
	return nil
}

// AsPrimaryKey implements the catalog.Constraint interface.
func (c constraint) AsPrimaryKey() catalog.Index {
	return nil
}

// AsUnique implements the catalog.Constraint interface.
func (c constraint) AsUnique() catalog.Index {
	return nil
}

// String implements the catalog.Constraint interface.
func (c constraint) String() string {
	return fmt.Sprintf("%v", c.desc)
}

// GetConstraintID implements the catalog.Constraint interface.
func (c constraint) GetConstraintID() descpb.ConstraintID {
	switch c.desc.ConstraintType {
	case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
		return c.desc.Check.ConstraintID
	case descpb.ConstraintToUpdate_FOREIGN_KEY:
		return c.desc.ForeignKey.ConstraintID
	case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
		return c.desc.UniqueWithoutIndexConstraint.ConstraintID
	}
	panic(errors.AssertionFailedf("unknown constraint type %v", c.desc.ConstraintType))
}

// GetConstraintValidity implements the catalog.Constraint interface.
func (c constraint) GetConstraintValidity() descpb.ConstraintValidity {
	switch c.desc.ConstraintType {
	case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
		return c.desc.Check.Validity
	case descpb.ConstraintToUpdate_FOREIGN_KEY:
		return c.desc.ForeignKey.Validity
	case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
		return c.desc.UniqueWithoutIndexConstraint.Validity
	}
	panic(errors.AssertionFailedf("unknown constraint type %v", c.desc.ConstraintType))
}

// modifyRowLevelTTL implements the catalog.ModifyRowLevelTTL interface.
type modifyRowLevelTTL struct {
	maybeMutation
	desc *descpb.ModifyRowLevelTTL
}

// RowLevelTTL contains the row level TTL config to add or remove.
func (c modifyRowLevelTTL) RowLevelTTL() *catpb.RowLevelTTL {
	return c.desc.RowLevelTTL
}

// primaryKeySwap implements the catalog.PrimaryKeySwap interface.
type primaryKeySwap struct {
	maybeMutation
	desc *descpb.PrimaryKeySwap
}

// PrimaryKeySwapDesc returns the underlying protobuf descriptor.
func (c primaryKeySwap) PrimaryKeySwapDesc() *descpb.PrimaryKeySwap {
	return c.desc
}

// NumOldIndexes returns the number of old active indexes to swap out.
func (c primaryKeySwap) NumOldIndexes() int {
	return 1 + len(c.desc.OldIndexes)
}

// ForEachOldIndexIDs iterates through each of the old index IDs.
// iterutil.StopIteration is supported.
func (c primaryKeySwap) ForEachOldIndexIDs(fn func(id descpb.IndexID) error) error {
	return c.forEachIndexIDs(c.desc.OldPrimaryIndexId, c.desc.OldIndexes, fn)
}

// NumNewIndexes returns the number of new active indexes to swap in.
func (c primaryKeySwap) NumNewIndexes() int {
	return 1 + len(c.desc.NewIndexes)
}

// ForEachNewIndexIDs iterates through each of the new index IDs.
// iterutil.StopIteration is supported.
func (c primaryKeySwap) ForEachNewIndexIDs(fn func(id descpb.IndexID) error) error {
	return c.forEachIndexIDs(c.desc.NewPrimaryIndexId, c.desc.NewIndexes, fn)
}

func (c primaryKeySwap) forEachIndexIDs(
	pkID descpb.IndexID, secIDs []descpb.IndexID, fn func(id descpb.IndexID) error,
) error {
	err := fn(pkID)
	if err != nil {
		return iterutil.Map(err)
	}
	for _, id := range secIDs {
		err = fn(id)
		if err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// HasLocalityConfig returns true iff the locality config is swapped also.
func (c primaryKeySwap) HasLocalityConfig() bool {
	return c.desc.LocalityConfigSwap != nil
}

// LocalityConfigSwap returns the locality config swap, if there is one.
func (c primaryKeySwap) LocalityConfigSwap() descpb.PrimaryKeySwap_LocalityConfigSwap {
	return *c.desc.LocalityConfigSwap
}

// computedColumnSwap implements the catalog.ComputedColumnSwap interface.
type computedColumnSwap struct {
	maybeMutation
	desc *descpb.ComputedColumnSwap
}

// ComputedColumnSwapDesc returns the underlying protobuf descriptor.
func (c computedColumnSwap) ComputedColumnSwapDesc() *descpb.ComputedColumnSwap {
	return c.desc
}

// materializedViewRefresh implements the catalog.MaterializedViewRefresh interface.
type materializedViewRefresh struct {
	maybeMutation
	desc *descpb.MaterializedViewRefresh
}

// MaterializedViewRefreshDesc returns the underlying protobuf descriptor.
func (c materializedViewRefresh) MaterializedViewRefreshDesc() *descpb.MaterializedViewRefresh {
	return c.desc
}

// ShouldBackfill returns true iff the query should be backfilled into the
// indexes.
func (c materializedViewRefresh) ShouldBackfill() bool {
	return c.desc.ShouldBackfill
}

// AsOf returns the timestamp at which the query should be run.
func (c materializedViewRefresh) AsOf() hlc.Timestamp {
	return c.desc.AsOf
}

// ForEachIndexID iterates through each of the index IDs.
// iterutil.StopIteration is supported.
func (c materializedViewRefresh) ForEachIndexID(fn func(id descpb.IndexID) error) error {
	err := fn(c.desc.NewPrimaryIndex.ID)

	if err != nil {
		return iterutil.Map(err)
	}
	for i := range c.desc.NewIndexes {
		err = fn(c.desc.NewIndexes[i].ID)
		if err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// TableWithNewIndexes returns a new TableDescriptor based on the old one
// but with the refreshed indexes put in.
func (c materializedViewRefresh) TableWithNewIndexes(
	tbl catalog.TableDescriptor,
) catalog.TableDescriptor {
	deepCopy := NewBuilder(tbl.TableDesc()).BuildCreatedMutableTable().TableDesc()
	deepCopy.PrimaryIndex = c.desc.NewPrimaryIndex
	deepCopy.Indexes = c.desc.NewIndexes
	return NewBuilder(deepCopy).BuildImmutableTable()
}

// mutation implements the
type mutation struct {
	maybeMutation
	column            catalog.Column
	index             catalog.Index
	constraint        catalog.Constraint
	pkSwap            catalog.PrimaryKeySwap
	ccSwap            catalog.ComputedColumnSwap
	mvRefresh         catalog.MaterializedViewRefresh
	modifyRowLevelTTL catalog.ModifyRowLevelTTL
	mutationOrdinal   int
}

// AsColumn returns the corresponding Column if the mutation is on a column,
// nil otherwise.
func (m mutation) AsColumn() catalog.Column {
	return m.column
}

// AsIndex returns the corresponding Index if the mutation is on an index,
// nil otherwise.
func (m mutation) AsIndex() catalog.Index {
	return m.index
}

// AsConstraint returns the corresponding Constraint if the
// mutation is on a constraint, nil otherwise.
func (m mutation) AsConstraint() catalog.Constraint {
	return m.constraint
}

// AsPrimaryKeySwap returns the corresponding PrimaryKeySwap if the mutation
// is a primary key swap, nil otherwise.
func (m mutation) AsPrimaryKeySwap() catalog.PrimaryKeySwap {
	return m.pkSwap
}

// AsModifyRowLevelTTL returns the corresponding ModifyRowLevelTTL if the
// mutation is a computed column swap, nil otherwise.
func (m mutation) AsModifyRowLevelTTL() catalog.ModifyRowLevelTTL {
	return m.modifyRowLevelTTL
}

// AsComputedColumnSwap returns the corresponding ComputedColumnSwap if the
// mutation is a computed column swap, nil otherwise.
func (m mutation) AsComputedColumnSwap() catalog.ComputedColumnSwap {
	return m.ccSwap
}

// AsMaterializedViewRefresh returns the corresponding MaterializedViewRefresh
// if the mutation is a materialized view refresh, nil otherwise.
func (m mutation) AsMaterializedViewRefresh() catalog.MaterializedViewRefresh {
	return m.mvRefresh
}

// MutationOrdinal returns the ordinal of the mutation in the underlying table
// descriptor's Mutations slice.
func (m mutation) MutationOrdinal() int {
	return m.mutationOrdinal
}

// mutationCache contains precomputed slices of catalog.Mutation interfaces.
type mutationCache struct {
	all     []catalog.Mutation
	columns []catalog.Mutation
	indexes []catalog.Mutation
}

// newMutationCache returns a fresh fully-populated mutationCache struct for the
// TableDescriptor.
func newMutationCache(desc *descpb.TableDescriptor) *mutationCache {
	c := mutationCache{}
	if len(desc.Mutations) == 0 {
		return &c
	}
	// Build slices of structs to back the interfaces in c.all.
	// This is better than allocating memory once per struct.
	backingStructs := make([]mutation, len(desc.Mutations))
	var columns []column
	var indexes []index
	var constraints []constraint
	var pkSwaps []primaryKeySwap
	var ccSwaps []computedColumnSwap
	var mvRefreshes []materializedViewRefresh
	var modifyRowLevelTTLs []modifyRowLevelTTL
	for i, m := range desc.Mutations {
		mm := maybeMutation{
			mutationID:         m.MutationID,
			mutationDirection:  m.Direction,
			mutationState:      m.State,
			mutationIsRollback: m.Rollback,
		}
		backingStructs[i] = mutation{
			maybeMutation:   mm,
			mutationOrdinal: i,
		}
		if pb := m.GetColumn(); pb != nil {
			columns = append(columns, column{
				maybeMutation: mm,
				desc:          pb,
				ordinal:       len(desc.Columns) + len(columns),
			})
			backingStructs[i].column = &columns[len(columns)-1]
		} else if pb := m.GetIndex(); pb != nil {
			indexes = append(indexes, index{
				maybeMutation: mm,
				desc:          pb,
				ordinal:       1 + len(desc.Indexes) + len(indexes),
			})
			backingStructs[i].index = &indexes[len(indexes)-1]
		} else if pb := m.GetConstraint(); pb != nil {
			constraints = append(constraints, constraint{
				maybeMutation: mm,
				desc:          pb,
			})
			backingStructs[i].constraint = &constraints[len(constraints)-1]
		} else if pb := m.GetPrimaryKeySwap(); pb != nil {
			pkSwaps = append(pkSwaps, primaryKeySwap{
				maybeMutation: mm,
				desc:          pb,
			})
			backingStructs[i].pkSwap = &pkSwaps[len(pkSwaps)-1]
		} else if pb := m.GetComputedColumnSwap(); pb != nil {
			ccSwaps = append(ccSwaps, computedColumnSwap{
				maybeMutation: mm,
				desc:          pb,
			})
			backingStructs[i].ccSwap = &ccSwaps[len(ccSwaps)-1]
		} else if pb := m.GetMaterializedViewRefresh(); pb != nil {
			mvRefreshes = append(mvRefreshes, materializedViewRefresh{
				maybeMutation: mm,
				desc:          pb,
			})
			backingStructs[i].mvRefresh = &mvRefreshes[len(mvRefreshes)-1]
		} else if pb := m.GetModifyRowLevelTTL(); pb != nil {
			modifyRowLevelTTLs = append(modifyRowLevelTTLs, modifyRowLevelTTL{
				maybeMutation: mm,
				desc:          pb,
			})
			backingStructs[i].modifyRowLevelTTL = &modifyRowLevelTTLs[len(modifyRowLevelTTLs)-1]
		}
	}
	// Populate the c.all slice with Mutation interfaces.
	c.all = make([]catalog.Mutation, len(backingStructs))
	for i := range backingStructs {
		c.all[i] = &backingStructs[i]
	}
	// Populate the remaining fields in c.
	// Use nil instead of empty slices.
	if len(columns) > 0 {
		c.columns = make([]catalog.Mutation, 0, len(columns))
	}
	if len(indexes) > 0 {
		c.indexes = make([]catalog.Mutation, 0, len(indexes))
	}
	for _, m := range c.all {
		if col := m.AsColumn(); col != nil {
			c.columns = append(c.columns, m)
		} else if idx := m.AsIndex(); idx != nil {
			c.indexes = append(c.indexes, m)
		}
	}
	return &c
}
