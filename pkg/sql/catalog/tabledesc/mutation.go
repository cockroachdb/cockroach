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
)

var _ catalog.TableElementMaybeMutation = maybeMutation{}
var _ catalog.TableElementMaybeMutation = constraintToUpdate{}
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
	return mm.mutationState == descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY
}

// DeleteOnly returns true iff the table element is in a mutation in the
// delete-only state.
func (mm maybeMutation) DeleteOnly() bool {
	return mm.mutationState == descpb.DescriptorMutation_DELETE_ONLY
}

// Adding returns true iff the table element is in an add mutation.
func (mm maybeMutation) Adding() bool {
	return mm.mutationDirection == descpb.DescriptorMutation_ADD
}

// Dropped returns true iff the table element is in a drop mutation.
func (mm maybeMutation) Dropped() bool {
	return mm.mutationDirection == descpb.DescriptorMutation_DROP
}

// constraintToUpdate implements the catalog.ConstraintToUpdate interface.
type constraintToUpdate struct {
	maybeMutation
	desc *descpb.ConstraintToUpdate
}

// ConstraintToUpdateDesc returns the underlying protobuf descriptor.
func (c constraintToUpdate) ConstraintToUpdateDesc() *descpb.ConstraintToUpdate {
	return c.desc
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

// mutation implements the
type mutation struct {
	maybeMutation
	column          catalog.Column
	index           catalog.Index
	constraint      catalog.ConstraintToUpdate
	pkSwap          catalog.PrimaryKeySwap
	ccSwap          catalog.ComputedColumnSwap
	mvRefresh       catalog.MaterializedViewRefresh
	mutationOrdinal int
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

// AsConstraint returns the corresponding ConstraintToUpdate if the
// mutation is on a constraint, nil otherwise.
func (m mutation) AsConstraint() catalog.ConstraintToUpdate {
	return m.constraint
}

// AsPrimaryKeySwap returns the corresponding PrimaryKeySwap if the mutation
// is a primary key swap, nil otherwise.
func (m mutation) AsPrimaryKeySwap() catalog.PrimaryKeySwap {
	return m.pkSwap
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
	var constraints []constraintToUpdate
	var pkSwaps []primaryKeySwap
	var ccSwaps []computedColumnSwap
	var mvRefreshes []materializedViewRefresh
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
			constraints = append(constraints, constraintToUpdate{
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
