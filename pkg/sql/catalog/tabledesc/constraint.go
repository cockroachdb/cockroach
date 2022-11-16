// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

type constraintBase struct {
	maybeMutation
}

// AsCheck implements the catalog.Constraint interface.
func (c constraintBase) AsCheck() catalog.CheckConstraint {
	return nil
}

// AsForeignKey implements the catalog.Constraint interface.
func (c constraintBase) AsForeignKey() catalog.ForeignKeyConstraint {
	return nil
}

// AsUniqueWithoutIndex implements the catalog.Constraint interface.
func (c constraintBase) AsUniqueWithoutIndex() catalog.UniqueWithoutIndexConstraint {
	return nil
}

// AsUnique implements the catalog.Constraint interface.
func (c constraintBase) AsUnique() catalog.UniqueWithIndexConstraint {
	return nil
}

type checkConstraint struct {
	constraintBase
	desc            *descpb.TableDescriptor_CheckConstraint
	notNullColumnID descpb.ColumnID
}

var _ catalog.CheckConstraint = (*checkConstraint)(nil)

// CheckDesc implements the catalog.CheckConstraint interface.
func (c checkConstraint) CheckDesc() *descpb.TableDescriptor_CheckConstraint {
	return c.desc
}

// Expr implements the catalog.CheckConstraint interface.
func (c checkConstraint) Expr() string {
	return c.desc.Expr
}

// NumReferencedColumns implements the catalog.CheckConstraint interface.
func (c checkConstraint) NumReferencedColumns() int {
	if c.notNullColumnID != 0 {
		return 1
	}
	return len(c.desc.ColumnIDs)
}

// GetReferencedColumnID implements the catalog.CheckConstraint
// interface.
func (c checkConstraint) GetReferencedColumnID(columnOrdinal int) descpb.ColumnID {
	if c.notNullColumnID != 0 {
		if columnOrdinal != 0 {
			return 0
		}
		return c.notNullColumnID
	}
	if columnOrdinal < 0 || columnOrdinal >= len(c.desc.ColumnIDs) {
		return 0
	}
	return c.desc.ColumnIDs[columnOrdinal]
}

// IsNotNullColumnConstraint implements the catalog.CheckConstraint interface.
func (c checkConstraint) IsNotNullColumnConstraint() bool {
	return c.notNullColumnID != 0
}

// GetConstraintID implements the catalog.Constraint interface.
func (c checkConstraint) GetConstraintID() descpb.ConstraintID {
	return c.desc.ConstraintID
}

// GetConstraintValidity implements the catalog.Constraint interface.
func (c checkConstraint) GetConstraintValidity() descpb.ConstraintValidity {
	return c.desc.Validity
}

// GetName implements the catalog.Constraint interface.
func (c checkConstraint) GetName() string {
	return c.desc.Name
}

// AsCheck implements the catalog.Constraint interface.
func (c checkConstraint) AsCheck() catalog.CheckConstraint {
	return c
}

// String implements the catalog.Constraint interface.
func (c checkConstraint) String() string {
	return fmt.Sprintf("%+v", c.desc)
}

type uniqueWithoutIndexConstraint struct {
	constraintBase
	desc *descpb.UniqueWithoutIndexConstraint
}

var _ catalog.UniqueWithoutIndexConstraint = (*uniqueWithoutIndexConstraint)(nil)

// UniqueWithoutIndexDesc implements the catalog.UniqueWithoutIndexConstraint
// interface.
func (c uniqueWithoutIndexConstraint) UniqueWithoutIndexDesc() *descpb.UniqueWithoutIndexConstraint {
	return c.desc
}

// GetConstraintID implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) GetConstraintID() descpb.ConstraintID {
	return c.desc.ConstraintID
}

// GetConstraintValidity implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) GetConstraintValidity() descpb.ConstraintValidity {
	return c.desc.Validity
}

// GetName implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) GetName() string {
	return c.desc.Name
}

// AsUniqueWithoutIndex implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) AsUniqueWithoutIndex() catalog.UniqueWithoutIndexConstraint {
	return c
}

// String implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) String() string {
	return fmt.Sprintf("%+v", c.desc)
}

type foreignKeyConstraint struct {
	constraintBase
	desc *descpb.ForeignKeyConstraint
}

var _ catalog.ForeignKeyConstraint = (*foreignKeyConstraint)(nil)

// ForeignKeyDesc implements the catalog.ForeignKeyConstraint interface.
func (c foreignKeyConstraint) ForeignKeyDesc() *descpb.ForeignKeyConstraint {
	return c.desc
}

// GetReferencedTableID implements the catalog.ForeignKeyConstraint
// interface.
func (c foreignKeyConstraint) GetReferencedTableID() descpb.ID {
	return c.desc.ReferencedTableID
}

// NumReferencedColumns implements the catalog.ForeignKeyConstraint
// interface.
func (c foreignKeyConstraint) NumReferencedColumns() int {
	return len(c.desc.ReferencedColumnIDs)
}

// GetReferencedColumnID implements the catalog.ForeignKeyConstraint
// interface.
func (c foreignKeyConstraint) GetReferencedColumnID(columnOrdinal int) descpb.ColumnID {
	if columnOrdinal < 0 || columnOrdinal >= len(c.desc.ReferencedColumnIDs) {
		return 0
	}
	return c.desc.ReferencedColumnIDs[columnOrdinal]
}

// GetConstraintID implements the catalog.Constraint interface.
func (c foreignKeyConstraint) GetConstraintID() descpb.ConstraintID {
	return c.desc.ConstraintID
}

// GetConstraintValidity implements the catalog.Constraint interface.
func (c foreignKeyConstraint) GetConstraintValidity() descpb.ConstraintValidity {
	return c.desc.Validity
}

// GetName implements the catalog.Constraint interface.
func (c foreignKeyConstraint) GetName() string {
	return c.desc.Name
}

// AsForeignKey implements the catalog.Constraint interface.
func (c foreignKeyConstraint) AsForeignKey() catalog.ForeignKeyConstraint {
	return c
}

// String implements the catalog.Constraint interface.
func (c foreignKeyConstraint) String() string {
	return fmt.Sprintf("%+v", c.desc)
}

// constraintCache contains precomputed slices of constraints, categorized by kind and validity.
// A constraint is considered
// - active if its validity is VALIDATED;
// - inactive if its validity is VALIDATING or UNVALIDATED;
// - dropping if its validity is DROPPING;
type constraintCache struct {
	all                  []catalog.Constraint
	allActiveAndInactive []catalog.Constraint
	allActive            []catalog.Constraint

	allChecks                  []catalog.Constraint
	allActiveAndInactiveChecks []catalog.Constraint
	allActiveChecks            []catalog.Constraint

	allNotNulls                  []catalog.Constraint
	allActiveAndInactiveNotNulls []catalog.Constraint
	allActiveNotNulls            []catalog.Constraint

	allFKs                  []catalog.Constraint
	allActiveAndInactiveFKs []catalog.Constraint
	allActiveFKs            []catalog.Constraint

	allUniqueWithoutIndexes                  []catalog.Constraint
	allActiveAndInactiveUniqueWithoutIndexes []catalog.Constraint
	allActiveUniqueWithoutIndexes            []catalog.Constraint
}

// newConstraintCache returns a fresh fully-populated constraintCache struct for the
// TableDescriptor.
func newConstraintCache(desc *descpb.TableDescriptor, mutations *mutationCache) *constraintCache {
	c := constraintCache{}

	// addIfNotExists is a function that adds constraint `c` to slice `dest` if this
	// constraint is not already in it (as determined by using a set of already added
	// constraint IDs `constraintIDsInDest`).
	// If `constraintIDsInDest` is nil, blindly append `c` to `dest`.
	addIfNotExists := func(
		c catalog.Constraint,
		dest []catalog.Constraint,
		constraintIDsInDest map[descpb.ConstraintID]bool,
	) []catalog.Constraint {
		if constraintIDsInDest == nil {
			dest = append(dest, c)
			return dest
		}

		if _, exist := constraintIDsInDest[c.GetConstraintID()]; exist {
			return dest
		}
		dest = append(dest, c)
		constraintIDsInDest[c.GetConstraintID()] = true
		return dest
	}

	// addConstraintToSetsByValidity is a function that adds constraint `c` to various slice
	// categorized by validity.
	addConstraintToSetsByValidity := func(
		c catalog.Constraint,
		all, allActiveAndInactive, allActive []catalog.Constraint,
	) (
		updatedAll []catalog.Constraint,
		updatedAllActiveAndInactive []catalog.Constraint,
		updatedAllActive []catalog.Constraint,
	) {
		cstValidity := c.GetConstraintValidity()
		all = addIfNotExists(c, all, nil)

		if cstValidity == descpb.ConstraintValidity_Validated ||
			cstValidity == descpb.ConstraintValidity_Validating ||
			cstValidity == descpb.ConstraintValidity_Unvalidated {
			allActiveAndInactive = addIfNotExists(c, allActiveAndInactive, nil)
		}
		if cstValidity == descpb.ConstraintValidity_Validated {
			allActive = addIfNotExists(c, allActive, nil)
		}
		return all, allActiveAndInactive, allActive
	}

	// Glean all constraints from `desc` and `mutations`.
	var allConstraints []catalog.Constraint
	constraintIDs := make(map[descpb.ConstraintID]bool)
	for _, cst := range []descpb.IndexDescriptor{desc.PrimaryIndex} {
		backingStruct := index{
			desc:                 &cst,
			validityIfConstraint: descpb.ConstraintValidity_Validated,
		}
		allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
	}
	for _, cst := range desc.Indexes {
		if cst.Unique {
			backingStruct := index{
				desc:                 &cst,
				validityIfConstraint: descpb.ConstraintValidity_Validated,
			}
			allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
		}
	}
	for _, cst := range desc.Checks {
		backingStruct := constraintToUpdate{
			desc: &descpb.ConstraintToUpdate{
				ConstraintType: descpb.ConstraintToUpdate_CHECK,
				Name:           cst.Name,
				Check:          *cst,
			},
		}
		allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
	}
	for _, cst := range append(desc.OutboundFKs, desc.InboundFKs...) {
		backingStruct := constraintToUpdate{
			desc: &descpb.ConstraintToUpdate{
				ConstraintType: descpb.ConstraintToUpdate_FOREIGN_KEY,
				Name:           cst.Name,
				ForeignKey:     cst,
			},
		}
		allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
	}
	for _, cst := range desc.UniqueWithoutIndexConstraints {
		backingStruct := constraintToUpdate{
			desc: &descpb.ConstraintToUpdate{
				ConstraintType:               descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX,
				Name:                         cst.Name,
				UniqueWithoutIndexConstraint: cst,
			},
		}
		allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
	}
	for _, cstMutation := range mutations.all {
		if cst := cstMutation.AsConstraintWithoutIndex(); cst != nil {
			backingStruct := constraintToUpdate{
				maybeMutation: maybeMutation{
					mutationID:         cstMutation.MutationID(),
					mutationDirection:  mutationDirection(cstMutation),
					mutationState:      mutationState(cstMutation),
					mutationIsRollback: cstMutation.IsRollback(),
				},
			}
			if cc, ok := cst.(*constraintToUpdate); ok {
				backingStruct.desc = cc.desc
			}
			allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
		}
		if cst := cstMutation.AsIndex(); cst != nil {
			validity := descpb.ConstraintValidity_Validating
			if !cstMutation.Adding() {
				validity = descpb.ConstraintValidity_Dropping
			}
			switch cst.GetEncodingType() {
			case descpb.PrimaryIndexEncoding:
				backingStruct := index{
					desc:                 cst.IndexDesc(),
					validityIfConstraint: validity,
				}
				allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
			case descpb.SecondaryIndexEncoding:
				if cst.IsUnique() {
					backingStruct := index{
						desc:                 cst.IndexDesc(),
						validityIfConstraint: validity,
					}
					allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
				}
			default:
				panic("unknown index encoding type")
			}
		}
	}

	// Populate constraintCache `c`.
	for _, cst := range allConstraints {
		c.all, c.allActiveAndInactive, c.allActive =
			addConstraintToSetsByValidity(cst, c.all, c.allActiveAndInactive, c.allActive)
		c.allChecks, c.allActiveAndInactiveChecks, c.allActiveChecks =
			addConstraintToSetsByValidity(cst, c.allChecks, c.allActiveAndInactiveChecks, c.allActiveChecks)
		c.allNotNulls, c.allActiveAndInactiveNotNulls, c.allActiveNotNulls =
			addConstraintToSetsByValidity(cst, c.allNotNulls, c.allActiveAndInactiveNotNulls, c.allActiveNotNulls)
		c.allFKs, c.allActiveAndInactiveFKs, c.allActiveFKs =
			addConstraintToSetsByValidity(cst, c.allFKs, c.allActiveAndInactiveFKs, c.allActiveFKs)
		c.allUniqueWithoutIndexes, c.allActiveAndInactiveUniqueWithoutIndexes, c.allActiveUniqueWithoutIndexes =
			addConstraintToSetsByValidity(cst, c.allUniqueWithoutIndexes, c.allActiveAndInactiveUniqueWithoutIndexes, c.allActiveUniqueWithoutIndexes)
	}

	return &c
}

func mutationState(mutation catalog.Mutation) (ret descpb.DescriptorMutation_State) {
	if mutation.DeleteOnly() {
		ret = descpb.DescriptorMutation_DELETE_ONLY
	} else if mutation.WriteAndDeleteOnly() {
		ret = descpb.DescriptorMutation_WRITE_ONLY
	} else if mutation.Backfilling() {
		ret = descpb.DescriptorMutation_BACKFILLING
	} else if mutation.Merging() {
		ret = descpb.DescriptorMutation_MERGING
	} else {
		panic(errors.AssertionFailedf("unknown mutation state"))
	}
	return ret
}

func mutationDirection(mutation catalog.Mutation) descpb.DescriptorMutation_Direction {
	if mutation.Adding() {
		return descpb.DescriptorMutation_ADD
	} else {
		return descpb.DescriptorMutation_DROP
	}
}
