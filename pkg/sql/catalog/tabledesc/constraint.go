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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

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
		backingStruct := constraint{
			desc: &descpb.ConstraintToUpdate{
				ConstraintType: descpb.ConstraintToUpdate_CHECK,
				Name:           cst.Name,
				Check:          *cst,
			},
		}
		allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
	}
	for _, cst := range append(desc.OutboundFKs, desc.InboundFKs...) {
		backingStruct := constraint{
			desc: &descpb.ConstraintToUpdate{
				ConstraintType: descpb.ConstraintToUpdate_FOREIGN_KEY,
				Name:           cst.Name,
				ForeignKey:     cst,
			},
		}
		allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
	}
	for _, cst := range desc.UniqueWithoutIndexConstraints {
		backingStruct := constraint{
			desc: &descpb.ConstraintToUpdate{
				ConstraintType:               descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX,
				Name:                         cst.Name,
				UniqueWithoutIndexConstraint: cst,
			},
		}
		allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
	}
	for _, cstMutation := range mutations.all {
		if cst := cstMutation.AsConstraint(); cst != nil {
			backingStruct := constraint{
				maybeMutation: maybeMutation{
					mutationID:         cstMutation.MutationID(),
					mutationDirection:  mutationDirection(cstMutation),
					mutationState:      mutationState(cstMutation),
					mutationIsRollback: cstMutation.IsRollback(),
				},
			}
			if cc, ok := cst.(*constraint); ok {
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
