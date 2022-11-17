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
func newConstraintCache(
	desc *descpb.TableDescriptor, columns *columnCache, mutations *mutationCache,
) *constraintCache {
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
		backingStruct := constraint{
			detail: descpb.ConstraintDetail{
				Kind:                            descpb.ConstraintTypePK,
				ConstraintID:                    cst.ConstraintID,
				Columns:                         cst.KeyColumnNames,
				Index:                           &cst,
				ValidityIfIndexBackedConstraint: descpb.ConstraintValidity_Validated,
			},
		}
		allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
	}
	for _, cst := range desc.Indexes {
		if cst.Unique {
			backingStruct := constraint{
				detail: descpb.ConstraintDetail{
					Kind:                            descpb.ConstraintTypeUnique,
					ConstraintID:                    cst.ConstraintID,
					Columns:                         cst.KeyColumnNames,
					Index:                           &cst,
					ValidityIfIndexBackedConstraint: descpb.ConstraintValidity_Validated,
				},
			}
			allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
		}
	}
	for _, cst := range desc.Checks {
		backingStruct := constraint{
			detail: descpb.ConstraintDetail{
				Kind:         descpb.ConstraintTypeCheck,
				ConstraintID: cst.ConstraintID,
				Columns:      columnNamesForColumnIDs(desc, columns, cst.ColumnIDs),
				Details:      cst.Expr,
				// Constraints in the Validating state are considered Unvalidated for this
				// purpose.
				Unvalidated:     cst.Validity != descpb.ConstraintValidity_Validated,
				CheckConstraint: cst,
			},
		}
		allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
	}
	for _, cst := range desc.OutboundFKs {
		backingStruct := constraint{
			detail: descpb.ConstraintDetail{
				Kind:         descpb.ConstraintTypeFK,
				ConstraintID: cst.ConstraintID,
				Columns:      columnNamesForColumnIDs(desc, columns, cst.OriginColumnIDs),
				Details:      "", // TODO (xiang): populate this field; it requires TableLookupFn
				// Constraints in the Validating state are considered Unvalidated for this
				// purpose.
				Unvalidated:     cst.Validity != descpb.ConstraintValidity_Validated,
				FK:              &cst,
				ReferencedTable: nil, // TODO (xiang): populate this field; it requires TableLookupFn
			},
		}
		allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
	}
	for _, cst := range desc.UniqueWithoutIndexConstraints {
		backingStruct := constraint{
			detail: descpb.ConstraintDetail{
				Kind:                         descpb.ConstraintTypeUnique,
				ConstraintID:                 cst.ConstraintID,
				Columns:                      columnNamesForColumnIDs(desc, columns, cst.ColumnIDs),
				Unvalidated:                  cst.Validity != descpb.ConstraintValidity_Validated,
				UniqueWithoutIndexConstraint: &cst,
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
				detail: descpb.ConstraintDetail{
					Kind:         getConstraintDetailTypeFromConstraint(cst),
					ConstraintID: cst.GetConstraintID(),
					Unvalidated:  cst.GetConstraintValidity() != descpb.ConstraintValidity_Validated,
				},
			}
			if cst.IsCheck() || cst.IsNotNull() {
				backingStruct.detail.Columns = columnNamesForColumnIDs(desc, columns, cst.Check().ColumnIDs)
				backingStruct.detail.Details = cst.Check().Expr
				backingStruct.detail.CheckConstraint = cst.Check()
			} else if cst.IsForeignKey() {
				backingStruct.detail.Columns = columnNamesForColumnIDs(desc, columns, cst.ForeignKey().OriginColumnIDs)
				backingStruct.detail.Details = "" // TODO (xiang): populate this field; it requires TableLookupFn
				backingStruct.detail.FK = cst.ForeignKey()
				backingStruct.detail.ReferencedTable = nil // TODO (xiang): populate this field; it requires TableLookupFn
			} else if cst.IsUniqueWithoutIndex() {
				backingStruct.detail.Columns = columnNamesForColumnIDs(desc, columns, cst.UniqueWithoutIndex().ColumnIDs)
				backingStruct.detail.UniqueWithoutIndexConstraint = cst.UniqueWithoutIndex()
			} else {
				panic(errors.AssertionFailedf("unknown mutation constraint type"))
			}
			allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
		}
		if cst := cstMutation.AsIndex(); cst != nil && cst.IsUnique() {
			backingStruct := constraint{
				maybeMutation: maybeMutation{
					mutationID:         cstMutation.MutationID(),
					mutationDirection:  mutationDirection(cstMutation),
					mutationState:      mutationState(cstMutation),
					mutationIsRollback: cstMutation.IsRollback(),
				},
				detail: descpb.ConstraintDetail{
					Kind:                            getConstraintDetailTypeFromEncodingType(cst.GetEncodingType()),
					ConstraintID:                    cst.GetConstraintID(),
					Columns:                         cst.IndexDesc().KeyColumnNames,
					Index:                           cst.IndexDesc(),
					ValidityIfIndexBackedConstraint: getIndexBackedConstraintMutationValidityFromDirection(cstMutation),
				},
			}
			allConstraints = addIfNotExists(backingStruct, allConstraints, constraintIDs)
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

func columnNamesForColumnIDs(
	tbl *descpb.TableDescriptor, columns *columnCache, columnIDs []descpb.ColumnID,
) []string {
	desc := wrapper{TableDescriptor: *tbl, columnCache: columns}
	ret, err := desc.NamesForColumnIDs(columnIDs)
	if err != nil {
		panic(err)
	}
	return ret
}

func getConstraintDetailTypeFromConstraint(c catalog.Constraint) descpb.ConstraintType {
	if c.IsPrimaryKey() {
		return descpb.ConstraintTypePK
	} else if c.IsForeignKey() {
		return descpb.ConstraintTypeFK
	} else if c.IsUniqueConstraint() || c.IsUniqueWithoutIndex() {
		return descpb.ConstraintTypeUnique
	} else if c.IsCheck() || c.IsNotNull() {
		return descpb.ConstraintTypeCheck
	} else {
		panic(errors.AssertionFailedf("unknown constraint type"))
	}
}

func getConstraintToUpdateTypeFromConstraint(
	c catalog.Constraint,
) descpb.ConstraintToUpdate_ConstraintType {
	if c.IsForeignKey() {
		return descpb.ConstraintToUpdate_FOREIGN_KEY
	} else if c.IsUniqueWithoutIndex() {
		return descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX
	} else if c.IsCheck() || c.IsNotNull() {
		return descpb.ConstraintToUpdate_CHECK
	} else {
		panic(errors.AssertionFailedf("unknown constraint type"))
	}
}

func getConstraintDetailTypeFromConstraintToUpdate(
	c *descpb.ConstraintToUpdate,
) descpb.ConstraintType {
	switch c.ConstraintType {
	case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
		return descpb.ConstraintTypeCheck
	case descpb.ConstraintToUpdate_FOREIGN_KEY:
		return descpb.ConstraintTypeFK
	case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
		return descpb.ConstraintTypeUnique
	default:
		panic(errors.AssertionFailedf("unknown ConstraintToUpdate type"))
	}
}

func getConstraintIDFromConstraintToUpdate(c *descpb.ConstraintToUpdate) descpb.ConstraintID {
	switch c.ConstraintType {
	case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
		return c.Check.ConstraintID
	case descpb.ConstraintToUpdate_FOREIGN_KEY:
		return c.ForeignKey.ConstraintID
	case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
		return c.UniqueWithoutIndexConstraint.ConstraintID
	default:
		panic(errors.AssertionFailedf("unknown ConstraintToUpdate type"))
	}
}

func getConstraintValidityFromConstraintToUpdate(
	c *descpb.ConstraintToUpdate,
) descpb.ConstraintValidity {
	switch c.ConstraintType {
	case descpb.ConstraintToUpdate_CHECK, descpb.ConstraintToUpdate_NOT_NULL:
		return c.Check.Validity
	case descpb.ConstraintToUpdate_FOREIGN_KEY:
		return c.ForeignKey.Validity
	case descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX:
		return c.UniqueWithoutIndexConstraint.Validity
	default:
		panic(errors.AssertionFailedf("unknown ConstraintToUpdate type"))
	}
}

func getConstraintDetailTypeFromEncodingType(
	et descpb.IndexDescriptorEncodingType,
) descpb.ConstraintType {
	switch et {
	case descpb.PrimaryIndexEncoding:
		return descpb.ConstraintTypePK
	case descpb.SecondaryIndexEncoding:
		return descpb.ConstraintTypeUnique
	default:
		panic(errors.AssertionFailedf("unknown index encoding type"))
	}
}

func getIndexBackedConstraintMutationValidityFromDirection(
	cstMutation catalog.Mutation,
) descpb.ConstraintValidity {
	validity := descpb.ConstraintValidity_Validating
	if !cstMutation.Adding() {
		validity = descpb.ConstraintValidity_Dropping
	}
	return validity
}
