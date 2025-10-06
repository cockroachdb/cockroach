// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
	"github.com/cockroachdb/cockroach/pkg/util"
)

type constraintBase struct {
	maybeMutation
}

// AsCheck implements the catalog.ConstraintProvider interface.
func (c constraintBase) AsCheck() catalog.CheckConstraint {
	return nil
}

// AsForeignKey implements the catalog.ConstraintProvider interface.
func (c constraintBase) AsForeignKey() catalog.ForeignKeyConstraint {
	return nil
}

// AsUniqueWithoutIndex implements the catalog.ConstraintProvider interface.
func (c constraintBase) AsUniqueWithoutIndex() catalog.UniqueWithoutIndexConstraint {
	return nil
}

// AsUniqueWithIndex implements the catalog.ConstraintProvider interface.
func (c constraintBase) AsUniqueWithIndex() catalog.UniqueWithIndexConstraint {
	return nil
}

type checkConstraint struct {
	constraintBase
	desc *descpb.TableDescriptor_CheckConstraint
}

var _ catalog.CheckConstraint = (*checkConstraint)(nil)

// CheckDesc implements the catalog.CheckConstraint interface.
func (c checkConstraint) CheckDesc() *descpb.TableDescriptor_CheckConstraint {
	return c.desc
}

// Expr implements the catalog.CheckConstraint interface.
func (c checkConstraint) GetExpr() string {
	return c.desc.Expr
}

// NumReferencedColumns implements the catalog.CheckConstraint interface.
func (c checkConstraint) NumReferencedColumns() int {
	return len(c.desc.ColumnIDs)
}

// GetReferencedColumnID implements the catalog.CheckConstraint
// interface.
func (c checkConstraint) GetReferencedColumnID(columnOrdinal int) descpb.ColumnID {
	return c.desc.ColumnIDs[columnOrdinal]
}

// CollectReferencedColumnIDs implements the catalog.CheckConstraint
// interface.
func (c checkConstraint) CollectReferencedColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(c.desc.ColumnIDs...)
}

// IsNotNullColumnConstraint implements the catalog.CheckConstraint interface.
func (c checkConstraint) IsNotNullColumnConstraint() bool {
	return c.desc.IsNonNullConstraint
}

// IsHashShardingConstraint implements the catalog.CheckConstraint interface.
func (c checkConstraint) IsHashShardingConstraint() bool {
	return c.desc.FromHashShardedColumn
}

// GetConstraintID implements the catalog.Constraint interface.
func (c checkConstraint) GetConstraintID() descpb.ConstraintID {
	return c.desc.ConstraintID
}

// GetConstraintValidity implements the catalog.Constraint interface.
func (c checkConstraint) GetConstraintValidity() descpb.ConstraintValidity {
	return c.desc.Validity
}

// IsConstraintValidated implements the catalog.Constraint interface.
func (c checkConstraint) IsConstraintValidated() bool {
	return c.desc.Validity == descpb.ConstraintValidity_Validated
}

// IsConstraintUnvalidated implements the catalog.Constraint interface.
func (c checkConstraint) IsConstraintUnvalidated() bool {
	return c.desc.Validity == descpb.ConstraintValidity_Unvalidated
}

// GetName implements the catalog.Constraint interface.
func (c checkConstraint) GetName() string {
	return c.desc.Name
}

// AsCheck implements the catalog.ConstraintProvider interface.
func (c checkConstraint) AsCheck() catalog.CheckConstraint {
	return &c
}

// String implements the catalog.Constraint interface.
func (c checkConstraint) String() string {
	return fmt.Sprintf("%+v", c.desc)
}

// IsEnforced implements the catalog.Constraint interface.
func (c checkConstraint) IsEnforced() bool {
	return !c.IsMutation() || c.WriteAndDeleteOnly()
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

// ParentTableID implements the catalog.UniqueWithoutIndexConstraint
// interface.
func (c uniqueWithoutIndexConstraint) ParentTableID() descpb.ID {
	return c.desc.TableID
}

// IsValidReferencedUniqueConstraint implements the catalog.UniqueConstraint
// interface.
func (c uniqueWithoutIndexConstraint) IsValidReferencedUniqueConstraint(
	fk catalog.ForeignKeyConstraint,
) bool {
	return !c.IsPartial() && descpb.ColumnIDs(c.desc.ColumnIDs).PermutationOf(fk.ForeignKeyDesc().ReferencedColumnIDs)
}

// NumKeyColumns implements the catalog.UniqueConstraint interface.
func (c uniqueWithoutIndexConstraint) NumKeyColumns() int {
	return len(c.desc.ColumnIDs)
}

// GetKeyColumnID implements the catalog.UniqueConstraint interface.
func (c uniqueWithoutIndexConstraint) GetKeyColumnID(columnOrdinal int) descpb.ColumnID {
	return c.desc.ColumnIDs[columnOrdinal]
}

// CollectKeyColumnIDs implements the catalog.UniqueConstraint
// interface.
func (c uniqueWithoutIndexConstraint) CollectKeyColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(c.desc.ColumnIDs...)
}

// IsPartial implements the catalog.UniqueConstraint interface.
func (c uniqueWithoutIndexConstraint) IsPartial() bool {
	return c.desc.Predicate != ""
}

// GetPredicate implements the catalog.UniqueConstraint interface.
func (c uniqueWithoutIndexConstraint) GetPredicate() string {
	return c.desc.Predicate
}

// GetConstraintID implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) GetConstraintID() descpb.ConstraintID {
	return c.desc.ConstraintID
}

// GetConstraintValidity implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) GetConstraintValidity() descpb.ConstraintValidity {
	return c.desc.Validity
}

// IsConstraintValidated implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) IsConstraintValidated() bool {
	return c.desc.Validity == descpb.ConstraintValidity_Validated
}

// IsConstraintUnvalidated implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) IsConstraintUnvalidated() bool {
	return c.desc.Validity == descpb.ConstraintValidity_Unvalidated
}

// GetName implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) GetName() string {
	return c.desc.Name
}

// AsUniqueWithoutIndex implements the catalog.ConstraintProvider interface.
func (c uniqueWithoutIndexConstraint) AsUniqueWithoutIndex() catalog.UniqueWithoutIndexConstraint {
	return &c
}

// String implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) String() string {
	return fmt.Sprintf("%+v", c.desc)
}

// IsEnforced implements the catalog.Constraint interface.
func (c uniqueWithoutIndexConstraint) IsEnforced() bool {
	return !c.IsMutation() || c.WriteAndDeleteOnly()
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

// GetOriginTableID implements the catalog.ForeignKeyConstraint interface.
func (c foreignKeyConstraint) GetOriginTableID() descpb.ID {
	return c.desc.OriginTableID
}

// NumOriginColumns implements the catalog.ForeignKeyConstraint interface.
func (c foreignKeyConstraint) NumOriginColumns() int {
	return len(c.desc.OriginColumnIDs)
}

// GetOriginColumnID implements the catalog.ForeignKeyConstraint interface.
func (c foreignKeyConstraint) GetOriginColumnID(columnOrdinal int) descpb.ColumnID {
	return c.desc.OriginColumnIDs[columnOrdinal]
}

// CollectOriginColumnIDs implements the catalog.ForeignKeyConstraint
// interface.
func (c foreignKeyConstraint) CollectOriginColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(c.desc.OriginColumnIDs...)
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
	return c.desc.ReferencedColumnIDs[columnOrdinal]
}

// CollectReferencedColumnIDs implements the catalog.ForeignKeyConstraint
// interface.
func (c foreignKeyConstraint) CollectReferencedColumnIDs() catalog.TableColSet {
	return catalog.MakeTableColSet(c.desc.ReferencedColumnIDs...)
}

// OnDelete implements the catalog.ForeignKeyConstraint interface.
func (c foreignKeyConstraint) OnDelete() semenumpb.ForeignKeyAction {
	return c.desc.OnDelete
}

// OnUpdate implements the catalog.ForeignKeyConstraint interface.
func (c foreignKeyConstraint) OnUpdate() semenumpb.ForeignKeyAction {
	return c.desc.OnUpdate
}

// Match implements the catalog.ForeignKeyConstraint interface.
func (c foreignKeyConstraint) Match() semenumpb.Match {
	return c.desc.Match
}

// GetConstraintID implements the catalog.Constraint interface.
func (c foreignKeyConstraint) GetConstraintID() descpb.ConstraintID {
	return c.desc.ConstraintID
}

// GetConstraintValidity implements the catalog.Constraint interface.
func (c foreignKeyConstraint) GetConstraintValidity() descpb.ConstraintValidity {
	return c.desc.Validity
}

// IsConstraintValidated implements the catalog.Constraint interface.
func (c foreignKeyConstraint) IsConstraintValidated() bool {
	return c.desc.Validity == descpb.ConstraintValidity_Validated
}

// IsConstraintUnvalidated implements the catalog.Constraint interface.
func (c foreignKeyConstraint) IsConstraintUnvalidated() bool {
	return c.desc.Validity == descpb.ConstraintValidity_Unvalidated
}

// GetName implements the catalog.Constraint interface.
func (c foreignKeyConstraint) GetName() string {
	return c.desc.Name
}

// AsForeignKey implements the catalog.ConstraintProvider interface.
func (c foreignKeyConstraint) AsForeignKey() catalog.ForeignKeyConstraint {
	return &c
}

// String implements the catalog.Constraint interface.
func (c foreignKeyConstraint) String() string {
	return fmt.Sprintf("%+v", c.desc)
}

// IsEnforced implements the catalog.Constraint interface.
func (c foreignKeyConstraint) IsEnforced() bool {
	return !c.IsMutation() || c.WriteAndDeleteOnly()
}

// constraintCache contains precomputed slices of constraints, categorized by:
//   - constraint subtype: checks, fks, etc.
//   - enforcement status: whether a constraint is enforced for data written to
//     the table, regardless of whether the constraint is valid for table data
//     which existed prior to the constraint being added to the table.
type constraintCache struct {
	all, allEnforced       []catalog.Constraint
	checks, checksEnforced []catalog.CheckConstraint
	fks, fksEnforced       []catalog.ForeignKeyConstraint
	uwis, uwisEnforced     []catalog.UniqueWithIndexConstraint
	uwois, uwoisEnforced   []catalog.UniqueWithoutIndexConstraint
	fkBackRefs             []catalog.ForeignKeyConstraint
}

// newConstraintCache returns a fresh fully-populated constraintCache struct for the
// TableDescriptor.
func newConstraintCache(
	desc *descpb.TableDescriptor, indexes *indexCache, mutations *mutationCache,
) *constraintCache {
	capUWIs := len(indexes.all)
	numEnforcedChecks := len(desc.Checks)
	capChecks := numEnforcedChecks + len(mutations.checks)
	numEnforcedFKs := len(desc.OutboundFKs)
	capFKs := numEnforcedFKs + len(mutations.fks)
	numEnforcedUWOIs := len(desc.UniqueWithoutIndexConstraints)
	capUWOIs := numEnforcedUWOIs + len(mutations.uniqueWithoutIndexes)
	capAll := capChecks + capFKs + capUWOIs + capUWIs
	// Pre-allocate slices which are known not to be empty:
	// physical tables always have at least one index-backed unique constraint
	// in the form of the primary key.
	c := constraintCache{
		all:          make([]catalog.Constraint, 0, capAll),
		allEnforced:  make([]catalog.Constraint, 0, capAll),
		uwis:         make([]catalog.UniqueWithIndexConstraint, 0, capUWIs),
		uwisEnforced: make([]catalog.UniqueWithIndexConstraint, 0, capUWIs),
	}
	// Populate with index-backed unique constraints.
	for _, idx := range indexes.all {
		if uwi := idx.AsUniqueWithIndex(); uwi != nil && uwi.NumKeyColumns() > 0 {
			c.all = append(c.all, uwi)
			c.uwis = append(c.uwis, uwi)
			if uwi.IsEnforced() {
				c.allEnforced = append(c.allEnforced, uwi)
				c.uwisEnforced = append(c.uwisEnforced, uwi)
			}
		}
	}
	// Populate with check constraints.
	if capChecks > 0 {
		c.checks = make([]catalog.CheckConstraint, 0, capChecks)
		var byID util.FastIntMap
		var checkBackingStructs []checkConstraint
		if numEnforcedChecks > 0 {
			checkBackingStructs = make([]checkConstraint, numEnforcedChecks)
			for i, ckDesc := range desc.Checks {
				checkBackingStructs[i].desc = ckDesc
				ck := &checkBackingStructs[i]
				byID.Set(int(ck.desc.ConstraintID), i)
				c.all = append(c.all, ck)
				c.allEnforced = append(c.allEnforced, ck)
				c.checks = append(c.checks, ck)
				c.checksEnforced = append(c.checksEnforced, ck)
			}
		}
		for _, m := range mutations.checks {
			ck := m.AsCheck()
			if ordinal, found := byID.Get(int(ck.GetConstraintID())); found {
				checkBackingStructs[ordinal].maybeMutation = ck.(*checkConstraint).maybeMutation
			} else {
				c.all = append(c.all, ck)
				c.checks = append(c.checks, ck)
				// TODO (xiang): If `m.WriteAndDeleteOnly()`, then `ck` is enforced and
				// we need to add it to `c.allEnforced` and `c.checksEnforced`. We don't
				// do it right now because it will break some existing unit test about
				// legacy schema changer, due to different semantics of when a check
				// is enforced.
			}
		}
	}
	// Populate with foreign key constraints.
	if capFKs > 0 {
		c.fks = make([]catalog.ForeignKeyConstraint, 0, capFKs)
		var byID util.FastIntMap
		var fkBackingStructs []foreignKeyConstraint
		if numEnforcedFKs > 0 {
			fkBackingStructs = make([]foreignKeyConstraint, numEnforcedFKs)
			for i := range desc.OutboundFKs {
				fkBackingStructs[i].desc = &desc.OutboundFKs[i]
				fk := &fkBackingStructs[i]
				byID.Set(int(fk.desc.ConstraintID), i)
				c.all = append(c.all, fk)
				c.allEnforced = append(c.allEnforced, fk)
				c.fks = append(c.fks, fk)
				c.fksEnforced = append(c.fksEnforced, fk)
			}
		}
		for _, m := range mutations.fks {
			fk := m.AsForeignKey()
			if ordinal, found := byID.Get(int(fk.GetConstraintID())); found {
				fkBackingStructs[ordinal].maybeMutation = fk.(*foreignKeyConstraint).maybeMutation
			} else {
				c.all = append(c.all, fk)
				c.fks = append(c.fks, fk)
				if m.WriteAndDeleteOnly() {
					c.allEnforced = append(c.allEnforced, fk)
					c.fksEnforced = append(c.fksEnforced, fk)
				}
			}
		}
	}
	// Populate with non-index-backed unique constraints.
	if capUWOIs > 0 {
		c.uwois = make([]catalog.UniqueWithoutIndexConstraint, 0, capUWOIs)
		var byID util.FastIntMap
		var uwoisBackingStructs []uniqueWithoutIndexConstraint
		if numEnforcedUWOIs > 0 {
			uwoisBackingStructs = make([]uniqueWithoutIndexConstraint, numEnforcedUWOIs)
			for i := range desc.UniqueWithoutIndexConstraints {
				uwoisBackingStructs[i].desc = &desc.UniqueWithoutIndexConstraints[i]
				uwoi := &uwoisBackingStructs[i]
				byID.Set(int(uwoi.desc.ConstraintID), i)
				c.all = append(c.all, uwoi)
				c.allEnforced = append(c.allEnforced, uwoi)
				c.uwois = append(c.uwois, uwoi)
				c.uwoisEnforced = append(c.uwoisEnforced, uwoi)
			}
		}
		for _, m := range mutations.uniqueWithoutIndexes {
			uwoi := m.AsUniqueWithoutIndex()
			if ordinal, found := byID.Get(int(uwoi.GetConstraintID())); found {
				uwoisBackingStructs[ordinal].maybeMutation = uwoi.(*uniqueWithoutIndexConstraint).maybeMutation
			} else {
				c.all = append(c.all, uwoi)
				c.uwois = append(c.uwois, uwoi)
				if m.WriteAndDeleteOnly() {
					c.allEnforced = append(c.allEnforced, uwoi)
					c.uwoisEnforced = append(c.uwoisEnforced, uwoi)
				}
			}
		}
	}
	// Populate foreign key back-reference slice.
	// These are not constraints on this table, but having them wrapped in the
	// catalog.ForeignKeyConstraint interface is useful.
	if numInboundFKS := len(desc.InboundFKs); numInboundFKS > 0 {
		fkBackRefBackingStructs := make([]foreignKeyConstraint, numInboundFKS)
		c.fkBackRefs = make([]catalog.ForeignKeyConstraint, numInboundFKS)
		for i := range desc.InboundFKs {
			fkBackRefBackingStructs[i].desc = &desc.InboundFKs[i]
			c.fkBackRefs[i] = &fkBackRefBackingStructs[i]
		}
	}
	return &c
}
