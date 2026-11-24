// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"bytes"
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/errors"
)

func (i *immediateVisitor) AddEnumTypeValue(ctx context.Context, op scop.AddEnumTypeValue) error {
	typ, err := i.checkOutType(ctx, op.TypeID)
	if err != nil {
		return err
	}

	insertIdx, _ := slices.BinarySearchFunc(typ.EnumMembers, op.PhysicalRepresentation,
		func(member descpb.TypeDescriptor_EnumMember, target []byte) int {
			return bytes.Compare(member.PhysicalRepresentation, target)
		})

	newMember := descpb.TypeDescriptor_EnumMember{
		PhysicalRepresentation: op.PhysicalRepresentation,
		LogicalRepresentation:  op.LogicalRepresentation,
		Capability:             descpb.TypeDescriptor_EnumMember_READ_ONLY,
		Direction:              descpb.TypeDescriptor_EnumMember_ADD,
	}

	typ.EnumMembers = append(typ.EnumMembers, descpb.TypeDescriptor_EnumMember{})
	copy(typ.EnumMembers[insertIdx+1:], typ.EnumMembers[insertIdx:])
	typ.EnumMembers[insertIdx] = newMember

	return nil
}

func (i *immediateVisitor) PromoteEnumTypeValue(
	ctx context.Context, op scop.PromoteEnumTypeValue,
) error {
	typ, err := i.checkOutType(ctx, op.TypeID)
	if err != nil {
		return err
	}

	for j := range typ.EnumMembers {
		member := &typ.EnumMembers[j]
		if member.LogicalRepresentation == op.LogicalRepresentation &&
			bytes.Equal(member.PhysicalRepresentation, op.PhysicalRepresentation) {
			member.Capability = descpb.TypeDescriptor_EnumMember_ALL
			member.Direction = descpb.TypeDescriptor_EnumMember_NONE
			return nil
		}
	}

	return errors.AssertionFailedf(
		"enum value %q not found in type descriptor %d",
		op.LogicalRepresentation, op.TypeID,
	)
}

func (i *immediateVisitor) DemoteEnumTypeValue(
	ctx context.Context, op scop.DemoteEnumTypeValue,
) error {
	typ, err := i.checkOutType(ctx, op.TypeID)
	if err != nil {
		return err
	}

	for j := range typ.EnumMembers {
		member := &typ.EnumMembers[j]
		if member.LogicalRepresentation == op.LogicalRepresentation &&
			bytes.Equal(member.PhysicalRepresentation, op.PhysicalRepresentation) {
			member.Capability = descpb.TypeDescriptor_EnumMember_READ_ONLY
			member.Direction = descpb.TypeDescriptor_EnumMember_REMOVE
			return nil
		}
	}

	return errors.AssertionFailedf(
		"enum value %q not found in type descriptor %d",
		op.LogicalRepresentation, op.TypeID,
	)
}

func (i *immediateVisitor) RemoveEnumTypeValue(
	ctx context.Context, op scop.RemoveEnumTypeValue,
) error {
	typ, err := i.checkOutType(ctx, op.TypeID)
	if err != nil {
		return err
	}

	for j := range typ.EnumMembers {
		member := &typ.EnumMembers[j]
		if member.LogicalRepresentation == op.LogicalRepresentation &&
			bytes.Equal(member.PhysicalRepresentation, op.PhysicalRepresentation) {
			typ.EnumMembers = append(typ.EnumMembers[:j], typ.EnumMembers[j+1:]...)
			return nil
		}
	}
	return errors.AssertionFailedf(
		"enum value %q not found in type descriptor %d",
		op.LogicalRepresentation, op.TypeID,
	)
}

func (i *immediateVisitor) AddDomainNotNull(ctx context.Context, op scop.AddDomainNotNull) error {
	typ, err := i.checkOutType(ctx, op.TypeID)
	if err != nil || typ.Dropped() {
		return err
	}
	if typ.Domain == nil {
		return errors.AssertionFailedf(
			"type descriptor %d is not a domain type", op.TypeID,
		)
	}
	typ.Domain.NotNullState = descpb.TypeDescriptor_Domain_VALIDATING
	typ.Domain.NotNullConstraintID = op.ConstraintID
	// Use a placeholder name until it's properly set.
	typ.Domain.NotNullConstraintName = tabledesc.ConstraintNamePlaceholder(op.ConstraintID)
	// Bump the persisted counter so that a future ALTER DOMAIN in a separate
	// transaction does not reuse this ID.
	if next := op.ConstraintID + 1; typ.Domain.NextConstraintID < next {
		typ.Domain.NextConstraintID = next
	}
	return nil
}

func (i *immediateVisitor) MakeValidatedDomainNotNullPublic(
	ctx context.Context, op scop.MakeValidatedDomainNotNullPublic,
) error {
	typ, err := i.checkOutType(ctx, op.TypeID)
	if err != nil || typ.Dropped() {
		return err
	}
	if typ.Domain == nil {
		return errors.AssertionFailedf(
			"type descriptor %d is not a domain type", op.TypeID,
		)
	}
	if typ.Domain.NotNullConstraintID != op.ConstraintID {
		return errors.AssertionFailedf(
			"NOT NULL constraint %d not found in domain type descriptor %d",
			op.ConstraintID, op.TypeID,
		)
	}
	typ.Domain.NotNullState = descpb.TypeDescriptor_Domain_ENFORCING
	return nil
}

func (i *immediateVisitor) MakePublicDomainNotNullValidated(
	ctx context.Context, op scop.MakePublicDomainNotNullValidated,
) error {
	typ, err := i.checkOutType(ctx, op.TypeID)
	if err != nil || typ.Dropped() {
		return err
	}
	if typ.Domain == nil {
		return errors.AssertionFailedf(
			"type descriptor %d is not a domain type", op.TypeID,
		)
	}
	if typ.Domain.NotNullConstraintID != op.ConstraintID {
		return errors.AssertionFailedf(
			"NOT NULL constraint %d not found in domain type descriptor %d",
			op.ConstraintID, op.TypeID,
		)
	}
	typ.Domain.NotNullState = descpb.TypeDescriptor_Domain_DROPPING
	return nil
}

func (i *immediateVisitor) RemoveDomainNotNull(
	ctx context.Context, op scop.RemoveDomainNotNull,
) error {
	typ, err := i.checkOutType(ctx, op.TypeID)
	if err != nil || typ.Dropped() {
		return err
	}
	if typ.Domain == nil {
		return errors.AssertionFailedf(
			"type descriptor %d is not a domain type", op.TypeID,
		)
	}
	// Guard against clobbering a different NOT NULL constraint that has been
	// added since this op was planned.
	if typ.Domain.NotNullConstraintID != op.ConstraintID {
		return errors.AssertionFailedf(
			"NOT NULL constraint %d not found in domain type descriptor %d",
			op.ConstraintID, op.TypeID,
		)
	}
	typ.Domain.NotNullState = descpb.TypeDescriptor_Domain_NONE
	typ.Domain.NotNullConstraintID = 0
	typ.Domain.NotNullConstraintName = ""
	return nil
}

func (i *immediateVisitor) SetDomainConstraintName(
	ctx context.Context, op scop.SetDomainConstraintName,
) error {
	typ, err := i.checkOutType(ctx, op.TypeID)
	if err != nil || typ.Dropped() {
		return err
	}
	if typ.Domain == nil {
		return errors.AssertionFailedf(
			"type descriptor %d is not a domain type", op.TypeID,
		)
	}
	if typ.Domain.NotNullConstraintID == op.ConstraintID {
		typ.Domain.NotNullConstraintName = op.Name
		return nil
	}
	for j := range typ.Domain.CheckConstraints {
		if typ.Domain.CheckConstraints[j].ConstraintID == op.ConstraintID {
			typ.Domain.CheckConstraints[j].Name = op.Name
			return nil
		}
	}
	return errors.AssertionFailedf(
		"constraint %d not found in domain type descriptor %d",
		op.ConstraintID, op.TypeID,
	)
}

func (i *immediateVisitor) RemoveDomainConstraintName(
	ctx context.Context, op scop.RemoveDomainConstraintName,
) error {
	typ, err := i.checkOutType(ctx, op.TypeID)
	if err != nil || typ.Dropped() {
		return err
	}
	if typ.Domain == nil {
		return errors.AssertionFailedf(
			"type descriptor %d is not a domain type", op.TypeID,
		)
	}
	// Use a placeholder to free the name for reuse while the constraint is
	// still present in the descriptor.
	placeholder := tabledesc.ConstraintNamePlaceholder(op.ConstraintID)
	if typ.Domain.NotNullConstraintID == op.ConstraintID {
		typ.Domain.NotNullConstraintName = placeholder
		return nil
	}
	for j := range typ.Domain.CheckConstraints {
		if typ.Domain.CheckConstraints[j].ConstraintID == op.ConstraintID {
			typ.Domain.CheckConstraints[j].Name = placeholder
			return nil
		}
	}
	return errors.AssertionFailedf(
		"constraint %d not found in domain type descriptor %d",
		op.ConstraintID, op.TypeID,
	)
}
