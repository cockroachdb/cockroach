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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
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

// checkOutDomain returns the domain type descriptor for typeID.
func (i *immediateVisitor) checkOutDomain(
	ctx context.Context, typeID descpb.ID,
) (*typedesc.Mutable, error) {
	typ, err := i.checkOutType(ctx, typeID)
	if err != nil {
		return nil, err
	}
	if typ.Domain == nil {
		return nil, errors.AssertionFailedf(
			"type descriptor %d is not a domain type", typeID,
		)
	}
	return typ, nil
}

func (i *immediateVisitor) AddDomainNotNull(ctx context.Context, op scop.AddDomainNotNull) error {
	typ, err := i.checkOutDomain(ctx, op.TypeID)
	if err != nil {
		return err
	}
	typ.Domain.NotNull = true
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

func (i *immediateVisitor) RemoveDomainNotNull(
	ctx context.Context, op scop.RemoveDomainNotNull,
) error {
	typ, err := i.checkOutDomain(ctx, op.TypeID)
	if err != nil {
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
	typ.Domain.NotNull = false
	typ.Domain.NotNullConstraintID = 0
	typ.Domain.NotNullConstraintName = ""
	return nil
}

func (i *immediateVisitor) SetDomainConstraintName(
	ctx context.Context, op scop.SetDomainConstraintName,
) error {
	typ, err := i.checkOutDomain(ctx, op.TypeID)
	if err != nil {
		return err
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
	typ, err := i.checkOutDomain(ctx, op.TypeID)
	if err != nil {
		return err
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

func (i *immediateVisitor) AddDomainCheckConstraint(
	ctx context.Context, op scop.AddDomainCheckConstraint,
) error {
	typ, err := i.checkOutDomain(ctx, op.TypeID)
	if err != nil {
		return err
	}
	typ.Domain.CheckConstraints = append(typ.Domain.CheckConstraints,
		descpb.TypeDescriptor_Domain_CheckConstraint{
			Name:         tabledesc.ConstraintNamePlaceholder(op.ConstraintID),
			Expr:         string(op.Expr),
			ConstraintID: op.ConstraintID,
			Validity:     op.Validity,
		},
	)
	return nil
}

func (i *immediateVisitor) RemoveDomainCheckConstraint(
	ctx context.Context, op scop.RemoveDomainCheckConstraint,
) error {
	typ, err := i.checkOutDomain(ctx, op.TypeID)
	if err != nil {
		return err
	}
	for j := range typ.Domain.CheckConstraints {
		if typ.Domain.CheckConstraints[j].ConstraintID == op.ConstraintID {
			typ.Domain.CheckConstraints = slices.Delete(
				typ.Domain.CheckConstraints, j, j+1,
			)
			return nil
		}
	}
	return errors.AssertionFailedf(
		"check constraint %d not found in domain type descriptor %d",
		op.ConstraintID, op.TypeID,
	)
}

func (i *immediateVisitor) MakeValidatedDomainCheckConstraintPublic(
	ctx context.Context, op scop.MakeValidatedDomainCheckConstraintPublic,
) error {
	return setDomainCheckConstraintValidity(
		ctx, i, op.TypeID, op.ConstraintID, descpb.ConstraintValidity_Validated,
	)
}

func (i *immediateVisitor) MakePublicDomainCheckConstraintValidated(
	ctx context.Context, op scop.MakePublicDomainCheckConstraintValidated,
) error {
	return setDomainCheckConstraintValidity(
		ctx, i, op.TypeID, op.ConstraintID, descpb.ConstraintValidity_Dropping,
	)
}

func setDomainCheckConstraintValidity(
	ctx context.Context,
	i *immediateVisitor,
	typeID descpb.ID,
	constraintID descpb.ConstraintID,
	validity descpb.ConstraintValidity,
) error {
	typ, err := i.checkOutDomain(ctx, typeID)
	if err != nil {
		return err
	}
	for j := range typ.Domain.CheckConstraints {
		if typ.Domain.CheckConstraints[j].ConstraintID == constraintID {
			typ.Domain.CheckConstraints[j].Validity = validity
			return nil
		}
	}
	return errors.AssertionFailedf(
		"check constraint %d not found in domain type descriptor %d",
		constraintID, typeID,
	)
}

func (i *immediateVisitor) SetDomainDefault(ctx context.Context, op scop.SetDomainDefault) error {
	typ, err := i.checkOutDomain(ctx, op.TypeID)
	if err != nil {
		return err
	}
	typ.Domain.DefaultExpr = string(op.Expr)
	return nil
}

func (i *immediateVisitor) RemoveDomainDefault(
	ctx context.Context, op scop.RemoveDomainDefault,
) error {
	typ, err := i.checkOutDomain(ctx, op.TypeID)
	if err != nil {
		return err
	}
	typ.Domain.DefaultExpr = ""
	return nil
}
