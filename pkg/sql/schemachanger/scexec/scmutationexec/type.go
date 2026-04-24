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
