// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package typedesc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// HydrateTypesInTableDescriptor uses res to install metadata in the types
// present in a table descriptor. res retrieves the fully qualified name and
// descriptor for a particular ID.
func HydrateTypesInTableDescriptor(
	ctx context.Context, desc *descpb.TableDescriptor, res catalog.TypeDescriptorResolver,
) error {
	for i := range desc.Columns {
		if err := EnsureTypeIsHydrated(ctx, desc.Columns[i].Type, res); err != nil {
			return err
		}
	}
	for i := range desc.Mutations {
		mut := &desc.Mutations[i]
		if col := mut.GetColumn(); col != nil {
			if err := EnsureTypeIsHydrated(ctx, col.Type, res); err != nil {
				return err
			}
		}
	}
	return nil
}

// HydrateTypesInFunctionDescriptor uses res to install metadata in the types
// present in a function descriptor.
func HydrateTypesInFunctionDescriptor(
	ctx context.Context, desc *descpb.FunctionDescriptor, res catalog.TypeDescriptorResolver,
) error {
	for i := range desc.Params {
		if err := EnsureTypeIsHydrated(ctx, desc.Params[i].Type, res); err != nil {
			return err
		}
	}
	if err := EnsureTypeIsHydrated(ctx, desc.GetReturnType().Type, res); err != nil {
		return err
	}
	return nil
}

// HydrateTypesInSchemaDescriptor uses res to install metadata in the types
// present in a function descriptor.
func HydrateTypesInSchemaDescriptor(
	ctx context.Context, desc *descpb.SchemaDescriptor, res catalog.TypeDescriptorResolver,
) error {
	for _, f := range desc.Functions {
		for i := range f.Overloads {
			for _, t := range f.Overloads[i].ArgTypes {
				if err := EnsureTypeIsHydrated(ctx, t, res); err != nil {
					return err
				}
			}
			if err := EnsureTypeIsHydrated(ctx, f.Overloads[i].ReturnType, res); err != nil {
				return err
			}
		}
	}
	return nil
}

// EnsureTypeIsHydrated makes sure that t is a fully-hydrated type.
func EnsureTypeIsHydrated(
	ctx context.Context, t *types.T, res catalog.TypeDescriptorResolver,
) error {
	if !t.UserDefined() {
		return nil
	}
	switch t.Family() {
	case types.ArrayFamily:
		if err := EnsureTypeIsHydrated(ctx, t.ArrayContents(), res); err != nil {
			return err
		}
	case types.TupleFamily:
		for _, e := range t.TupleContents() {
			if err := EnsureTypeIsHydrated(ctx, e, res); err != nil {
				return err
			}
		}
	}
	id := GetUserDefinedTypeDescID(t)
	name, desc, err := res.GetTypeDescriptor(ctx, id)
	if err != nil {
		return err
	}
	if t.Oid() != catid.TypeIDToOID(desc.GetID()) {
		return errors.AssertionFailedf("unexpected mismatch during type hydration: "+
			"type %s has OID %d, descriptor has ID %d", t, t.Oid(), desc.GetID())
	}
	ensureTypeMetadataIsHydrated(&t.TypeMeta, &name, desc)
	return nil
}

func ensureTypeMetadataIsHydrated(
	tm *types.UserDefinedTypeMetadata, name *tree.TypeName, desc catalog.TypeDescriptor,
) {
	if *tm != (types.UserDefinedTypeMetadata{}) && tm.Version == uint32(desc.GetVersion()) {
		return
	}
	*tm = types.UserDefinedTypeMetadata{
		Name: &types.UserDefinedTypeName{
			Catalog:        name.Catalog(),
			ExplicitSchema: name.ExplicitSchema,
			Schema:         name.Schema(),
			Name:           name.Object(),
		},
		Version: uint32(desc.GetVersion()),
	}
	switch desc.GetKind() {
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		n := desc.NumEnumMembers()
		tm.EnumData = &types.EnumMetadata{
			LogicalRepresentations:  make([]string, n),
			PhysicalRepresentations: make([][]byte, n),
			IsMemberReadOnly:        make([]bool, n),
		}
		for i := 0; i < n; i++ {
			tm.EnumData.LogicalRepresentations[i] = desc.GetMemberLogicalRepresentation(i)
			tm.EnumData.PhysicalRepresentations[i] = desc.GetMemberPhysicalRepresentation(i)
			tm.EnumData.IsMemberReadOnly[i] = desc.IsMemberReadOnly(i)
		}
	case descpb.TypeDescriptor_TABLE_IMPLICIT_RECORD_TYPE:
		tm.ImplicitRecordType = true
	}
}
