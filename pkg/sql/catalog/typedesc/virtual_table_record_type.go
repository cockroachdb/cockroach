// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// VirtualTableRecordType is an implementation of catalog.TypeDescriptor that
// represents a record type for a particular table: meaning, the composite type
// that contains, in order, all of the visible columns for the table.
type VirtualTableRecordType struct {
	// desc is the TableDescriptor that this virtual record type is created from.
	// TODO(jordan): does it make more sense to eagerly pop out all the fields
	// that we care about from this backing TableDescriptor into the struct,
	// rather than have all of the methods delegate?
	desc catalog.TableDescriptor

	typ   *types.T
	privs *descpb.PrivilegeDescriptor
}

// CreateVirtualRecordTypeFromTableDesc creates a TypeDescriptor that represents
// the implicit record type for a table, which has 1 field for every visible
// column in the table.
func CreateVirtualRecordTypeFromTableDesc(
	descriptor catalog.TableDescriptor,
) (catalog.TypeDescriptor, error) {

	cols := descriptor.VisibleColumns()
	typs := make([]*types.T, len(cols))
	names := make([]string, len(cols))
	for i, col := range cols {
		if col.GetType().UserDefined() && !col.GetType().IsHydrated() {
			return nil, errors.AssertionFailedf("encountered unhydrated col %s while creating virtual record type from"+
				" table %s", col.ColName(), descriptor.GetName())
		}
		typs[i] = col.GetType()
		names[i] = col.GetName()
	}
	// The TypeDescriptor will be an alias to this Tuple type, which contains
	// all of the table's visible columns in order, labeled by the table's column
	// names.
	typ := types.MakeLabeledTuple(typs, names)
	tableID := descriptor.GetID()
	typeOID := TypeIDToOID(tableID)
	// Setting the type's OID allows us to properly report and display this type
	// as having ID <tableID> + 100000 in the pg_type table and ::REGTYPE casts.
	// It will also be used to serialize expressions casted to this type for
	// distribution with DistSQL. The receiver of such a serialized expression
	// will then be able to look up and rehydrate this type via the type cache.
	typ.InternalType.Oid = typeOID
	typ.TypeMeta = types.UserDefinedTypeMetadata{
		Name: &types.UserDefinedTypeName{
			Name: descriptor.GetName(),
		},
		Version: uint32(descriptor.GetVersion()),
	}

	tablePrivs := descriptor.GetPrivileges()
	newPrivs := make([]descpb.UserPrivileges, len(tablePrivs.Users))
	for i := range tablePrivs.Users {
		newPrivs[i].UserProto = tablePrivs.Users[i].UserProto
		// An table's record type has USAGE privs if a user has SELECT on the table.
		if privilege.SELECT.Mask()&tablePrivs.Users[i].Privileges != 0 {
			newPrivs[i].Privileges = newPrivs[i].Privileges | privilege.USAGE.Mask()
		}
	}

	return &VirtualTableRecordType{
		desc: descriptor,
		typ:  typ,
		privs: &descpb.PrivilegeDescriptor{
			Users:      newPrivs,
			OwnerProto: tablePrivs.OwnerProto,
			Version:    tablePrivs.Version,
		},
	}, nil
}

// GetName implements the NameKey interface.
func (v VirtualTableRecordType) GetName() string { return v.desc.GetName() }

// GetParentID implements the NameKey interface.
func (v VirtualTableRecordType) GetParentID() descpb.ID { return v.desc.GetParentID() }

// GetParentSchemaID implements the NameKey interface.
func (v VirtualTableRecordType) GetParentSchemaID() descpb.ID { return v.desc.GetParentSchemaID() }

// GetID implements the NameEntry interface.
func (v VirtualTableRecordType) GetID() descpb.ID { return v.desc.GetID() }

// IsUncommittedVersion implements the Descriptor interface.
func (v VirtualTableRecordType) IsUncommittedVersion() bool { return v.desc.IsUncommittedVersion() }

// GetVersion implements the Descriptor interface.
func (v VirtualTableRecordType) GetVersion() descpb.DescriptorVersion { return v.desc.GetVersion() }

// GetModificationTime implements the Descriptor interface.
func (v VirtualTableRecordType) GetModificationTime() hlc.Timestamp {
	return v.desc.GetModificationTime()
}

// GetDrainingNames implements the Descriptor interface.
func (v VirtualTableRecordType) GetDrainingNames() []descpb.NameInfo {
	// TODO(jordan): I don't think we need to return anything here, because this
	// descriptor doesn't really "own" any names on top of what the table already
	// does.
	return nil
}

// GetPrivileges implements the Descriptor interface.
func (v VirtualTableRecordType) GetPrivileges() *descpb.PrivilegeDescriptor {
	return v.privs
}

// DescriptorType implements the Descriptor interface.
func (v VirtualTableRecordType) DescriptorType() catalog.DescriptorType {
	return catalog.Type
}

// GetAuditMode implements the Descriptor interface.
func (v VirtualTableRecordType) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// Public implements the Descriptor interface.
func (v VirtualTableRecordType) Public() bool { return v.desc.Public() }

// Adding implements the Descriptor interface.
func (v VirtualTableRecordType) Adding() bool {
	v.panicNotSupported("Adding")
	return false
}

// Dropped implements the Descriptor interface.
func (v VirtualTableRecordType) Dropped() bool {
	v.panicNotSupported("Dropped")
	return false
}

// Offline implements the Descriptor interface.
func (v VirtualTableRecordType) Offline() bool {
	v.panicNotSupported("Offline")
	return false
}

// GetOfflineReason implements the Descriptor interface.
func (v VirtualTableRecordType) GetOfflineReason() string {
	v.panicNotSupported("GetOfflineReason")
	return ""
}

// DescriptorProto implements the Descriptor interface.
func (v VirtualTableRecordType) DescriptorProto() *descpb.Descriptor {
	v.panicNotSupported("DescriptorProto")
	return nil
}

// GetReferencedDescIDs implements the Descriptor interface.
func (v VirtualTableRecordType) GetReferencedDescIDs() (catalog.DescriptorIDSet, error) {
	return v.desc.GetReferencedDescIDs()
}

// ValidateSelf implements the Descriptor interface.
func (v VirtualTableRecordType) ValidateSelf(_ catalog.ValidationErrorAccumulator) {}

// ValidateCrossReferences implements the Descriptor interface.
func (v VirtualTableRecordType) ValidateCrossReferences(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}

// ValidateTxnCommit implements the Descriptor interface.
func (v VirtualTableRecordType) ValidateTxnCommit(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}

// TypeDesc implements the TypeDescriptor interface.
func (v VirtualTableRecordType) TypeDesc() *descpb.TypeDescriptor {
	v.panicNotSupported("TypeDesc")
	return nil
}

// HydrateTypeInfoWithName implements the TypeDescriptor interface.
func (v VirtualTableRecordType) HydrateTypeInfoWithName(
	ctx context.Context, typ *types.T, name *tree.TypeName, res catalog.TypeDescriptorResolver,
) error {
	if typ.IsHydrated() {
		return nil
	}
	typ.TypeMeta.Name = &types.UserDefinedTypeName{
		Catalog:        name.Catalog(),
		ExplicitSchema: name.ExplicitSchema,
		Schema:         name.Schema(),
		Name:           name.Object(),
	}
	typ.TypeMeta.Version = uint32(v.desc.GetVersion())
	for _, t := range typ.TupleContents() {
		if t.UserDefined() {
			if err := hydrateElementType(ctx, t, res); err != nil {
				return err
			}
		}
	}
	return nil
}

// MakeTypesT implements the TypeDescriptor interface.
func (v VirtualTableRecordType) MakeTypesT(
	_ context.Context, _ *tree.TypeName, _ catalog.TypeDescriptorResolver,
) (*types.T, error) {
	return v.typ, nil
}

// HasPendingSchemaChanges implements the TypeDescriptor interface.
func (v VirtualTableRecordType) HasPendingSchemaChanges() bool { return false }

// GetIDClosure implements the TypeDescriptor interface.
func (v VirtualTableRecordType) GetIDClosure() (map[descpb.ID]struct{}, error) {
	ret := make(map[descpb.ID]struct{})
	for _, elt := range v.typ.TupleContents() {
		children, err := GetTypeDescriptorClosure(elt)
		if err != nil {
			return nil, err
		}
		for id := range children {
			ret[id] = struct{}{}
		}
	}
	return ret, nil
}

// IsCompatibleWith implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) IsCompatibleWith(_ catalog.TypeDescriptor) error {
	return errors.AssertionFailedf("compatibility comparison unsupported for virtual table record types")
}

// PrimaryRegionName implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) PrimaryRegionName() (descpb.RegionName, error) {
	return "", errors.AssertionFailedf(
		"can not get primary region of a virtual table record type")
}

// RegionNames implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) RegionNames() (descpb.RegionNames, error) {
	return nil, errors.AssertionFailedf(
		"can not get region names of a virtual table record type")
}

// RegionNamesIncludingTransitioning implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) RegionNamesIncludingTransitioning() (descpb.RegionNames, error) {
	return nil, errors.AssertionFailedf(
		"can not get region names of a virtual table record type")
}

// RegionNamesForValidation implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) RegionNamesForValidation() (descpb.RegionNames, error) {
	return nil, errors.AssertionFailedf(
		"can not get region names of a virtual table record type")
}

// TransitioningRegionNames implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) TransitioningRegionNames() (descpb.RegionNames, error) {
	return nil, errors.AssertionFailedf(
		"can not get region names of a virtual table record type")
}

// GetArrayTypeID implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) GetArrayTypeID() descpb.ID {
	return 0
}

// GetKind implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) GetKind() descpb.TypeDescriptor_Kind {
	return descpb.TypeDescriptor_VIRTUAL_TABLE_RECORD_TYPE
}

// NumEnumMembers implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) NumEnumMembers() int { return 0 }

// GetMemberPhysicalRepresentation implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) GetMemberPhysicalRepresentation(_ int) []byte { return nil }

// GetMemberLogicalRepresentation implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) GetMemberLogicalRepresentation(_ int) string { return "" }

// IsMemberReadOnly implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) IsMemberReadOnly(_ int) bool { return false }

// NumReferencingDescriptors implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) NumReferencingDescriptors() int { return 0 }

// GetReferencingDescriptorID implements the TypeDescriptorInterface.
func (v VirtualTableRecordType) GetReferencingDescriptorID(_ int) descpb.ID { return 0 }

func (v VirtualTableRecordType) panicNotSupported(message string) {
	panic(errors.AssertionFailedf("virtual table record type for table %q: not supported: %s", v.GetName(), message))
}
