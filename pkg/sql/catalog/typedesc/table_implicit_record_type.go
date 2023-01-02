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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// TableImplicitRecordType is an implementation of catalog.TypeDescriptor that
// represents a record type for a particular table: meaning, the composite type
// that contains, in order, all of the visible columns for the table.
type TableImplicitRecordType struct {
	// desc is the TableDescriptor that this implicit record type is created from.
	desc catalog.TableDescriptor
	// privs holds the privileges for this implicit record type. It's calculated
	// by examining the privileges for the table that the record type corresponds
	// to, and providing the USAGE privilege if the table had the SELECT
	// privilege.
	privs *catpb.PrivilegeDescriptor
}

var _ catalog.TypeDescriptor = (*TableImplicitRecordType)(nil)

// CreateImplicitRecordTypeFromTableDesc creates a TypeDescriptor that represents
// the implicit record type for a table, which has 1 field for every visible
// column in the table.
func CreateImplicitRecordTypeFromTableDesc(
	descriptor catalog.TableDescriptor,
) (catalog.TypeDescriptor, error) {

	// Note: Implicit types for virtual tables are hardcoded to have USAGE
	// privileges and this can't be modified. The virtual table itself does have
	// synthetic privileges (as of v22.2), but accessing those requires a planner
	// by using getPrivilegeDescriptor.
	// It is fine to hardcode USAGE for implicit types for virtual tables since
	// nothing about those types is sensitive.
	tablePrivs := descriptor.GetPrivileges()
	newPrivs := make([]catpb.UserPrivileges, len(tablePrivs.Users))
	for i := range tablePrivs.Users {
		newPrivs[i].UserProto = tablePrivs.Users[i].UserProto
		// A table's record type has USAGE privs if a user has SELECT on the table.
		if privilege.SELECT.IsSetIn(tablePrivs.Users[i].Privileges) {
			newPrivs[i].Privileges = privilege.USAGE.Mask()
		}
	}

	return &TableImplicitRecordType{
		desc: descriptor,
		privs: &catpb.PrivilegeDescriptor{
			Users:      newPrivs,
			OwnerProto: tablePrivs.OwnerProto,
			Version:    tablePrivs.Version,
		},
	}, nil
}

// GetName implements the Namespace interface.
func (v TableImplicitRecordType) GetName() string { return v.desc.GetName() }

// GetParentID implements the Namespace interface.
func (v TableImplicitRecordType) GetParentID() descpb.ID { return v.desc.GetParentID() }

// GetParentSchemaID implements the Namespace interface.
func (v TableImplicitRecordType) GetParentSchemaID() descpb.ID { return v.desc.GetParentSchemaID() }

// GetID implements the NameEntry interface.
func (v TableImplicitRecordType) GetID() descpb.ID { return v.desc.GetID() }

// IsUncommittedVersion implements the Descriptor interface.
func (v TableImplicitRecordType) IsUncommittedVersion() bool { return v.desc.IsUncommittedVersion() }

// GetVersion implements the Descriptor interface.
func (v TableImplicitRecordType) GetVersion() descpb.DescriptorVersion { return v.desc.GetVersion() }

// GetModificationTime implements the Descriptor interface.
func (v TableImplicitRecordType) GetModificationTime() hlc.Timestamp {
	return v.desc.GetModificationTime()
}

// GetPrivileges implements the Descriptor interface.
func (v TableImplicitRecordType) GetPrivileges() *catpb.PrivilegeDescriptor {
	return v.privs
}

// DescriptorType implements the Descriptor interface.
func (v TableImplicitRecordType) DescriptorType() catalog.DescriptorType {
	return catalog.Type
}

// GetAuditMode implements the Descriptor interface.
func (v TableImplicitRecordType) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// Public implements the Descriptor interface.
func (v TableImplicitRecordType) Public() bool { return v.desc.Public() }

// Adding implements the Descriptor interface.
func (v TableImplicitRecordType) Adding() bool {
	v.panicNotSupported("Adding")
	return false
}

// Dropped implements the Descriptor interface.
func (v TableImplicitRecordType) Dropped() bool {
	v.panicNotSupported("Dropped")
	return false
}

// Offline implements the Descriptor interface.
func (v TableImplicitRecordType) Offline() bool {
	v.panicNotSupported("Offline")
	return false
}

// GetOfflineReason implements the Descriptor interface.
func (v TableImplicitRecordType) GetOfflineReason() string {
	v.panicNotSupported("GetOfflineReason")
	return ""
}

// DescriptorProto implements the Descriptor interface.
func (v TableImplicitRecordType) DescriptorProto() *descpb.Descriptor {
	v.panicNotSupported("DescriptorProto")
	return nil
}

// ByteSize implements the Descriptor interface.
func (v TableImplicitRecordType) ByteSize() int64 {
	mem := v.desc.ByteSize()
	if v.privs != nil {
		mem += int64(v.privs.Size())
	}
	return mem
}

// NewBuilder implements the Descriptor interface.
func (v TableImplicitRecordType) NewBuilder() catalog.DescriptorBuilder {
	v.panicNotSupported("NewBuilder")
	return nil
}

// GetReferencedDescIDs implements the Descriptor interface.
func (v TableImplicitRecordType) GetReferencedDescIDs() (catalog.DescriptorIDSet, error) {
	return catalog.DescriptorIDSet{}, errors.AssertionFailedf(
		"GetReferencedDescIDs are unsupported for implicit table record types")
}

// ValidateSelf implements the Descriptor interface.
func (v TableImplicitRecordType) ValidateSelf(_ catalog.ValidationErrorAccumulator) {
}

// ValidateForwardReferences implements the Descriptor interface.
func (v TableImplicitRecordType) ValidateForwardReferences(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}

// ValidateBackReferences implements the Descriptor interface.
func (v TableImplicitRecordType) ValidateBackReferences(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}

// ValidateTxnCommit implements the Descriptor interface.
func (v TableImplicitRecordType) ValidateTxnCommit(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}

// GetRawBytesInStorage implements the catalog.Descriptor interface.
func (v TableImplicitRecordType) GetRawBytesInStorage() []byte {
	return nil
}

// ForEachUDTDependentForHydration implements the catalog.Descriptor interface.
func (v TableImplicitRecordType) ForEachUDTDependentForHydration(_ func(t *types.T) error) error {
	return nil
}

// TypeDesc implements the TypeDescriptor interface.
func (v TableImplicitRecordType) TypeDesc() *descpb.TypeDescriptor {
	v.panicNotSupported("TypeDesc")
	return nil
}

// AsTypesT implements the TypeDescriptor interface.
func (v TableImplicitRecordType) AsTypesT() *types.T {
	cols := v.desc.VisibleColumns()
	typs := make([]*types.T, len(cols))
	names := make([]string, len(cols))
	for i, col := range cols {
		typs[i] = col.GetType()
		names[i] = col.GetName()
	}
	// The TypeDescriptor will be an alias to this Tuple type, which contains
	// all of the table's visible columns in order, labeled by the table's column
	// names.
	typ := types.MakeLabeledTuple(typs, names)
	tableID := v.desc.GetID()
	typeOID := TableIDToImplicitTypeOID(tableID)
	// Setting the type's OID allows us to properly report and display this type
	// as having ID <tableID> + 100000 in the pg_type table and ::REGTYPE casts.
	// It will also be used to serialize expressions casted to this type for
	// distribution with DistSQL. The receiver of such a serialized expression
	// will then be able to look up and rehydrate this type via the type cache.
	typ.InternalType.Oid = typeOID
	typ.TypeMeta = types.UserDefinedTypeMetadata{
		Name: &types.UserDefinedTypeName{
			Name: v.desc.GetName(),
		},
		Version:            uint32(v.desc.GetVersion()),
		ImplicitRecordType: true,
	}
	return typ
}

// HasPendingSchemaChanges implements the TypeDescriptor interface.
func (v TableImplicitRecordType) HasPendingSchemaChanges() bool { return false }

// GetIDClosure implements the TypeDescriptor interface.
func (v TableImplicitRecordType) GetIDClosure() (map[descpb.ID]struct{}, error) {
	return nil, errors.AssertionFailedf("IDClosure unsupported for implicit table record types")
}

// IsCompatibleWith implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) IsCompatibleWith(_ catalog.TypeDescriptor) error {
	return errors.AssertionFailedf("compatibility comparison unsupported for implicit table record types")
}

// PrimaryRegionName implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) PrimaryRegionName() (catpb.RegionName, error) {
	return "", errors.AssertionFailedf(
		"can not get primary region of a implicit table record type")
}

// RegionNames implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) RegionNames() (catpb.RegionNames, error) {
	return nil, errors.AssertionFailedf(
		"can not get region names of a implicit table record type")
}

// RegionNamesIncludingTransitioning implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) RegionNamesIncludingTransitioning() (catpb.RegionNames, error) {
	return nil, errors.AssertionFailedf(
		"can not get region names of a implicit table record type")
}

// RegionNamesForValidation implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) RegionNamesForValidation() (catpb.RegionNames, error) {
	return nil, errors.AssertionFailedf(
		"can not get region names of a implicit table record type")
}

// TransitioningRegionNames implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) TransitioningRegionNames() (catpb.RegionNames, error) {
	return nil, errors.AssertionFailedf(
		"can not get region names of a implicit table record type")
}

// SuperRegions implements the TypeDescriptor interface.
func (v TableImplicitRecordType) SuperRegions() ([]descpb.SuperRegion, error) {
	return nil, errors.AssertionFailedf(
		"can not get super regions of a implicit table record type",
	)
}

// ZoneConfigExtensions implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) ZoneConfigExtensions() (descpb.ZoneConfigExtensions, error) {
	return descpb.ZoneConfigExtensions{}, errors.AssertionFailedf(
		"can not get the zone config extensions of a implicit table record type")
}

// GetArrayTypeID implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) GetArrayTypeID() descpb.ID {
	return 0
}

// GetKind implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) GetKind() descpb.TypeDescriptor_Kind {
	return descpb.TypeDescriptor_TABLE_IMPLICIT_RECORD_TYPE
}

// NumEnumMembers implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) NumEnumMembers() int { return 0 }

// GetMemberPhysicalRepresentation implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) GetMemberPhysicalRepresentation(_ int) []byte { return nil }

// GetMemberLogicalRepresentation implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) GetMemberLogicalRepresentation(_ int) string { return "" }

// IsMemberReadOnly implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) IsMemberReadOnly(_ int) bool { return false }

// NumReferencingDescriptors implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) NumReferencingDescriptors() int { return 0 }

// GetReferencingDescriptorID implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) GetReferencingDescriptorID(_ int) descpb.ID { return 0 }

// GetReferencingDescriptorIDs implements the TypeDescriptorInterface.
func (v TableImplicitRecordType) GetReferencingDescriptorIDs() []descpb.ID { return nil }

// GetPostDeserializationChanges implements the Descriptor interface.
func (v TableImplicitRecordType) GetPostDeserializationChanges() catalog.PostDeserializationChanges {
	return catalog.PostDeserializationChanges{}
}

// HasConcurrentSchemaChanges implements catalog.Descriptor.
func (v TableImplicitRecordType) HasConcurrentSchemaChanges() bool {
	return false
}

// SkipNamespace implements catalog.Descriptor. We never store table implicit
// record type which is always constructed in memory.
func (v TableImplicitRecordType) SkipNamespace() bool {
	return true
}

func (v TableImplicitRecordType) panicNotSupported(message string) {
	panic(errors.AssertionFailedf("implicit table record type for table %q: not supported: %s", v.GetName(), message))
}

// GetDeclarativeSchemaChangerState implements the Descriptor interface.
func (v TableImplicitRecordType) GetDeclarativeSchemaChangerState() *scpb.DescriptorState {
	v.panicNotSupported("GetDeclarativeSchemaChangeState")
	return nil
}

// GetObjectType implements the Object interface.
func (v TableImplicitRecordType) GetObjectType() privilege.ObjectType {
	v.panicNotSupported("GetObjectType")
	return ""
}
