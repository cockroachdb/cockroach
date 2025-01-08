// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// tableImplicitRecordType is an implementation of catalog.TypeDescriptor that
// represents a record type for a particular table: meaning, the composite type
// that contains, in order, all of the visible columns for the table.
type tableImplicitRecordType struct {
	// desc is the TableDescriptor that this implicit record type is created from.
	desc catalog.TableDescriptor
	// privs holds the privileges for this implicit record type. It's calculated
	// by examining the privileges for the table that the record type corresponds
	// to, and providing the USAGE privilege if the table had the SELECT
	// privilege.
	privs *catpb.PrivilegeDescriptor
}

var _ catalog.TableImplicitRecordTypeDescriptor = (*tableImplicitRecordType)(nil)

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

	return &tableImplicitRecordType{
		desc: descriptor,
		privs: &catpb.PrivilegeDescriptor{
			Users:      newPrivs,
			OwnerProto: tablePrivs.OwnerProto,
			Version:    tablePrivs.Version,
		},
	}, nil
}

// GetName implements the catalog.NameKey interface.
func (v *tableImplicitRecordType) GetName() string { return v.desc.GetName() }

// GetParentID implements the catalog.NameKey interface.
func (v *tableImplicitRecordType) GetParentID() descpb.ID { return v.desc.GetParentID() }

// GetParentSchemaID implements the catalog.NameKey interface.
func (v *tableImplicitRecordType) GetParentSchemaID() descpb.ID { return v.desc.GetParentSchemaID() }

// GetID implements the NameEntry interface.
func (v *tableImplicitRecordType) GetID() descpb.ID { return v.desc.GetID() }

// IsUncommittedVersion implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) IsUncommittedVersion() bool { return v.desc.IsUncommittedVersion() }

// GetVersion implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) GetVersion() descpb.DescriptorVersion { return v.desc.GetVersion() }

// GetModificationTime implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) GetModificationTime() hlc.Timestamp {
	return v.desc.GetModificationTime()
}

// GetPrivileges implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) GetPrivileges() *catpb.PrivilegeDescriptor {
	return v.privs
}

// DescriptorType implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) DescriptorType() catalog.DescriptorType {
	return catalog.Type
}

// GetAuditMode implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// Public implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) Public() bool { return v.desc.Public() }

// Adding implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) Adding() bool {
	v.panicNotSupported("Adding")
	return false
}

// Dropped implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) Dropped() bool {
	return v.desc.Dropped()
}

// Offline implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) Offline() bool {
	v.panicNotSupported("Offline")
	return false
}

// GetOfflineReason implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) GetOfflineReason() string {
	v.panicNotSupported("GetOfflineReason")
	return ""
}

// DescriptorProto implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) DescriptorProto() *descpb.Descriptor {
	v.panicNotSupported("DescriptorProto")
	return nil
}

// ByteSize implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) ByteSize() int64 {
	mem := v.desc.ByteSize()
	if v.privs != nil {
		mem += int64(v.privs.Size())
	}
	return mem
}

// NewBuilder implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) NewBuilder() catalog.DescriptorBuilder {
	v.panicNotSupported("NewBuilder")
	return nil
}

// GetReferencedDescIDs implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) GetReferencedDescIDs(
	catalog.ValidationLevel,
) (catalog.DescriptorIDSet, error) {
	return catalog.DescriptorIDSet{}, errors.AssertionFailedf(
		"GetReferencedDescIDs are unsupported for implicit table record types")
}

// ValidateSelf implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) ValidateSelf(_ catalog.ValidationErrorAccumulator) {
}

// ValidateForwardReferences implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) ValidateForwardReferences(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}

// ValidateBackReferences implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) ValidateBackReferences(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}

// ValidateTxnCommit implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) ValidateTxnCommit(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
}

// GetRawBytesInStorage implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) GetRawBytesInStorage() []byte {
	return nil
}

// ForEachUDTDependentForHydration implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) ForEachUDTDependentForHydration(_ func(t *types.T) error) error {
	return nil
}

// MaybeRequiresTypeHydration implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) MaybeRequiresTypeHydration() bool { return false }

// TypeDesc implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) TypeDesc() *descpb.TypeDescriptor {
	v.panicNotSupported("TypeDesc")
	return nil
}

// AsTypesT implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) AsTypesT() *types.T {
	cols := v.desc.VisibleColumns()
	typs := make([]*types.T, len(cols))
	names := make([]string, len(cols))
	for i, col := range cols {
		typs[i] = col.GetType().CopyForHydrate()
		names[i] = col.GetName()
	}
	// the catalog.TypeDescriptor will be an alias to this Tuple type, which contains
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

// HasPendingSchemaChanges implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) HasPendingSchemaChanges() bool { return false }

// GetIDClosure implements the TypeDescriptor interface.
func (v *tableImplicitRecordType) GetIDClosure() catalog.DescriptorIDSet {
	v.panicNotSupported("GetIDClosure")
	return catalog.DescriptorIDSet{}
}

// IsCompatibleWith implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) IsCompatibleWith(_ catalog.TypeDescriptor) error {
	return errors.AssertionFailedf("compatibility comparison unsupported for implicit table record types")
}

// GetArrayTypeID implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) GetArrayTypeID() descpb.ID {
	return 0
}

// GetKind implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) GetKind() descpb.TypeDescriptor_Kind {
	return descpb.TypeDescriptor_TABLE_IMPLICIT_RECORD_TYPE
}

// NumEnumMembers implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) NumEnumMembers() int { return 0 }

// GetMemberPhysicalRepresentation implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) GetMemberPhysicalRepresentation(_ int) []byte { return nil }

// GetMemberLogicalRepresentation implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) GetMemberLogicalRepresentation(_ int) string { return "" }

// IsMemberReadOnly implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) IsMemberReadOnly(_ int) bool { return false }

// NumReferencingDescriptors implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) NumReferencingDescriptors() int { return 0 }

// GetReferencingDescriptorID implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) GetReferencingDescriptorID(_ int) descpb.ID { return 0 }

// GetPostDeserializationChanges implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) GetPostDeserializationChanges() catalog.PostDeserializationChanges {
	return catalog.PostDeserializationChanges{}
}

// HasConcurrentSchemaChanges implements catalog.Descriptor.
func (v *tableImplicitRecordType) HasConcurrentSchemaChanges() bool {
	return false
}

// ConcurrentSchemaChangeJobIDs implements catalog.Descriptor.
func (v *tableImplicitRecordType) ConcurrentSchemaChangeJobIDs() []catpb.JobID {
	return nil
}

// SkipNamespace implements catalog.Descriptor. We never store table implicit
// record type which is always constructed in memory.
func (v *tableImplicitRecordType) SkipNamespace() bool {
	return true
}

func (v *tableImplicitRecordType) panicNotSupported(message string) {
	panic(errors.AssertionFailedf("implicit table record type for table %q: not supported: %s", v.GetName(), message))
}

// GetDeclarativeSchemaChangerState implements the catalog.Descriptor interface.
func (v *tableImplicitRecordType) GetDeclarativeSchemaChangerState() *scpb.DescriptorState {
	v.panicNotSupported("GetDeclarativeSchemaChangeState")
	return nil
}

// GetObjectType implements the Object interface.
func (v *tableImplicitRecordType) GetObjectType() privilege.ObjectType {
	v.panicNotSupported("GetObjectType")
	return ""
}

// GetObjectTypeString implements the Object interface.
func (v *tableImplicitRecordType) GetObjectTypeString() string {
	v.panicNotSupported("GetObjectTypeString")
	return ""
}

// AsEnumTypeDescriptor implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) AsEnumTypeDescriptor() catalog.EnumTypeDescriptor {
	return nil
}

// AsRegionEnumTypeDescriptor implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) AsRegionEnumTypeDescriptor() catalog.RegionEnumTypeDescriptor {
	return nil
}

// AsAliasTypeDescriptor implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) AsAliasTypeDescriptor() catalog.AliasTypeDescriptor {
	return nil
}

// AsCompositeTypeDescriptor implements the catalog.TypeDescriptor interface.
func (v *tableImplicitRecordType) AsCompositeTypeDescriptor() catalog.CompositeTypeDescriptor {
	return nil
}

// AsTableImplicitRecordTypeDescriptor implements the catalog.TypeDescriptor
// interface.
func (v *tableImplicitRecordType) AsTableImplicitRecordTypeDescriptor() catalog.TableImplicitRecordTypeDescriptor {
	return v
}

// UnderlyingTableDescriptor implements the
// catalog.TableImplicitRecordTypeDescriptor interface.
func (v *tableImplicitRecordType) UnderlyingTableDescriptor() catalog.TableDescriptor {
	return v.desc
}

// GetReplicatedPCRVersion is a part of the catalog.Descriptor
func (v *tableImplicitRecordType) GetReplicatedPCRVersion() descpb.DescriptorVersion {
	return v.desc.GetReplicatedPCRVersion()
}
