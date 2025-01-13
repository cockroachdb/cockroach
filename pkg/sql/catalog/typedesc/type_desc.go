// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package typedesc contains the concrete implementations of
// catalog.TypeDescriptor.
package typedesc

import (
	"bytes"
	"context"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ catalog.TypeDescriptor = (*immutable)(nil)
var _ catalog.EnumTypeDescriptor = (*immutable)(nil)
var _ catalog.RegionEnumTypeDescriptor = (*immutable)(nil)
var _ catalog.AliasTypeDescriptor = (*immutable)(nil)
var _ catalog.CompositeTypeDescriptor = (*immutable)(nil)
var _ catalog.TypeDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)

// MakeSimpleAlias creates a type descriptor that is an alias for the input
// type. It is intended to be used as an intermediate for name resolution, and
// should not be serialized and stored on disk.
func MakeSimpleAlias(typ *types.T, parentSchemaID descpb.ID) catalog.TypeDescriptor {
	return NewBuilder(&descpb.TypeDescriptor{
		// TODO(#sql-features): this should be attached to the current database.
		// We don't have a way of doing this yet (and virtual tables use some
		// fake magic).
		ParentID:       descpb.InvalidID,
		ParentSchemaID: parentSchemaID,
		Name:           typ.Name(),
		// TODO(#sql-features): give this a hardcoded alias.
		ID:    descpb.InvalidID,
		Kind:  descpb.TypeDescriptor_ALIAS,
		Alias: typ,
	}).BuildImmutableType()
}

// Mutable is a custom type for TypeDescriptors undergoing
// any types of modifications.
type Mutable struct {

	// TODO(ajwerner): Decide whether we're okay embedding the
	// immutable or whether we should be embedding some other base
	// struct that implements the various methods. For now we have the trap that
	// the code really wants direct field access and moving all access to
	// getters on an interface is a bigger task.
	immutable

	// ClusterVersion represents the version of the type descriptor read
	// from the store.
	ClusterVersion *immutable
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Mutable) IsUncommittedVersion() bool {
	return desc.IsNew() || desc.ClusterVersion.GetVersion() != desc.GetVersion()
}

// immutable is a custom type for wrapping TypeDescriptors
// when used in a read only way.
type immutable struct {
	descpb.TypeDescriptor

	// The fields below are used to fill user defined type metadata for ENUMs.
	logicalReps     []string
	physicalReps    [][]byte
	readOnlyMembers []bool

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool

	// changes represents how descriptor was changes	after
	// RunPostDeserializationChanges.
	changes catalog.PostDeserializationChanges

	// This is the raw bytes (tag + data) of the type descriptor in storage.
	rawBytesInStorage []byte
}

// UpdateCachedFieldsOnModifiedMutable refreshes the immutable field by
// reconstructing it. This means that the fields used to fill enumMetadata
// (readOnly, logicalReps, physicalReps) are reconstructed to reflect the
// modified Mutable's state. This allows us to hydrate tables correctly even
// when preceded by a type descriptor modification in the same transaction.
func UpdateCachedFieldsOnModifiedMutable(desc catalog.TypeDescriptor) (*Mutable, error) {
	mutable, ok := desc.(*Mutable)
	if !ok {
		return nil, errors.AssertionFailedf("type descriptor was not mutable")
	}
	mutable.immutable = *mutable.ImmutableCopy().(*immutable)
	return mutable, nil
}

// TableIDToImplicitTypeOID converts the given ID into the ID for the implicit
// reccord type for that table. We re-use the type ID to OID logic, as type IDs
// and table IDs do not share the same ID space. For virtual tables, we just use
// the virtual table ID itself, to avoid addition overflow.
func TableIDToImplicitTypeOID(id descpb.ID) oid.Oid {
	if descpb.IsVirtualTable(id) {
		// Virtual table OIDs start at max UInt32, so doing OID math would overflow.
		return oid.Oid(id)
	}
	return catid.TypeIDToOID(id)
}

// UserDefinedTypeOIDToID converts a user defined type OID into a descriptor ID.
// Returns zero when the OID is not for a user-defined type. If the OID is for
// a virtual table, then the ID itself is returned, since the type can then be
// assumed to be the implict record type for that table.
func UserDefinedTypeOIDToID(oid oid.Oid) descpb.ID {
	if descpb.IsVirtualTable(descpb.ID(oid)) {
		return descpb.ID(oid)
	}
	return catid.UserDefinedOIDToID(oid)
}

// GetUserDefinedTypeDescID gets the type descriptor ID from a user defined type.
func GetUserDefinedTypeDescID(t *types.T) descpb.ID {
	return UserDefinedTypeOIDToID(t.Oid())
}

// GetUserDefinedArrayTypeDescID gets the ID of the array type descriptor from a user
// defined type.
func GetUserDefinedArrayTypeDescID(t *types.T) descpb.ID {
	return UserDefinedTypeOIDToID(t.UserDefinedArrayOID())
}

// TypeDesc implements the Descriptor interface.
func (desc *immutable) TypeDesc() *descpb.TypeDescriptor {
	return &desc.TypeDescriptor
}

// Public implements the Descriptor interface.
func (desc *immutable) Public() bool {
	return desc.State == descpb.DescriptorState_PUBLIC
}

// Adding implements the Descriptor interface.
func (desc *immutable) Adding() bool {
	return false
}

// Offline implements the Descriptor interface.
func (desc *immutable) Offline() bool {
	return desc.State == descpb.DescriptorState_OFFLINE
}

// Dropped implements the Descriptor interface.
func (desc *immutable) Dropped() bool {
	return desc.State == descpb.DescriptorState_DROP
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// DescriptorProto returns a Descriptor for serialization.
func (desc *immutable) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Type{
			Type: &desc.TypeDescriptor,
		},
	}
}

// ByteSize implements the Descriptor interface.
func (desc *immutable) ByteSize() int64 {
	return int64(desc.Size())
}

// GetDeclarativeSchemaChangerState is part of the catalog.MutableDescriptor
// interface.
func (desc *immutable) GetDeclarativeSchemaChangerState() *scpb.DescriptorState {
	return desc.DeclarativeSchemaChangerState.Clone()
}

// NewBuilder implements the catalog.Descriptor interface.
//
// It overrides the wrapper's implementation to deal with the fact that
// mutable has overridden the definition of IsUncommittedVersion.
func (desc *Mutable) NewBuilder() catalog.DescriptorBuilder {
	b := newBuilder(desc.TypeDesc(), hlc.Timestamp{}, desc.IsUncommittedVersion(), desc.changes)
	b.SetRawBytesInStorage(desc.GetRawBytesInStorage())
	return b
}

// NewBuilder implements the catalog.Descriptor interface.
func (desc *immutable) NewBuilder() catalog.DescriptorBuilder {
	b := newBuilder(desc.TypeDesc(), hlc.Timestamp{}, desc.IsUncommittedVersion(), desc.changes)
	b.SetRawBytesInStorage(desc.GetRawBytesInStorage())
	return b
}

// PrimaryRegion implements the catalog.RegionEnumTypeDescriptor interface.
func (desc *immutable) PrimaryRegion() catpb.RegionName {
	return desc.RegionConfig.PrimaryRegion
}

// ForEachRegion implements the catalog.RegionEnumTypeDescriptor interface.
func (desc *immutable) ForEachRegion(
	f func(name catpb.RegionName, transition descpb.TypeDescriptor_EnumMember_Direction) error,
) error {
	for _, member := range desc.EnumMembers {
		if err := f(catpb.RegionName(member.LogicalRepresentation), member.Direction); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// ForEachPublicRegion implements the catalog.RegionEnumTypeDescriptor interface.
func (desc *immutable) ForEachPublicRegion(f func(name catpb.RegionName) error) error {
	for _, member := range desc.EnumMembers {
		if member.Capability == descpb.TypeDescriptor_EnumMember_READ_ONLY {
			continue
		}
		if err := f(catpb.RegionName(member.LogicalRepresentation)); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// GetAuditMode implements the DescriptorProto interface.
func (desc *immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// DescriptorType implements the catalog.Descriptor interface.
func (desc *immutable) DescriptorType() catalog.DescriptorType {
	return catalog.Type
}

// GetReplicatedPCRVersion is a part of the catalog.Descriptor
func (desc *immutable) GetReplicatedPCRVersion() descpb.DescriptorVersion {
	return desc.ReplicatedPCRVersion
}

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *Mutable) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.ClusterVersion == nil || desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ResetModificationTime()
}

// ResetModificationTime implements the catalog.MutableDescriptor interface.
func (desc *Mutable) ResetModificationTime() {
	desc.ModificationTime = hlc.Timestamp{}
}

// OriginalName implements the MutableDescriptor interface.
func (desc *Mutable) OriginalName() string {
	if desc.ClusterVersion == nil {
		return ""
	}
	return desc.ClusterVersion.Name
}

// OriginalID implements the MutableDescriptor interface.
func (desc *Mutable) OriginalID() descpb.ID {
	if desc.ClusterVersion == nil {
		return descpb.InvalidID
	}
	return desc.ClusterVersion.ID
}

// OriginalVersion implements the MutableDescriptor interface.
func (desc *Mutable) OriginalVersion() descpb.DescriptorVersion {
	if desc.ClusterVersion == nil {
		return 0
	}
	return desc.ClusterVersion.Version
}

// ImmutableCopy implements the MutableDescriptor interface.
func (desc *Mutable) ImmutableCopy() catalog.Descriptor {
	return desc.NewBuilder().(TypeDescriptorBuilder).BuildImmutableType()
}

// IsNew implements the MutableDescriptor interface.
func (desc *Mutable) IsNew() bool {
	return desc.ClusterVersion == nil
}

// SetPublic implements the MutableDescriptor interface.
func (desc *Mutable) SetPublic() {
	desc.State = descpb.DescriptorState_PUBLIC
	desc.OfflineReason = ""
}

// SetDropped implements the MutableDescriptor interface.
func (desc *Mutable) SetDropped() {
	desc.State = descpb.DescriptorState_DROP
	desc.OfflineReason = ""
}

// SetOffline implements the MutableDescriptor interface.
func (desc *Mutable) SetOffline(reason string) {
	desc.State = descpb.DescriptorState_OFFLINE
	desc.OfflineReason = reason
}

// DropEnumValue removes an enum member from the type.
// DropEnumValue assumes that the type is an enum and that the value being
// removed is in the prerequisite state to remove.
func (desc *Mutable) DropEnumValue(value tree.EnumValue) {
	for i := range desc.EnumMembers {
		member := &desc.EnumMembers[i]
		if member.LogicalRepresentation == string(value) {
			member.Capability = descpb.TypeDescriptor_EnumMember_READ_ONLY
			member.Direction = descpb.TypeDescriptor_EnumMember_REMOVE
			break
		}
	}
}

// AddEnumValue adds an enum member to the type.
// AddEnumValue assumes that the type is an enum, and that the new value
// doesn't exist already in the enum.
func (desc *Mutable) AddEnumValue(node *tree.AlterTypeAddValue) error {
	getPhysicalRep := func(idx int) []byte {
		if idx < 0 || idx >= len(desc.EnumMembers) {
			return nil
		}
		return desc.EnumMembers[idx].PhysicalRepresentation
	}

	// pos represents the spot to insert the new value. The new value will
	// be inserted between pos and pos+1. By default, values are inserted
	// at the end of the current list of members.
	pos := len(desc.EnumMembers) - 1
	if node.Placement != nil {
		// If the value was requested to be added before or after an existing
		// value, then find the index of where it should be inserted.
		foundIndex := -1
		existing := string(node.Placement.ExistingVal)
		for i, member := range desc.EnumMembers {
			if member.LogicalRepresentation == existing {
				foundIndex = i
			}
		}
		if foundIndex == -1 {
			return pgerror.Newf(pgcode.InvalidParameterValue, "%q is not an existing enum value", existing)
		}

		pos = foundIndex
		// If we were requested to insert before the element, shift pos down
		// one so that the desired element is the upper bound.
		if node.Placement.Before {
			pos--
		}
	}

	// Construct the new enum member. New enum values are added in the READ_ONLY
	// capability to ensure that they aren't written before all other nodes know
	// how to decode the physical representation.
	newPhysicalRep := enum.GenByteStringBetween(getPhysicalRep(pos), getPhysicalRep(pos+1), enum.SpreadSpacing)
	newMember := descpb.TypeDescriptor_EnumMember{
		LogicalRepresentation:  string(node.NewVal),
		PhysicalRepresentation: newPhysicalRep,
		Capability:             descpb.TypeDescriptor_EnumMember_READ_ONLY,
		Direction:              descpb.TypeDescriptor_EnumMember_ADD,
	}

	// Now, insert the new member.
	if len(desc.EnumMembers) == 0 {
		desc.EnumMembers = []descpb.TypeDescriptor_EnumMember{newMember}
	} else {
		if pos < 0 {
			// Insert the element in the front of the slice.
			desc.EnumMembers = append([]descpb.TypeDescriptor_EnumMember{newMember}, desc.EnumMembers...)
		} else {
			// Insert the element in front of pos.
			desc.EnumMembers = append(desc.EnumMembers, descpb.TypeDescriptor_EnumMember{})
			copy(desc.EnumMembers[pos+2:], desc.EnumMembers[pos+1:])
			desc.EnumMembers[pos+1] = newMember
		}
	}
	return nil
}

// AddReferencingDescriptorID adds a new referencing descriptor ID to the
// TypeDescriptor. It ensures that duplicates are not added.
func (desc *Mutable) AddReferencingDescriptorID(new descpb.ID) {
	for _, id := range desc.ReferencingDescriptorIDs {
		if new == id {
			return
		}
	}
	desc.ReferencingDescriptorIDs = append(desc.ReferencingDescriptorIDs, new)
}

// RemoveReferencingDescriptorID removes the desired referencing descriptor ID
// from the catalog.TypeDescriptor. It has no effect if the requested ID is not present.
func (desc *Mutable) RemoveReferencingDescriptorID(remove descpb.ID) {
	for i, id := range desc.ReferencingDescriptorIDs {
		if id == remove {
			desc.ReferencingDescriptorIDs = append(desc.ReferencingDescriptorIDs[:i], desc.ReferencingDescriptorIDs[i+1:]...)
			return
		}
	}
}

// SetParentSchemaID sets the SchemaID of the type.
func (desc *Mutable) SetParentSchemaID(schemaID descpb.ID) {
	desc.ParentSchemaID = schemaID
}

// SetName sets the catalog.TypeDescriptor's name.
func (desc *Mutable) SetName(name string) {
	desc.Name = name
}

// ValidateSelf performs validation on the catalog.TypeDescriptor.
func (desc *immutable) ValidateSelf(vea catalog.ValidationErrorAccumulator) {
	// Validate local properties of the descriptor.
	vea.Report(catalog.ValidateName(desc))
	if desc.GetID() == descpb.InvalidID {
		vea.Report(errors.AssertionFailedf("invalid ID %d", desc.GetID()))
	}
	if desc.GetParentID() == descpb.InvalidID {
		vea.Report(errors.AssertionFailedf("invalid parentID %d", desc.GetParentID()))
	}
	if desc.GetParentSchemaID() == descpb.InvalidID {
		vea.Report(errors.AssertionFailedf("invalid parent schema ID %d", desc.GetParentSchemaID()))
	}

	if desc.Privileges == nil {
		vea.Report(errors.AssertionFailedf("privileges not set"))
	} else if desc.Kind != descpb.TypeDescriptor_ALIAS {
		vea.Report(catprivilege.Validate(*desc.Privileges, desc, privilege.Type))
	}

	switch desc.Kind {
	case descpb.TypeDescriptor_MULTIREGION_ENUM:
		// Check presence of region config
		if desc.RegionConfig == nil {
			vea.Report(errors.AssertionFailedf("no region config on %s type desc", desc.Kind.String()))
		}
		if desc.validateEnumMembers(vea) {
			// In the case of the multi-region enum, we also keep the logical descriptors
			// sorted. Validate that's the case.
			for i := 0; i < len(desc.EnumMembers)-1; i++ {
				if desc.EnumMembers[i].LogicalRepresentation > desc.EnumMembers[i+1].LogicalRepresentation {
					vea.Report(errors.AssertionFailedf(
						"multi-region enum is out of order %q > %q",
						desc.EnumMembers[i].LogicalRepresentation,
						desc.EnumMembers[i+1].LogicalRepresentation,
					))
				}
			}
		}
	case descpb.TypeDescriptor_ENUM:
		if desc.RegionConfig != nil {
			vea.Report(errors.AssertionFailedf("found region config on %s type desc", desc.Kind.String()))
		}
		desc.validateEnumMembers(vea)
	case descpb.TypeDescriptor_ALIAS:
		if desc.RegionConfig != nil {
			vea.Report(errors.AssertionFailedf("found region config on %s type desc", desc.Kind.String()))
		}
		if desc.Alias == nil {
			vea.Report(errors.AssertionFailedf("ALIAS type desc has nil alias type"))
		}
		if desc.ArrayTypeID != descpb.InvalidID {
			vea.Report(errors.AssertionFailedf("ALIAS type desc has array type ID %d", desc.ArrayTypeID))
		}
	case descpb.TypeDescriptor_COMPOSITE:
		if desc.Composite == nil {
			vea.Report(errors.AssertionFailedf("COMPOSITE type desc has nil composite type"))
		}
	case descpb.TypeDescriptor_TABLE_IMPLICIT_RECORD_TYPE:
		vea.Report(errors.AssertionFailedf("invalid type descriptor: kind %s should never be serialized or validated", desc.Kind.String()))
	default:
		vea.Report(errors.AssertionFailedf("invalid type descriptor kind %s", desc.Kind.String()))
	}
}

// validateEnumMembers performs enum member checks.
// Returns true iff the enums are sorted.
func (desc *immutable) validateEnumMembers(vea catalog.ValidationErrorAccumulator) (isSorted bool) {
	// All of the enum members should be in sorted order.
	isSorted = slices.IsSortedFunc(desc.EnumMembers, func(a, b descpb.TypeDescriptor_EnumMember) int {
		return bytes.Compare(a.PhysicalRepresentation, b.PhysicalRepresentation)
	})
	if !isSorted {
		vea.Report(errors.AssertionFailedf("enum members are not sorted %v", desc.EnumMembers))
	}
	// Ensure there are no duplicate enum physical and logical reps.
	physicalMap := make(map[string]struct{}, len(desc.EnumMembers))
	logicalMap := make(map[string]struct{}, len(desc.EnumMembers))
	for _, member := range desc.EnumMembers {
		// Ensure there are no duplicate enum physical reps.
		_, duplicatePhysical := physicalMap[string(member.PhysicalRepresentation)]
		if duplicatePhysical {
			vea.Report(errors.AssertionFailedf("duplicate enum physical rep %v", member.PhysicalRepresentation))
		}
		physicalMap[string(member.PhysicalRepresentation)] = struct{}{}
		// Ensure there are no duplicate enum logical reps.
		_, duplicateLogical := logicalMap[member.LogicalRepresentation]
		if duplicateLogical {
			vea.Report(errors.AssertionFailedf("duplicate enum member %q", member.LogicalRepresentation))
		}
		logicalMap[member.LogicalRepresentation] = struct{}{}
		// Ensure the sanity of enum capabilities and transition directions.
		switch member.Capability {
		case descpb.TypeDescriptor_EnumMember_READ_ONLY:
			if member.Direction == descpb.TypeDescriptor_EnumMember_NONE {
				vea.Report(errors.AssertionFailedf(
					"read only capability member must have transition direction set"))
			}
		case descpb.TypeDescriptor_EnumMember_ALL:
			if member.Direction != descpb.TypeDescriptor_EnumMember_NONE {
				vea.Report(errors.AssertionFailedf("public enum member can not have transition direction set"))
			}
		default:
			vea.Report(errors.AssertionFailedf("invalid member capability %s", member.Capability))
		}
	}
	return isSorted
}

// GetReferencedDescIDs returns the IDs of all descriptors referenced by
// this descriptor, including itself.
func (desc *immutable) GetReferencedDescIDs(
	catalog.ValidationLevel,
) (catalog.DescriptorIDSet, error) {
	ids := catalog.MakeDescriptorIDSet(desc.GetReferencingDescriptorIDs()...)
	ids.Add(desc.GetParentID())
	// TODO(richardjcai): Remove logic for keys.PublicSchemaID in 22.2.
	if desc.GetParentSchemaID() != keys.PublicSchemaID {
		ids.Add(desc.GetParentSchemaID())
	}
	desc.GetIDClosure().ForEach(ids.Add)
	return ids, nil
}

// ValidateForwardReferences implements the catalog.Descriptor interface.
func (desc *immutable) ValidateForwardReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	// Validate the parentID.
	dbDesc, err := vdg.GetDatabaseDescriptor(desc.GetParentID())
	if err != nil {
		vea.Report(err)
	} else if dbDesc.Dropped() {
		vea.Report(errors.AssertionFailedf("parent database %q (%d) is dropped",
			dbDesc.GetName(), dbDesc.GetID()))
	}

	// Check that the parent schema exists.
	// TODO(richardjcai): Remove logic for keys.PublicSchemaID in 22.2.
	if desc.GetParentSchemaID() != keys.PublicSchemaID {
		schemaDesc, err := vdg.GetSchemaDescriptor(desc.GetParentSchemaID())
		vea.Report(err)
		if schemaDesc != nil && dbDesc != nil && schemaDesc.GetParentID() != dbDesc.GetID() {
			vea.Report(errors.AssertionFailedf("parent schema %d is in different database %d",
				desc.GetParentSchemaID(), schemaDesc.GetParentID()))
		}
		if schemaDesc != nil && schemaDesc.Dropped() {
			vea.Report(errors.AssertionFailedf("parent schema %q (%d) is dropped",
				schemaDesc.GetName(), schemaDesc.GetID()))
		}
	}

	if r := desc.AsRegionEnumTypeDescriptor(); r != nil && dbDesc != nil {
		validateMultiRegion(r, dbDesc, vea)
	}

	// Validate that the forward-referenced types exist.
	if a := desc.AsAliasTypeDescriptor(); a != nil && a.Aliased().UserDefined() {
		aliasedID := UserDefinedTypeOIDToID(a.Aliased().Oid())
		if typ, err := vdg.GetTypeDescriptor(aliasedID); err != nil {
			vea.Report(errors.Wrapf(err, "aliased type %d does not exist", aliasedID))
		} else if typ.Dropped() {
			vea.Report(errors.AssertionFailedf("aliased type %q (%d) is dropped", typ.GetName(), typ.GetID()))
		}
	}

	if c := desc.AsCompositeTypeDescriptor(); c != nil {
		for i := 0; i < c.NumElements(); i++ {
			t := c.GetElementType(i)
			if t.UserDefined() {
				// User-defined type references within user-defined types are currently
				// not supported, but this should be validated elsewhere.
				// See issue https://github.com/cockroachdb/cockroach/issues/91779.
				vea.Report(errors.AssertionFailedf("invalid reference to user-defined type %q from composite type %q",
					t.String(), desc.GetName(),
				))
			}
		}
	}
}

// ValidateBackReferences implements the catalog.Descriptor interface.
func (desc *immutable) ValidateBackReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {

	// Validate that the backward-referenced types exist.
	if e := desc.AsEnumTypeDescriptor(); e != nil {
		// Ensure that the array type exists.
		// This is considered to be a backward reference, not a forward reference,
		// as the element type doesn't need the array type to exist, but the
		// converse is not true.
		if typ, err := vdg.GetTypeDescriptor(e.GetArrayTypeID()); err != nil {
			vea.Report(errors.Wrapf(err, "arrayTypeID %d does not exist for %q", e.GetArrayTypeID(), e.GetKind()))
		} else if typ.Dropped() {
			vea.Report(errors.AssertionFailedf("array type %q (%d) is dropped", typ.GetName(), typ.GetID()))
		}
	}

	// Validate that all of the referencing descriptors exist.
	for _, id := range desc.GetReferencingDescriptorIDs() {
		depDesc, err := vdg.GetDescriptor(id)
		if err != nil {
			vea.Report(err)
			continue
		}
		switch depDesc.DescriptorType() {
		case catalog.Table, catalog.Function, catalog.Type:
			if depDesc.Dropped() {
				vea.Report(errors.AssertionFailedf(
					"referencing %s %d was dropped without dependency unlinking", depDesc.DescriptorType(), id))
			}
		default:
			vea.Report(errors.AssertionFailedf("type %s (%d) is depended on by unexpected %s %s (%d)",
				desc.GetName(), desc.GetID(), depDesc.DescriptorType(), depDesc.GetName(), depDesc.GetID()))
		}
	}
}

func validateMultiRegion(
	desc catalog.RegionEnumTypeDescriptor,
	dbDesc catalog.DatabaseDescriptor,
	vea catalog.ValidationErrorAccumulator,
) {
	// Parent database must be a multi-region database if it includes a
	// multi-region enum.
	if !dbDesc.IsMultiRegion() {
		vea.Report(errors.AssertionFailedf("parent database is not a multi-region database"))
		return
	}

	primaryRegion := desc.PrimaryRegion()

	{
		found := false
		for i := 0; i < desc.NumEnumMembers(); i++ {
			if catpb.RegionName(desc.GetMemberLogicalRepresentation(i)) == primaryRegion {
				found = true
			}
		}
		if !found {
			vea.Report(
				errors.AssertionFailedf("primary region %q not found in list of enum members",
					primaryRegion,
				))
		}
	}

	dbPrimaryRegion, err := dbDesc.PrimaryRegionName()
	if err != nil {
		vea.Report(err)
	}
	if dbPrimaryRegion != primaryRegion {
		vea.Report(errors.AssertionFailedf("unexpected primary region on db desc: %q expected %q",
			dbPrimaryRegion, primaryRegion))
	}

	var regionNames catpb.RegionNames
	_ = desc.ForEachPublicRegion(func(name catpb.RegionName) error {
		regionNames = append(regionNames, name)
		return nil
	})

	// The system database can be configured to be SURVIVE REGION without enough
	// regions. This would just mean that it will behave as SURVIVE ZONE until
	// enough regions are added by the user.
	if dbDesc.GetRegionConfig().SurvivalGoal == descpb.SurvivalGoal_REGION_FAILURE &&
		dbDesc.GetID() != keys.SystemDatabaseID {
		if len(regionNames) < 3 {
			vea.Report(
				errors.AssertionFailedf(
					"expected >= 3 regions, got %d: %s",
					len(regionNames),
					strings.Join(regionNames.ToStrings(), ","),
				),
			)
		}
	}

	superRegions := desc.TypeDesc().RegionConfig.SuperRegions
	multiregion.ValidateSuperRegions(superRegions, dbDesc.GetRegionConfig().SurvivalGoal, regionNames, func(err error) {
		vea.Report(err)
	})

	zoneCfgExtensions := desc.TypeDesc().RegionConfig.ZoneConfigExtensions
	multiregion.ValidateZoneConfigExtensions(regionNames, zoneCfgExtensions, func(err error) {
		vea.Report(err)
	})
}

// ValidateTxnCommit implements the catalog.Descriptor interface.
func (desc *immutable) ValidateTxnCommit(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
	// No-op.
}

// TypeLookupFunc is a type alias for a function that looks up a type by ID.
type TypeLookupFunc func(ctx context.Context, id descpb.ID) (tree.TypeName, catalog.TypeDescriptor, error)

// GetTypeDescriptor implements the catalog.TypeDescriptorResolver interface.
func (t TypeLookupFunc) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	return t(ctx, id)
}

// AsTypesT implements the catalog.TypeDescriptor interface.
func (desc *immutable) AsTypesT() *types.T {
	switch desc.Kind {
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		return types.MakeEnum(catid.TypeIDToOID(desc.GetID()), catid.TypeIDToOID(desc.ArrayTypeID))
	case descpb.TypeDescriptor_ALIAS:
		return desc.Alias.CopyForHydrate()
	case descpb.TypeDescriptor_COMPOSITE:
		contents := make([]*types.T, len(desc.Composite.Elements))
		labels := make([]string, len(desc.Composite.Elements))
		for i, e := range desc.Composite.Elements {
			contents[i] = e.ElementType.CopyForHydrate()
			labels[i] = e.ElementLabel
		}
		return types.NewCompositeType(
			catid.TypeIDToOID(desc.GetID()),
			catid.TypeIDToOID(desc.ArrayTypeID),
			contents,
			labels,
		)
	}
	panic(errors.AssertionFailedf("unsupported descriptor kind %s", desc.Kind.String()))
}

// NumEnumMembers implements the catalog.EnumTypeDescriptor interface.
func (desc *immutable) NumEnumMembers() int {
	return len(desc.EnumMembers)
}

// GetMemberPhysicalRepresentation implements the catalog.EnumTypeDescriptor interface.
func (desc *immutable) GetMemberPhysicalRepresentation(enumMemberOrdinal int) []byte {
	return desc.physicalReps[enumMemberOrdinal]
}

// GetMemberLogicalRepresentation implements the catalog.EnumTypeDescriptor interface.
func (desc *immutable) GetMemberLogicalRepresentation(enumMemberOrdinal int) string {
	return desc.logicalReps[enumMemberOrdinal]
}

// IsMemberReadOnly implements the catalog.EnumTypeDescriptor interface.
func (desc *immutable) IsMemberReadOnly(enumMemberOrdinal int) bool {
	return desc.readOnlyMembers[enumMemberOrdinal]
}

// NumReferencingDescriptors implements the catalog.TypeDescriptor interface.
func (desc *immutable) NumReferencingDescriptors() int {
	return len(desc.ReferencingDescriptorIDs)
}

// GetReferencingDescriptorID implements the catalog.TypeDescriptor interface.
func (desc *immutable) GetReferencingDescriptorID(refOrdinal int) descpb.ID {
	return desc.ReferencingDescriptorIDs[refOrdinal]
}

// IsCompatibleWith implements the catalog.TypeDescriptor interface.
func (desc *immutable) IsCompatibleWith(other catalog.TypeDescriptor) error {
	if desc.AsEnumTypeDescriptor() == nil {
		return errors.Newf("compatibility comparison unsupported for type kind %s", desc.GetKind())
	}
	if other.GetKind() != desc.GetKind() {
		return errors.Newf("%q of type %q is not compatible with type %q",
			other.GetName(), other.GetKind(), desc.GetKind())
	}
	e := other.AsEnumTypeDescriptor()
	// Every enum value in desc must be present in other, and all of the
	// physical representations must be the same.
	for _, thisMember := range desc.EnumMembers {
		var found bool
		for i := 0; i < e.NumEnumMembers(); i++ {
			if thisMember.LogicalRepresentation == e.GetMemberLogicalRepresentation(i) {
				// We've found a match. Now the physical representations must be
				// the same, otherwise the enums are incompatible.
				if !bytes.Equal(thisMember.PhysicalRepresentation, e.GetMemberPhysicalRepresentation(i)) {
					return errors.Newf(
						"%q has differing physical representation for value %q",
						e.GetName(),
						thisMember.LogicalRepresentation,
					)
				}
				found = true
			}
		}
		if !found {
			return errors.Newf(
				"could not find enum value %q in %q", thisMember.LogicalRepresentation, e.GetName())
		}
	}
	return nil
}

// HasPendingSchemaChanges implements the catalog.TypeDescriptor interface.
func (desc *immutable) HasPendingSchemaChanges() bool {
	switch desc.Kind {
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		// If there are any non-public enum members, then a type schema change is
		// needed to promote the members.
		for i := range desc.EnumMembers {
			if desc.EnumMembers[i].Capability != descpb.TypeDescriptor_EnumMember_ALL {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// GetPostDeserializationChanges implements the Descriptor interface.
func (desc *immutable) GetPostDeserializationChanges() catalog.PostDeserializationChanges {
	return desc.changes
}

// HasConcurrentSchemaChanges implements catalog.Descriptor.
func (desc *immutable) HasConcurrentSchemaChanges() bool {
	if desc.DeclarativeSchemaChangerState != nil &&
		desc.DeclarativeSchemaChangerState.JobID != catpb.InvalidJobID {
		return true
	}
	// Check if any enum members are transitioning, which should
	// block declarative jobs.
	for _, member := range desc.EnumMembers {
		if member.Direction != descpb.TypeDescriptor_EnumMember_NONE {
			return true
		}
	}
	// TODO(fqazi): In the future we may not have concurrent declarative schema
	// changes without a job ID. So, we should scan the elements involved for
	// types.
	return false
}

// ConcurrentSchemaChangeJobIDs implements catalog.Descriptor.
func (desc *immutable) ConcurrentSchemaChangeJobIDs() (ret []catpb.JobID) {
	if desc.DeclarativeSchemaChangerState != nil &&
		desc.DeclarativeSchemaChangerState.JobID != catpb.InvalidJobID {
		ret = append(ret, desc.DeclarativeSchemaChangerState.JobID)
	}
	return ret
}

// SkipNamespace implements the descriptor interface.
func (desc *immutable) SkipNamespace() bool {
	return false
}

// GetRawBytesInStorage implements the catalog.Descriptor interface.
func (desc *immutable) GetRawBytesInStorage() []byte {
	return desc.rawBytesInStorage
}

// ForEachUDTDependentForHydration implements the catalog.Descriptor interface.
func (desc *immutable) ForEachUDTDependentForHydration(fn func(t *types.T) error) error {
	if desc.Alias != nil && catid.IsOIDUserDefined(desc.Alias.Oid()) {
		if err := fn(desc.Alias); err != nil {
			return iterutil.Map(err)
		}
	}
	if desc.Composite == nil {
		return nil
	}
	for _, e := range desc.Composite.Elements {
		if !catid.IsOIDUserDefined(e.ElementType.Oid()) {
			continue
		}
		if err := fn(e.ElementType); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// MaybeRequiresTypeHydration implements the catalog.Descriptor interface.
func (desc *immutable) MaybeRequiresTypeHydration() bool {
	if desc.Alias != nil && catid.IsOIDUserDefined(desc.Alias.Oid()) {
		return true
	}
	if desc.Composite == nil {
		return false
	}
	for _, e := range desc.Composite.Elements {
		if catid.IsOIDUserDefined(e.ElementType.Oid()) {
			return true
		}
	}
	return false
}

// GetIDClosure implements the TypeDescriptor interface.
func (desc *immutable) GetIDClosure() (ret catalog.DescriptorIDSet) {
	// Collect the descriptor's own ID.
	ret.Add(desc.ID)
	switch desc.Kind {
	case descpb.TypeDescriptor_ALIAS:
		// If this descriptor is an alias for another type, then get collect the
		// closure for alias.
		GetTypeDescriptorClosure(desc.Alias).ForEach(ret.Add)
	case descpb.TypeDescriptor_COMPOSITE:
		for _, e := range desc.Composite.Elements {
			GetTypeDescriptorClosure(e.ElementType).ForEach(ret.Add)
		}
	default:
		// Otherwise, take the array type ID.
		ret.Add(desc.ArrayTypeID)
	}
	return ret
}

// GetObjectType implements the Object interface.
func (desc *immutable) GetObjectType() privilege.ObjectType {
	return privilege.Type
}

// GetObjectTypeString implements the Object interface.
func (desc *immutable) GetObjectTypeString() string {
	return string(privilege.Type)
}

// GetTypeDescriptorClosure returns all type descriptor IDs that are
// referenced by this input types.T.
func GetTypeDescriptorClosure(typ *types.T) (ret catalog.DescriptorIDSet) {
	if !typ.UserDefined() {
		return catalog.DescriptorIDSet{}
	}
	// Collect the type's descriptor ID.
	ret.Add(GetUserDefinedTypeDescID(typ))
	switch typ.Family() {
	case types.ArrayFamily:
		// If we have an array type, then collect all types in the contents.
		GetTypeDescriptorClosure(typ.ArrayContents()).ForEach(ret.Add)
	case types.TupleFamily:
		// If we have a tuple type, collect all types in the contents.
		for _, elt := range typ.TupleContents() {
			GetTypeDescriptorClosure(elt).ForEach(ret.Add)
		}
	default:
		// Otherwise, take the array type ID.
		ret.Add(GetUserDefinedArrayTypeDescID(typ))
	}
	return ret
}

// AsEnumTypeDescriptor implements the catalog.TypeDescriptor interface.
func (desc *immutable) AsEnumTypeDescriptor() catalog.EnumTypeDescriptor {
	if desc.Kind == descpb.TypeDescriptor_ENUM ||
		desc.Kind == descpb.TypeDescriptor_MULTIREGION_ENUM {
		return desc
	}
	return nil
}

// AsRegionEnumTypeDescriptor implements the catalog.TypeDescriptor interface.
func (desc *immutable) AsRegionEnumTypeDescriptor() catalog.RegionEnumTypeDescriptor {
	if desc.Kind == descpb.TypeDescriptor_MULTIREGION_ENUM {
		return desc
	}
	return nil
}

// AsAliasTypeDescriptor implements the catalog.TypeDescriptor interface.
func (desc *immutable) AsAliasTypeDescriptor() catalog.AliasTypeDescriptor {
	if desc.Kind == descpb.TypeDescriptor_ALIAS {
		return desc
	}
	return nil
}

// AsCompositeTypeDescriptor implements the catalog.TypeDescriptor interface.
func (desc *immutable) AsCompositeTypeDescriptor() catalog.CompositeTypeDescriptor {
	if desc.Kind == descpb.TypeDescriptor_COMPOSITE {
		return desc
	}
	return nil
}

// AsTableImplicitRecordTypeDescriptor implements the catalog.TypeDescriptor
// interface.
func (desc *immutable) AsTableImplicitRecordTypeDescriptor() catalog.TableImplicitRecordTypeDescriptor {
	return nil
}

// Aliased implements the catalog.AliasTypeDescriptor interface.
func (desc *immutable) Aliased() *types.T {
	return desc.Alias
}

// NumElements implements the catalog.CompositeTypeDescriptor interface.
func (desc *immutable) NumElements() int {
	return len(desc.Composite.Elements)
}

// GetElementLabel implements the catalog.CompositeTypeDescriptor interface.
func (desc *immutable) GetElementLabel(ordinal int) string {
	return desc.Composite.Elements[ordinal].ElementLabel
}

// GetElementType implements the catalog.CompositeTypeDescriptor interface.
func (desc *immutable) GetElementType(ordinal int) *types.T {
	return desc.Composite.Elements[ordinal].ElementType
}

// ForEachRegionInSuperRegion implements the catalog.RegionEnumTypeDescriptor
// interface.
func (desc *immutable) ForEachRegionInSuperRegion(
	superRegionName string, f func(region catpb.RegionName) error,
) error {
	for _, s := range desc.RegionConfig.SuperRegions {
		if superRegionName == s.SuperRegionName {
			for _, r := range s.Regions {
				if err := f(r); err != nil {
					return iterutil.Map(err)
				}
			}
			break
		}
	}
	return nil
}

// ForEachSuperRegion implements the catalog.RegionEnumTypeDescriptor
// interface.
func (desc *immutable) ForEachSuperRegion(f func(superRegionName string) error) error {
	for _, s := range desc.RegionConfig.SuperRegions {
		if err := f(s.SuperRegionName); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// SetDeclarativeSchemaChangerState is part of the catalog.MutableDescriptor
// interface.
func (desc *Mutable) SetDeclarativeSchemaChangerState(state *scpb.DescriptorState) {
	desc.DeclarativeSchemaChangerState = state
}
