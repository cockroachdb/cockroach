// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package typedesc contains the concrete implementations of
// catalog.TypeDescriptor.
package typedesc

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ catalog.TypeDescriptor = (*immutable)(nil)
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

// TypeIDToOID converts a type descriptor ID into a type OID.
func TypeIDToOID(id descpb.ID) oid.Oid {
	return oid.Oid(id) + oidext.CockroachPredefinedOIDMax
}

// UserDefinedTypeOIDToID converts a user defined type OID into a
// descriptor ID. OID of a user-defined type must be greater than
// CockroachPredefinedOIDMax. The function returns an error if the
// given OID is less than or equals to CockroachPredefinedMax.
func UserDefinedTypeOIDToID(oid oid.Oid) (descpb.ID, error) {
	if descpb.ID(oid) <= oidext.CockroachPredefinedOIDMax {
		return 0, errors.Newf("user-defined OID %d should be greater "+
			"than predefined Max: %d.", oid, oidext.CockroachPredefinedOIDMax)
	}
	return descpb.ID(oid) - oidext.CockroachPredefinedOIDMax, nil
}

// GetUserDefinedTypeDescID gets the type descriptor ID from a user defined type.
func GetUserDefinedTypeDescID(t *types.T) (descpb.ID, error) {
	return UserDefinedTypeOIDToID(t.Oid())
}

// GetUserDefinedArrayTypeDescID gets the ID of the array type descriptor from a user
// defined type.
func GetUserDefinedArrayTypeDescID(t *types.T) (descpb.ID, error) {
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

// NewBuilder implements the catalog.Descriptor interface.
//
// It overrides the wrapper's implementation to deal with the fact that
// mutable has overridden the definition of IsUncommittedVersion.
func (desc *Mutable) NewBuilder() catalog.DescriptorBuilder {
	return newBuilder(desc.TypeDesc(), desc.IsUncommittedVersion(), desc.changes)
}

// NewBuilder implements the catalog.Descriptor interface.
func (desc *immutable) NewBuilder() catalog.DescriptorBuilder {
	return newBuilder(desc.TypeDesc(), desc.IsUncommittedVersion(), desc.changes)
}

// PrimaryRegionName implements the TypeDescriptor interface.
func (desc *immutable) PrimaryRegionName() (catpb.RegionName, error) {
	if desc.Kind != descpb.TypeDescriptor_MULTIREGION_ENUM {
		return "", errors.AssertionFailedf(
			"can not get primary region of a non multi-region enum")
	}
	return desc.RegionConfig.PrimaryRegion, nil
}

// RegionNames implements the TypeDescriptor interface.
func (desc *immutable) RegionNames() (catpb.RegionNames, error) {
	if desc.Kind != descpb.TypeDescriptor_MULTIREGION_ENUM {
		return nil, errors.AssertionFailedf(
			"can not get regions of a non multi-region enum %d", desc.ID,
		)
	}
	var regions catpb.RegionNames
	for _, member := range desc.EnumMembers {
		if member.Capability == descpb.TypeDescriptor_EnumMember_READ_ONLY {
			continue
		}
		regions = append(regions, catpb.RegionName(member.LogicalRepresentation))
	}
	return regions, nil
}

// TransitioningRegionNames implements the TypeDescriptor interface.
func (desc *immutable) TransitioningRegionNames() (catpb.RegionNames, error) {
	if desc.Kind != descpb.TypeDescriptor_MULTIREGION_ENUM {
		return nil, errors.AssertionFailedf(
			"can not get regions of a non multi-region enum %d", desc.ID,
		)
	}
	var regions catpb.RegionNames
	for _, member := range desc.EnumMembers {
		if member.Direction != descpb.TypeDescriptor_EnumMember_NONE {
			regions = append(regions, catpb.RegionName(member.LogicalRepresentation))
		}
	}
	return regions, nil
}

// RegionNamesForValidation implements the TypeDescriptor interface.
func (desc *immutable) RegionNamesForValidation() (catpb.RegionNames, error) {
	if desc.Kind != descpb.TypeDescriptor_MULTIREGION_ENUM {
		return nil, errors.AssertionFailedf(
			"can not get regions of a non multi-region enum %d", desc.ID,
		)
	}
	var regions catpb.RegionNames
	for _, member := range desc.EnumMembers {
		if member.Capability == descpb.TypeDescriptor_EnumMember_READ_ONLY &&
			member.Direction == descpb.TypeDescriptor_EnumMember_ADD {
			continue
		}
		regions = append(regions, catpb.RegionName(member.LogicalRepresentation))
	}
	return regions, nil
}

// RegionNamesIncludingTransitioning implements the TypeDescriptor interface.
func (desc *immutable) RegionNamesIncludingTransitioning() (catpb.RegionNames, error) {
	if desc.Kind != descpb.TypeDescriptor_MULTIREGION_ENUM {
		return nil, errors.AssertionFailedf(
			"can not get regions of a non multi-region enum %d", desc.ID,
		)
	}
	var regions catpb.RegionNames
	for _, member := range desc.EnumMembers {
		regions = append(regions, catpb.RegionName(member.LogicalRepresentation))
	}
	return regions, nil
}

// SetDrainingNames implements the MutableDescriptor interface.
//
// Deprecated: Do not use.
func (desc *Mutable) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}

// GetAuditMode implements the DescriptorProto interface.
func (desc *immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// DescriptorType implements the catalog.Descriptor interface.
func (desc *immutable) DescriptorType() catalog.DescriptorType {
	return catalog.Type
}

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *Mutable) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.ClusterVersion == nil || desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
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
// from the TypeDescriptor. It has no effect if the requested ID is not present.
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

// AddDrainingName adds a draining name to the TypeDescriptor's slice of
// draining names.
//
// Deprecated: Do not use.
func (desc *Mutable) AddDrainingName(name descpb.NameInfo) {
	desc.DrainingNames = append(desc.DrainingNames, name)
}

// SetName sets the TypeDescriptor's name.
func (desc *Mutable) SetName(name string) {
	desc.Name = name
}

// EnumMembers is a sortable list of TypeDescriptor_EnumMember, sorted by the
// physical representation.
type EnumMembers []descpb.TypeDescriptor_EnumMember

func (e EnumMembers) Len() int { return len(e) }
func (e EnumMembers) Less(i, j int) bool {
	return bytes.Compare(e[i].PhysicalRepresentation, e[j].PhysicalRepresentation) < 0
}
func (e EnumMembers) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

// ValidateSelf performs validation on the TypeDescriptor.
func (desc *immutable) ValidateSelf(vea catalog.ValidationErrorAccumulator) {
	// Validate local properties of the descriptor.
	vea.Report(catalog.ValidateName(desc.Name, "type"))
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
		if desc.GetArrayTypeID() != descpb.InvalidID {
			vea.Report(errors.AssertionFailedf("ALIAS type desc has array type ID %d", desc.GetArrayTypeID()))
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
	isSorted = sort.IsSorted(EnumMembers(desc.EnumMembers))
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
func (desc *immutable) GetReferencedDescIDs() (catalog.DescriptorIDSet, error) {
	ids := catalog.MakeDescriptorIDSet(desc.GetReferencingDescriptorIDs()...)
	ids.Add(desc.GetParentID())
	// TODO(richardjcai): Remove logic for keys.PublicSchemaID in 22.2.
	if desc.GetParentSchemaID() != keys.PublicSchemaID {
		ids.Add(desc.GetParentSchemaID())
	}
	children, err := desc.GetIDClosure()
	if err != nil {
		return catalog.DescriptorIDSet{}, err
	}
	for id := range children {
		ids.Add(id)
	}
	return ids, nil
}

// ValidateCrossReferences performs cross reference checks on the type descriptor.
func (desc *immutable) ValidateCrossReferences(
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

	if desc.GetKind() == descpb.TypeDescriptor_MULTIREGION_ENUM && dbDesc != nil {
		desc.validateMultiRegion(dbDesc, vea)
	}

	// Validate that the referenced types exist.
	switch desc.GetKind() {
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		// Ensure that the referenced array type exists.
		if typ, err := vdg.GetTypeDescriptor(desc.GetArrayTypeID()); err != nil {
			vea.Report(errors.Wrapf(err, "arrayTypeID %d does not exist for %q", desc.GetArrayTypeID(), desc.GetKind()))
		} else if typ.Dropped() {
			vea.Report(errors.AssertionFailedf("array type %q (%d) is dropped", typ.GetName(), typ.GetID()))
		}
	case descpb.TypeDescriptor_ALIAS:
		if desc.GetAlias().UserDefined() {
			aliasedID, err := UserDefinedTypeOIDToID(desc.GetAlias().Oid())
			if err != nil {
				vea.Report(err)
			}
			if typ, err := vdg.GetTypeDescriptor(aliasedID); err != nil {
				vea.Report(errors.Wrapf(err, "aliased type %d does not exist", aliasedID))
			} else if typ.Dropped() {
				vea.Report(errors.AssertionFailedf("aliased type %q (%d) is dropped", typ.GetName(), typ.GetID()))
			}
		}
	}

	// Validate that all of the referencing descriptors exist.
	for _, id := range desc.GetReferencingDescriptorIDs() {
		tableDesc, err := vdg.GetTableDescriptor(id)
		if err != nil {
			vea.Report(err)
			continue
		}
		if tableDesc.Dropped() {
			vea.Report(errors.AssertionFailedf(
				"referencing table %d was dropped without dependency unlinking", id))
		}
	}
}

func (desc *immutable) validateMultiRegion(
	dbDesc catalog.DatabaseDescriptor, vea catalog.ValidationErrorAccumulator,
) {
	// Parent database must be a multi-region database if it includes a
	// multi-region enum.
	if !dbDesc.IsMultiRegion() {
		vea.Report(errors.AssertionFailedf("parent database is not a multi-region database"))
		return
	}

	primaryRegion, err := desc.PrimaryRegionName()
	if err != nil {
		vea.Report(err)
	}

	{
		found := false
		for _, member := range desc.EnumMembers {
			if catpb.RegionName(member.LogicalRepresentation) == primaryRegion {
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

	if dbDesc.GetRegionConfig().SurvivalGoal == descpb.SurvivalGoal_REGION_FAILURE {
		regionNames, err := desc.RegionNames()
		if err != nil {
			vea.Report(err)
		}
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
}

// ValidateTxnCommit implements the catalog.Descriptor interface.
func (desc *immutable) ValidateTxnCommit(
	_ catalog.ValidationErrorAccumulator, _ catalog.ValidationDescGetter,
) {
	// No-op.
}

// TypeLookupFunc is a type alias for a function that looks up a type by ID.
type TypeLookupFunc func(ctx context.Context, id descpb.ID) (tree.TypeName, catalog.TypeDescriptor, error)

// GetTypeDescriptor implements the TypeDescriptorResolver interface.
func (t TypeLookupFunc) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	return t(ctx, id)
}

// MakeTypesT implements the TypeDescriptor interface.
func (desc *immutable) MakeTypesT(
	ctx context.Context, name *tree.TypeName, res catalog.TypeDescriptorResolver,
) (*types.T, error) {
	switch t := desc.Kind; t {
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		typ := types.MakeEnum(TypeIDToOID(desc.GetID()), TypeIDToOID(desc.ArrayTypeID))
		if err := desc.HydrateTypeInfoWithName(ctx, typ, name, res); err != nil {
			return nil, err
		}
		return typ, nil
	case descpb.TypeDescriptor_ALIAS:
		// Hydrate the alias and return it.
		if err := desc.HydrateTypeInfoWithName(ctx, desc.Alias, name, res); err != nil {
			return nil, err
		}
		return desc.Alias, nil
	default:
		return nil, errors.AssertionFailedf("unknown type kind %s", t.String())
	}
}

// EnsureTypeIsHydrated makes sure that t is a fully-hydrated type.
func EnsureTypeIsHydrated(
	ctx context.Context, t *types.T, res catalog.TypeDescriptorResolver,
) error {
	if t.Family() == types.TupleFamily {
		for _, typ := range t.TupleContents() {
			if err := EnsureTypeIsHydrated(ctx, typ, res); err != nil {
				return err
			}
		}
		return nil
	}
	if !t.UserDefined() || t.IsHydrated() {
		return nil
	}
	id, err := GetUserDefinedTypeDescID(t)
	if err != nil {
		return err
	}
	elemTypName, elemTypDesc, err := res.GetTypeDescriptor(ctx, id)
	if err != nil {
		return err
	}
	return elemTypDesc.HydrateTypeInfoWithName(ctx, t, &elemTypName, res)
}

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

// HydrateTypeInfoWithName implements the TypeDescriptor interface.
func (desc *immutable) HydrateTypeInfoWithName(
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
	typ.TypeMeta.Version = uint32(desc.Version)
	switch desc.Kind {
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		if typ.Family() != types.EnumFamily {
			return errors.New("cannot hydrate a non-enum type with an enum type descriptor")
		}
		typ.TypeMeta.EnumData = &types.EnumMetadata{
			LogicalRepresentations:  desc.logicalReps,
			PhysicalRepresentations: desc.physicalReps,
			IsMemberReadOnly:        desc.readOnlyMembers,
		}
		return nil
	case descpb.TypeDescriptor_ALIAS:
		if typ.UserDefined() {
			switch typ.Family() {
			case types.ArrayFamily:
				// Hydrate the element type.
				elemType := typ.ArrayContents()
				return EnsureTypeIsHydrated(ctx, elemType, res)
			case types.TupleFamily:
				return EnsureTypeIsHydrated(ctx, typ, res)
			default:
				return errors.AssertionFailedf("unhandled alias type family %s", typ.Family())
			}
		}
		return nil
	default:
		return errors.AssertionFailedf("unknown type descriptor kind %s", desc.Kind)
	}
}

// NumEnumMembers implements the TypeDescriptor interface.
func (desc *immutable) NumEnumMembers() int {
	return len(desc.EnumMembers)
}

// GetMemberPhysicalRepresentation implements the TypeDescriptor interface.
func (desc *immutable) GetMemberPhysicalRepresentation(enumMemberOrdinal int) []byte {
	return desc.physicalReps[enumMemberOrdinal]
}

// GetMemberLogicalRepresentation implements the TypeDescriptor interface.
func (desc *immutable) GetMemberLogicalRepresentation(enumMemberOrdinal int) string {
	return desc.logicalReps[enumMemberOrdinal]
}

// IsMemberReadOnly implements the TypeDescriptor interface.
func (desc *immutable) IsMemberReadOnly(enumMemberOrdinal int) bool {
	return desc.readOnlyMembers[enumMemberOrdinal]
}

// NumReferencingDescriptors implements the TypeDescriptor interface.
func (desc *immutable) NumReferencingDescriptors() int {
	return len(desc.ReferencingDescriptorIDs)
}

// GetReferencingDescriptorID implements the TypeDescriptor interface.
func (desc *immutable) GetReferencingDescriptorID(refOrdinal int) descpb.ID {
	return desc.ReferencingDescriptorIDs[refOrdinal]
}

// IsCompatibleWith implements the TypeDescriptor interface.
func (desc *immutable) IsCompatibleWith(other catalog.TypeDescriptor) error {

	switch desc.Kind {
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		if other.GetKind() != desc.Kind {
			return errors.Newf("%q of type %q is not compatible with type %q",
				other.GetName(), other.GetKind(), desc.Kind)
		}
		// Every enum value in desc must be present in other, and all of the
		// physical representations must be the same.
		for _, thisMember := range desc.EnumMembers {
			found := false
			for i := 0; i < other.NumEnumMembers(); i++ {
				if thisMember.LogicalRepresentation == other.GetMemberLogicalRepresentation(i) {
					// We've found a match. Now the physical representations must be
					// the same, otherwise the enums are incompatible.
					if !bytes.Equal(thisMember.PhysicalRepresentation, other.GetMemberPhysicalRepresentation(i)) {
						return errors.Newf(
							"%q has differing physical representation for value %q",
							other.GetName(),
							thisMember.LogicalRepresentation,
						)
					}
					found = true
				}
			}
			if !found {
				return errors.Newf(
					"could not find enum value %q in %q", thisMember.LogicalRepresentation, other.GetName())
			}
		}
		return nil
	default:
		return errors.Newf("compatibility comparison unsupported for type kind %s", desc.Kind.String())
	}
}

// HasPendingSchemaChanges implements the TypeDescriptor interface.
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

// GetIDClosure implements the TypeDescriptor interface.
func (desc *immutable) GetIDClosure() (map[descpb.ID]struct{}, error) {
	ret := make(map[descpb.ID]struct{})
	// Collect the descriptor's own ID.
	ret[desc.ID] = struct{}{}
	if desc.Kind == descpb.TypeDescriptor_ALIAS {
		// If this descriptor is an alias for another type, then get collect the
		// closure for alias.
		children, err := GetTypeDescriptorClosure(desc.Alias)
		if err != nil {
			return nil, err
		}
		for id := range children {
			ret[id] = struct{}{}
		}
	} else {
		// Otherwise, take the array type ID.
		ret[desc.ArrayTypeID] = struct{}{}
	}
	return ret, nil
}

// GetTypeDescriptorClosure returns all type descriptor IDs that are
// referenced by this input types.T.
func GetTypeDescriptorClosure(typ *types.T) (map[descpb.ID]struct{}, error) {
	if !typ.UserDefined() {
		return map[descpb.ID]struct{}{}, nil
	}
	id, err := GetUserDefinedTypeDescID(typ)
	if err != nil {
		return nil, err
	}
	// Collect the type's descriptor ID.
	ret := map[descpb.ID]struct{}{
		id: {},
	}
	switch typ.Family() {
	case types.ArrayFamily:
		// If we have an array type, then collect all types in the contents.
		children, err := GetTypeDescriptorClosure(typ.ArrayContents())
		if err != nil {
			return nil, err
		}
		for id := range children {
			ret[id] = struct{}{}
		}
	case types.TupleFamily:
		// If we have a tuple type, collect all types in the contents.
		for _, elt := range typ.TupleContents() {
			children, err := GetTypeDescriptorClosure(elt)
			if err != nil {
				return nil, err
			}
			for id := range children {
				ret[id] = struct{}{}
			}
		}
	default:
		// Otherwise, take the array type ID.
		id, err := GetUserDefinedArrayTypeDescID(typ)
		if err != nil {
			return nil, err
		}
		ret[id] = struct{}{}
	}
	return ret, nil
}

// SetDeclarativeSchemaChangerState is part of the catalog.MutableDescriptor
// interface.
func (desc *Mutable) SetDeclarativeSchemaChangerState(state *scpb.DescriptorState) {
	desc.DeclarativeSchemaChangerState = state
}
