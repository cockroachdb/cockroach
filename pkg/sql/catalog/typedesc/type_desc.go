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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

var _ catalog.TypeDescriptor = (*Immutable)(nil)
var _ catalog.TypeDescriptor = (*Mutable)(nil)
var _ catalog.MutableDescriptor = (*Mutable)(nil)

// MakeSimpleAlias creates a type descriptor that is an alias for the input
// type. It is intended to be used as an intermediate for name resolution, and
// should not be serialized and stored on disk.
func MakeSimpleAlias(typ *types.T, parentSchemaID descpb.ID) *Immutable {
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

// NameResolutionResult implements the NameResolutionResult interface.
func (desc *Immutable) NameResolutionResult() {}

// Mutable is a custom type for TypeDescriptors undergoing
// any types of modifications.
type Mutable struct {

	// TODO(ajwerner): Decide whether we're okay embedding the
	// Immutable or whether we should be embedding some other base
	// struct that implements the various methods. For now we have the trap that
	// the code really wants direct field access and moving all access to
	// getters on an interface is a bigger task.
	Immutable

	// ClusterVersion represents the version of the type descriptor read
	// from the store.
	ClusterVersion *Immutable
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Mutable) IsUncommittedVersion() bool {
	return desc.IsNew() || desc.ClusterVersion.GetVersion() != desc.GetVersion()
}

// Immutable is a custom type for wrapping TypeDescriptors
// when used in a read only way.
type Immutable struct {
	descpb.TypeDescriptor

	// The fields below are used to fill user defined type metadata for ENUMs.
	logicalReps     []string
	physicalReps    [][]byte
	readOnlyMembers []bool

	// isUncommittedVersion is set to true if this descriptor was created from
	// a copy of a Mutable with an uncommitted version.
	isUncommittedVersion bool
}

// UpdateCachedFieldsOnModifiedMutable refreshes the Immutable field by
// reconstructing it. This means that the fields used to fill enumMetadata
// (readOnly, logicalReps, physicalReps) are reconstructed to reflect the
// modified Mutable's state. This allows us to hydrate tables correctly even
// when preceded by a type descriptor modification in the same transaction.
func UpdateCachedFieldsOnModifiedMutable(desc catalog.TypeDescriptor) (*Mutable, error) {
	mutable, ok := desc.(*Mutable)
	if !ok {
		return nil, errors.AssertionFailedf("type descriptor was not mutable")
	}
	mutable.Immutable = *mutable.ImmutableCopy().(*Immutable)
	return mutable, nil
}

// TypeIDToOID converts a type descriptor ID into a type OID.
func TypeIDToOID(id descpb.ID) oid.Oid {
	return oid.Oid(id) + oidext.CockroachPredefinedOIDMax
}

// UserDefinedTypeOIDToID converts a user defined type OID into a
// descriptor ID.
func UserDefinedTypeOIDToID(oid oid.Oid) descpb.ID {
	return descpb.ID(oid) - oidext.CockroachPredefinedOIDMax
}

// GetTypeDescID gets the type descriptor ID from a user defined type.
func GetTypeDescID(t *types.T) descpb.ID {
	return UserDefinedTypeOIDToID(t.Oid())
}

// GetArrayTypeDescID gets the ID of the array type descriptor from a user
// defined type.
func GetArrayTypeDescID(t *types.T) descpb.ID {
	return UserDefinedTypeOIDToID(t.UserDefinedArrayOID())
}

// TypeDesc implements the Descriptor interface.
func (desc *Immutable) TypeDesc() *descpb.TypeDescriptor {
	return &desc.TypeDescriptor
}

// Public implements the Descriptor interface.
func (desc *Immutable) Public() bool {
	return desc.State == descpb.DescriptorState_PUBLIC
}

// Adding implements the Descriptor interface.
func (desc *Immutable) Adding() bool {
	return false
}

// Offline implements the Descriptor interface.
func (desc *Immutable) Offline() bool {
	return desc.State == descpb.DescriptorState_OFFLINE
}

// Dropped implements the Descriptor interface.
func (desc *Immutable) Dropped() bool {
	return desc.State == descpb.DescriptorState_DROP
}

// IsUncommittedVersion implements the Descriptor interface.
func (desc *Immutable) IsUncommittedVersion() bool {
	return desc.isUncommittedVersion
}

// DescriptorProto returns a Descriptor for serialization.
func (desc *Immutable) DescriptorProto() *descpb.Descriptor {
	return &descpb.Descriptor{
		Union: &descpb.Descriptor_Type{
			Type: &desc.TypeDescriptor,
		},
	}
}

// PrimaryRegionName returns the primary region for a multi-region enum.
func (desc *Immutable) PrimaryRegionName() (descpb.RegionName, error) {
	if desc.Kind != descpb.TypeDescriptor_MULTIREGION_ENUM {
		return "", errors.AssertionFailedf(
			"cannot get primary region of a non multi-region enum %q (%d)", desc.GetName(), desc.GetID())
	}
	return desc.RegionConfig.PrimaryRegion, nil
}

// RegionNames returns all the regions on the multi-region enum.
// This includes regions that are in `READ_ONLY` state as well, if they've just
// been added or are in the process of being removed (pre-validation).
func (desc *Immutable) RegionNames() (descpb.RegionNames, error) {
	if desc.Kind != descpb.TypeDescriptor_MULTIREGION_ENUM {
		return nil, errors.AssertionFailedf(
			"can not get regions of a non multi-region enum %q (%d)", desc.GetName(), desc.GetID())
	}
	var regions descpb.RegionNames
	for _, member := range desc.EnumMembers {
		regions = append(regions, descpb.RegionName(member.LogicalRepresentation))
	}
	return regions, nil
}

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *Mutable) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}

// GetAuditMode implements the DescriptorProto interface.
func (desc *Immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// DescriptorType implements the catalog.Descriptor interface.
func (desc *Immutable) DescriptorType() catalog.DescriptorType {
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
	imm := NewBuilder(desc.TypeDesc()).BuildImmutableType()
	imm.isUncommittedVersion = desc.IsUncommittedVersion()
	return imm
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
func (desc *Mutable) AddDrainingName(name descpb.NameInfo) {
	desc.DrainingNames = append(desc.DrainingNames, name)
}

// EnumMembers is a sortable list of TypeDescriptor_EnumMember, sorted by the
// physical representation.
type EnumMembers []descpb.TypeDescriptor_EnumMember

func (e EnumMembers) Len() int { return len(e) }
func (e EnumMembers) Less(i, j int) bool {
	return bytes.Compare(e[i].PhysicalRepresentation, e[j].PhysicalRepresentation) < 0
}
func (e EnumMembers) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

func isBeingDropped(member *descpb.TypeDescriptor_EnumMember) bool {
	return member.Capability == descpb.TypeDescriptor_EnumMember_READ_ONLY &&
		member.Direction == descpb.TypeDescriptor_EnumMember_REMOVE
}

// ValidateSelf performs validation on the TypeDescriptor.
func (desc *Immutable) ValidateSelf(vea catalog.ValidationErrorAccumulator) {
	// Validate local properties of the descriptor.
	vea.Report(catalog.BadName, catalog.ValidateName(desc.Name, "type"))
	if desc.GetID() == descpb.InvalidID {
		vea.ReportInternalf(catalog.BadID, "invalid ID")
	}
	if desc.GetParentID() == descpb.InvalidID {
		vea.ReportInternalf(catalog.BadParentID, "invalid parent database ID")
	}
	if desc.GetParentSchemaID() == descpb.InvalidID {
		vea.ReportInternalf(catalog.BadParentSchemaID, "invalid parent schema ID")
	}

	if desc.Kind != descpb.TypeDescriptor_ALIAS {
		vea.Report(catalog.BadPrivileges, desc.Privileges.Validate(desc.ID, privilege.Type))
	}

	switch desc.Kind {
	case descpb.TypeDescriptor_MULTIREGION_ENUM:
		func() {
			vea.PushContextf(`kind_multi_region_enum`, "multi-region enum")
			defer vea.PopContext()
			// Check presence of region config
			if desc.RegionConfig == nil {
				vea.ReportInternalf(catalog.NotSet, "region config not set")
			}
			if desc.validateEnumMembers(vea) {
				// In the case of the multi-region enum, we also keep the logical descriptors
				// sorted. Validate that's the case.
				for i := 0; i < len(desc.EnumMembers)-1; i++ {
					if desc.EnumMembers[i].LogicalRepresentation > desc.EnumMembers[i+1].LogicalRepresentation {
						vea.ReportInternalf(catalog.BadOrder, "enum members out of order %q > %q",
							desc.EnumMembers[i].LogicalRepresentation, desc.EnumMembers[i+1].LogicalRepresentation)
					}
				}
			}
		}()
	case descpb.TypeDescriptor_ENUM:
		func() {
			vea.PushContextf(`kind_enum`, "enum")
			defer vea.PopContext()
			if desc.RegionConfig != nil {
				vea.ReportInternalf(catalog.NotUnset, "unexpected region config %v", desc.RegionConfig)
			}
			desc.validateEnumMembers(vea)
		}()
	case descpb.TypeDescriptor_ALIAS:
		func() {
			vea.PushContextf(`kind_alias`, "alias")
			defer vea.PopContext()
			if desc.RegionConfig != nil {
				vea.ReportInternalf(catalog.NotUnset, "unexpected region config %v", desc.RegionConfig)
			}
			if desc.Alias == nil {
				vea.ReportInternalf(catalog.NotSet, "alias not set")
			}
			if desc.GetArrayTypeID() != descpb.InvalidID {
				vea.ReportInternalf(catalog.NotUnset, "unexpected array type (%d)", desc.GetArrayTypeID())
			}
		}()
	default:
		vea.ReportInternalf(catalog.Unclassified, "invalid type descriptor kind %q", desc.Kind)
	}
}

// validateEnumMembers performs enum member checks.
// Returns true iff the enums are sorted.
func (desc *Immutable) validateEnumMembers(vea catalog.ValidationErrorAccumulator) (isSorted bool) {
	// All of the enum members should be in sorted order.
	isSorted = sort.IsSorted(EnumMembers(desc.EnumMembers))
	if !isSorted {
		vea.ReportInternalf(`not_sorted`, "enum members are not sorted: %v", desc.EnumMembers)
	}
	// Ensure there are no duplicate enum physical and logical reps.
	physicalMap := make(map[string]struct{}, len(desc.EnumMembers))
	logicalMap := make(map[string]struct{}, len(desc.EnumMembers))
	for _, member := range desc.EnumMembers {
		// Ensure there are no duplicate enum physical reps.
		_, duplicatePhysical := physicalMap[string(member.PhysicalRepresentation)]
		if duplicatePhysical {
			vea.ReportInternalf(catalog.NotUnique, "duplicate enum physical rep %v", member.PhysicalRepresentation)
		}
		physicalMap[string(member.PhysicalRepresentation)] = struct{}{}
		// Ensure there are no duplicate enum logical reps.
		_, duplicateLogical := logicalMap[member.LogicalRepresentation]
		if duplicateLogical {
			vea.ReportInternalf(catalog.NotUnique, "duplicate enum member %q", member.LogicalRepresentation)
		}
		logicalMap[member.LogicalRepresentation] = struct{}{}
		// Ensure the sanity of enum capabilities and transition directions.
		switch member.Capability {
		case descpb.TypeDescriptor_EnumMember_READ_ONLY:
			if member.Direction == descpb.TypeDescriptor_EnumMember_NONE {
				vea.ReportInternalf(`missing_direction`, "read only capability member must have transition direction set")
			}
		case descpb.TypeDescriptor_EnumMember_ALL:
			if member.Direction != descpb.TypeDescriptor_EnumMember_NONE {
				vea.ReportInternalf(`unexpected_direction`, "public enum member can not have transition direction set")
			}
		default:
			vea.ReportInternalf(catalog.Unclassified, "invalid member capability %s", member.Capability)
		}
	}
	return isSorted
}

// GetReferencedDescIDs returns the IDs of all descriptors referenced by
// this descriptor, including itself.
func (desc *Immutable) GetReferencedDescIDs() catalog.DescriptorIDSet {
	ids := catalog.MakeDescriptorIDSet(desc.GetReferencingDescriptorIDs()...)
	ids.Add(desc.GetParentID())
	if desc.GetParentSchemaID() != keys.PublicSchemaID {
		ids.Add(desc.GetParentSchemaID())
	}
	for id := range desc.GetIDClosure() {
		ids.Add(id)
	}
	return ids
}

// ValidateCrossReferences performs cross reference checks on the type descriptor.
func (desc *Immutable) ValidateCrossReferences(
	vea catalog.ValidationErrorAccumulator, vdg catalog.ValidationDescGetter,
) {
	if desc.Dropped() {
		return
	}

	// Check that the parent database exists.
	dbDesc, err := vdg.GetDatabaseDescriptor(desc.GetParentID())
	if err != nil {
		vea.Report(catalog.NotFound, err)
	}

	// Check that the parent schema exists and shares the same parent database.
	if desc.GetParentSchemaID() != keys.PublicSchemaID {
		schemaDesc, err := vdg.GetSchemaDescriptor(desc.GetParentSchemaID())
		if err != nil {
			vea.Report(catalog.NotFound, err)
		} else if dbDesc != nil && schemaDesc.GetParentID() != dbDesc.GetID() {
			vea.ReportInternalf(catalog.BadParentID,
				"parent database (%d) of parent schema %q (%d) different than table parent database %q (%d)",
				schemaDesc.GetParentID(), schemaDesc.GetName(), schemaDesc.GetID(), dbDesc.GetName(), dbDesc.GetID())
		}
	}

	switch desc.GetKind() {
	case descpb.TypeDescriptor_MULTIREGION_ENUM:
		func() {
			vea.PushContextf(`kind_multi_region_enum`, "multi-region enum")
			defer vea.PopContext()
			if dbDesc != nil {
				desc.validateMultiRegion(dbDesc, vea)
			}
			// Ensure that the referenced array type exists.
			_, err = vdg.GetTypeDescriptor(desc.GetArrayTypeID())
			vea.Report(catalog.NotFound, err)
		}()
	case descpb.TypeDescriptor_ENUM:
		func() {
			vea.PushContextf(`kind_enum`, "enum")
			defer vea.PopContext()
			// Ensure that the referenced array type exists.
			_, err = vdg.GetTypeDescriptor(desc.GetArrayTypeID())
			vea.Report(catalog.NotFound, err)
		}()
	case descpb.TypeDescriptor_ALIAS:
		func() {
			vea.PushContextf(`kind_alias`, "alias")
			defer vea.PopContext()
			// Ensure that the referenced aliased type exists.
			if desc.GetAlias().UserDefined() {
				aliasedID := UserDefinedTypeOIDToID(desc.GetAlias().Oid())
				_, err := vdg.GetTypeDescriptor(aliasedID)
				vea.Report(catalog.NotFound, err)
			}
		}()
	}

	// Validate that all of the referencing descriptors exist.
	for _, id := range desc.GetReferencingDescriptorIDs() {
		tableDesc, err := vdg.GetTableDescriptor(id)
		if err != nil {
			vea.Report(catalog.NotFound, err)
		} else if tableDesc.Dropped() {
			vea.ReportInternalf(catalog.BadState,
				"back-referenced table %q (%d) dropped without dependency unlinking",
				tableDesc.GetName(), tableDesc.GetID())
		}
	}
}

func (desc *Immutable) validateMultiRegion(
	db catalog.DatabaseDescriptor, vea catalog.ValidationErrorAccumulator,
) {
	vea.PushContextf(`parent_database`, "parent database %q (%d)", db.GetName(), db.GetID())
	defer vea.PopContext()

	// Parent database must be a multi-region database if it includes a
	// multi-region enum.
	if !db.IsMultiRegion() {
		vea.ReportInternalf(`not_multi_region`, "not a multi-region database")
		return
	}

	dbRegions := map[string]struct{}{}
	for _, r := range db.GetRegionConfig().Regions {
		dbRegions[string(r.Name)] = struct{}{}
	}

	numEnumRegions := 0
	for _, member := range desc.EnumMembers {
		if isBeingDropped(&member) {
			continue
		}
		numEnumRegions++
		if _, ok := dbRegions[member.LogicalRepresentation]; !ok {
			vea.ReportInternalf(catalog.NotFound, "region %q from enum not found", member.LogicalRepresentation)
		}
	}
	if numEnumRegions != len(dbRegions) {
		vea.ReportInternalf(catalog.Mismatch, "has %d regions instead of %d in enum", len(dbRegions), numEnumRegions)
	}

	if desc.GetRegionConfig().PrimaryRegion != db.GetRegionConfig().PrimaryRegion {
		vea.ReportInternalf(catalog.Mismatch,
			"primary region is %q instead of %q in enum",
			db.GetRegionConfig().PrimaryRegion, desc.GetRegionConfig().PrimaryRegion)
	}
}

// ValidateTxnCommit implements the catalog.Descriptor interface.
func (desc *Immutable) ValidateTxnCommit(
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

// MakeTypesT creates a types.T from the input type descriptor.
func (desc *Immutable) MakeTypesT(
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

// HydrateTypesInTableDescriptor uses typeLookup to install metadata in the
// types present in a table descriptor. typeLookup retrieves the fully
// qualified name and descriptor for a particular ID.
func HydrateTypesInTableDescriptor(
	ctx context.Context, desc *descpb.TableDescriptor, res catalog.TypeDescriptorResolver,
) error {
	hydrateCol := func(col *descpb.ColumnDescriptor) error {
		if col.Type.UserDefined() {
			// Look up its type descriptor.
			name, typDesc, err := res.GetTypeDescriptor(ctx, GetTypeDescID(col.Type))
			if err != nil {
				return err
			}
			// Note that this will no-op if the type is already hydrated.
			if err := typDesc.HydrateTypeInfoWithName(ctx, col.Type, &name, res); err != nil {
				return err
			}
		}
		return nil
	}
	for i := range desc.Columns {
		if err := hydrateCol(&desc.Columns[i]); err != nil {
			return err
		}
	}
	for i := range desc.Mutations {
		mut := &desc.Mutations[i]
		if col := mut.GetColumn(); col != nil {
			if err := hydrateCol(col); err != nil {
				return err
			}
		}
	}
	return nil
}

// HydrateTypeInfoWithName fills in user defined type metadata for
// a type and also sets the name in the metadata to the passed in name.
// This is used when hydrating a type with a known qualified name.
//
// Note that if the passed type is already hydrated, regardless of the version
// with which it has been hydrated, this is a no-op.
func (desc *Immutable) HydrateTypeInfoWithName(
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
				elemTypName, elemTypDesc, err := res.GetTypeDescriptor(ctx, GetTypeDescID(elemType))
				if err != nil {
					return err
				}
				if err := elemTypDesc.HydrateTypeInfoWithName(ctx, elemType, &elemTypName, res); err != nil {
					return err
				}
				return nil
			default:
				return errors.AssertionFailedf("only array types aliases can be user defined")
			}
		}
		return nil
	default:
		return errors.AssertionFailedf("unknown type descriptor kind %s", desc.Kind)
	}
}

// IsCompatibleWith returns whether the type "desc" is compatible with "other".
// As of now "compatibility" entails that disk encoded data of "desc" can be
// interpreted and used by "other".
func (desc *Immutable) IsCompatibleWith(other *Immutable) error {
	switch desc.Kind {
	case descpb.TypeDescriptor_ENUM, descpb.TypeDescriptor_MULTIREGION_ENUM:
		if other.Kind != desc.Kind {
			return errors.Newf("%q of type %q is not compatible with type %q",
				other.Name, other.Kind, desc.Kind)
		}
		// Every enum value in desc must be present in other, and all of the
		// physical representations must be the same.
		for _, thisMember := range desc.EnumMembers {
			found := false
			for _, otherMember := range other.EnumMembers {
				if thisMember.LogicalRepresentation == otherMember.LogicalRepresentation {
					// We've found a match. Now the physical representations must be
					// the same, otherwise the enums are incompatible.
					if !bytes.Equal(thisMember.PhysicalRepresentation, otherMember.PhysicalRepresentation) {
						return errors.Newf(
							"%q has differing physical representation for value %q",
							other.Name,
							thisMember.LogicalRepresentation,
						)
					}
					found = true
				}
			}
			if !found {
				return errors.Newf(
					"could not find enum value %q in %q", thisMember.LogicalRepresentation, other.Name)
			}
		}
		return nil
	default:
		return errors.Newf("compatibility comparison unsupported for type kind %s", desc.Kind.String())
	}
}

// HasPendingSchemaChanges returns whether or not this descriptor has schema
// changes that need to be completed.
func (desc *Immutable) HasPendingSchemaChanges() bool {
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

// GetIDClosure returns all type descriptor IDs that are referenced by this
// type descriptor.
func (desc *Immutable) GetIDClosure() map[descpb.ID]struct{} {
	ret := make(map[descpb.ID]struct{})
	// Collect the descriptor's own ID.
	ret[desc.ID] = struct{}{}
	if desc.Kind == descpb.TypeDescriptor_ALIAS {
		// If this descriptor is an alias for another type, then get collect the
		// closure for alias.
		children := GetTypeDescriptorClosure(desc.Alias)
		for id := range children {
			ret[id] = struct{}{}
		}
	} else {
		// Otherwise, take the array type ID.
		ret[desc.ArrayTypeID] = struct{}{}
	}
	return ret
}

// GetTypeDescriptorClosure returns all type descriptor IDs that are
// referenced by this input types.T.
func GetTypeDescriptorClosure(typ *types.T) map[descpb.ID]struct{} {
	if !typ.UserDefined() {
		return map[descpb.ID]struct{}{}
	}
	// Collect the type's descriptor ID.
	ret := map[descpb.ID]struct{}{
		GetTypeDescID(typ): {},
	}
	if typ.Family() == types.ArrayFamily {
		// If we have an array type, then collect all types in the contents.
		children := GetTypeDescriptorClosure(typ.ArrayContents())
		for id := range children {
			ret[id] = struct{}{}
		}
	} else {
		// Otherwise, take the array type ID.
		ret[GetArrayTypeDescID(typ)] = struct{}{}
	}
	return ret
}
