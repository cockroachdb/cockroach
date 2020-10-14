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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
	return NewImmutable(descpb.TypeDescriptor{
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
	})
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

// NewCreatedMutable returns a Mutable from the given type descriptor with the
// cluster version being the zero type. This is for a type that is created in
// the same transaction.
func NewCreatedMutable(desc descpb.TypeDescriptor) *Mutable {
	return &Mutable{
		Immutable: makeImmutable(desc),
	}
}

// NewExistingMutable returns a Mutable from the given type descriptor with the
// cluster version also set to the descriptor. This is for types that already
// exist.
func NewExistingMutable(desc descpb.TypeDescriptor) *Mutable {
	return &Mutable{
		Immutable:      makeImmutable(*protoutil.Clone(&desc).(*descpb.TypeDescriptor)),
		ClusterVersion: NewImmutable(desc),
	}
}

// NewImmutable returns an Immutable from the given TypeDescriptor.
func NewImmutable(desc descpb.TypeDescriptor) *Immutable {
	m := makeImmutable(desc)
	return &m
}

func makeImmutable(desc descpb.TypeDescriptor) Immutable {
	immutDesc := Immutable{TypeDescriptor: desc}

	// Initialize metadata specific to the TypeDescriptor kind.
	switch immutDesc.Kind {
	case descpb.TypeDescriptor_ENUM:
		immutDesc.logicalReps = make([]string, len(desc.EnumMembers))
		immutDesc.physicalReps = make([][]byte, len(desc.EnumMembers))
		immutDesc.readOnlyMembers = make([]bool, len(desc.EnumMembers))
		for i := range desc.EnumMembers {
			member := &desc.EnumMembers[i]
			immutDesc.logicalReps[i] = member.LogicalRepresentation
			immutDesc.physicalReps[i] = member.PhysicalRepresentation
			immutDesc.readOnlyMembers[i] =
				member.Capability == descpb.TypeDescriptor_EnumMember_READ_ONLY
		}
	}

	return immutDesc
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

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *Mutable) SetDrainingNames(names []descpb.NameInfo) {
	desc.DrainingNames = names
}

// GetAuditMode implements the DescriptorProto interface.
func (desc *Immutable) GetAuditMode() descpb.TableDescriptor_AuditMode {
	return descpb.TableDescriptor_DISABLED
}

// TypeName implements the DescriptorProto interface.
func (desc *Immutable) TypeName() string {
	return "type"
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
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	imm := NewImmutable(*protoutil.Clone(desc.TypeDesc()).(*descpb.TypeDescriptor))
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
			return pgerror.Newf(pgcode.InvalidParameterValue, "%q is not an existing enum label", existing)
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

// Validate performs validation on the TypeDescriptor.
func (desc *Immutable) Validate(ctx context.Context, dg catalog.DescGetter) error {
	// Validate local properties of the descriptor.
	if err := catalog.ValidateName(desc.Name, "type"); err != nil {
		return err
	}

	if desc.ID == descpb.InvalidID {
		return errors.AssertionFailedf("invalid ID %d", errors.Safe(desc.ID))
	}
	if desc.ParentID == descpb.InvalidID {
		return errors.AssertionFailedf("invalid parentID %d", errors.Safe(desc.ParentID))
	}

	switch desc.Kind {
	case descpb.TypeDescriptor_ENUM:
		// All of the enum members should be in sorted order.
		if !sort.IsSorted(EnumMembers(desc.EnumMembers)) {
			return errors.AssertionFailedf("enum members are not sorted %v", desc.EnumMembers)
		}
		// Ensure there are no duplicate enum physical reps.
		for i := 0; i < len(desc.EnumMembers)-1; i++ {
			if bytes.Equal(desc.EnumMembers[i].PhysicalRepresentation, desc.EnumMembers[i+1].PhysicalRepresentation) {
				return errors.AssertionFailedf("duplicate enum physical rep %v", desc.EnumMembers[i].PhysicalRepresentation)
			}
		}
		// Ensure there are no duplicate enum labels.
		members := make(map[string]struct{}, len(desc.EnumMembers))
		for i := range desc.EnumMembers {
			_, ok := members[desc.EnumMembers[i].LogicalRepresentation]
			if ok {
				return errors.AssertionFailedf("duplicate enum member %q", desc.EnumMembers[i].LogicalRepresentation)
			}
			members[desc.EnumMembers[i].LogicalRepresentation] = struct{}{}
		}

		// Validate the Privileges of the descriptor.
		if err := desc.Privileges.Validate(desc.ID, privilege.Type); err != nil {
			return err
		}
	case descpb.TypeDescriptor_ALIAS:
		if desc.Alias == nil {
			return errors.AssertionFailedf("ALIAS type desc has nil alias type")
		}
	default:
		return errors.AssertionFailedf("invalid desc kind %s", desc.Kind.String())
	}

	// Validate all cross references on the descriptor.

	// Buffer all the requested requests and error checks together to run at once.
	var checks []func(got catalog.Descriptor) error
	var reqs []descpb.ID

	// Validate the parentID.
	reqs = append(reqs, desc.ParentID)
	checks = append(checks, func(got catalog.Descriptor) error {
		if _, isDB := got.(catalog.DatabaseDescriptor); !isDB {
			return errors.AssertionFailedf("parentID %d does not exist", errors.Safe(desc.ParentID))
		}
		return nil
	})

	// Validate the parentSchemaID.
	if desc.ParentSchemaID != keys.PublicSchemaID {
		reqs = append(reqs, desc.ParentSchemaID)
		checks = append(checks, func(got catalog.Descriptor) error {
			if _, isSchema := got.(catalog.SchemaDescriptor); !isSchema {
				return errors.AssertionFailedf("parentSchemaID %d does not exist", errors.Safe(desc.ParentSchemaID))
			}
			return nil
		})
	}

	switch desc.Kind {
	case descpb.TypeDescriptor_ENUM:
		// Ensure that the referenced array type exists.
		reqs = append(reqs, desc.ArrayTypeID)
		checks = append(checks, func(got catalog.Descriptor) error {
			if _, isType := got.(catalog.TypeDescriptor); !isType {
				return errors.AssertionFailedf("arrayTypeID %d does not exist", errors.Safe(desc.ArrayTypeID))
			}
			return nil
		})
	case descpb.TypeDescriptor_ALIAS:
		if desc.ArrayTypeID != descpb.InvalidID {
			return errors.AssertionFailedf("ALIAS type desc has array type ID %d", desc.ArrayTypeID)
		}
	}

	// Validate that all of the referencing descriptors exist.
	tableExists := func(id descpb.ID) func(got catalog.Descriptor) error {
		return func(got catalog.Descriptor) error {
			if _, isTable := got.(catalog.TableDescriptor); !isTable {
				return errors.AssertionFailedf("referencing descriptor %d does not exist", id)
			}
			return nil
		}
	}
	if !desc.Dropped() {

		for _, id := range desc.ReferencingDescriptorIDs {
			reqs = append(reqs, id)
			checks = append(checks, tableExists(id))
		}
	}

	descs, err := dg.GetDescs(ctx, reqs)
	if err != nil {
		return err
	}

	// For each result in the batch, apply the corresponding check.
	for i := range checks {
		if err := checks[i](descs[i]); err != nil {
			return err
		}
	}

	return nil
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
	case descpb.TypeDescriptor_ENUM:
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
	case descpb.TypeDescriptor_ENUM:
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
	case descpb.TypeDescriptor_ENUM:
		if other.Kind != descpb.TypeDescriptor_ENUM {
			return errors.Newf("%q is not an enum", other.Name)
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
	case descpb.TypeDescriptor_ENUM:
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
