// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"bytes"
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// GetTypeDescFromID retrieves the type descriptor for the type ID passed
// in using an existing proto getter. It returns an error if the descriptor
// doesn't exist or if it exists and is not a type descriptor.
//
// TODO(ajwerner): Move this to catalogkv or something like it.
func GetTypeDescFromID(
	ctx context.Context, protoGetter protoGetter, codec keys.SQLCodec, id ID,
) (*ImmutableTypeDescriptor, error) {
	descKey := MakeDescMetadataKey(codec, id)
	desc := &Descriptor{}
	_, err := protoGetter.GetProtoTs(ctx, descKey, desc)
	if err != nil {
		return nil, err
	}
	typ := desc.GetType()
	if typ == nil {
		return nil, ErrDescriptorNotFound
	}
	// TODO(ajwerner): Fill in ModificationTime.
	return NewImmutableTypeDescriptor(*typ), nil
}

// TypeDescriptorInterface will eventually be called typedesc.Descriptor.
// It is implemented by (Imm|M)utableTypeDescriptor.
type TypeDescriptorInterface interface {
	BaseDescriptorInterface
	TypeDesc() *TypeDescriptor
	HydrateTypeInfoWithName(ctx context.Context, typ *types.T, name *tree.TypeName, res TypeDescriptorResolver) error
	MakeTypesT(ctx context.Context, name *tree.TypeName, res TypeDescriptorResolver) (*types.T, error)
}

var _ TypeDescriptorInterface = (*ImmutableTypeDescriptor)(nil)
var _ TypeDescriptorInterface = (*MutableTypeDescriptor)(nil)

// MakeSimpleAliasTypeDescriptor creates a type descriptor that is an alias
// for the input type. It is intended to be used as an intermediate for name
// resolution, and should not be serialized and stored on disk.
func MakeSimpleAliasTypeDescriptor(typ *types.T) *ImmutableTypeDescriptor {
	return NewImmutableTypeDescriptor(TypeDescriptor{
		ParentID:       InvalidID,
		ParentSchemaID: InvalidID,
		Name:           typ.Name(),
		ID:             InvalidID,
		Kind:           TypeDescriptor_ALIAS,
		Alias:          typ,
	})
}

// NameResolutionResult implements the NameResolutionResult interface.
func (desc *TypeDescriptor) NameResolutionResult() {}

// MutableTypeDescriptor is a custom type for TypeDescriptors undergoing
// any types of modifications.
type MutableTypeDescriptor struct {

	// TODO(ajwerner): Decide whether we're okay embedding the
	// ImmutableTypeDescriptor or whether we should be embedding some other base
	// struct that implements the various methods. For now we have the trap that
	// the code really wants direct field access and moving all access to
	// getters on an interface is a bigger task.
	ImmutableTypeDescriptor

	// ClusterVersion represents the version of the type descriptor read
	// from the store.
	ClusterVersion *ImmutableTypeDescriptor
}

// ImmutableTypeDescriptor is a custom type for wrapping TypeDescriptors
// when used in a read only way.
type ImmutableTypeDescriptor struct {
	TypeDescriptor
}

// NewMutableCreatedTypeDescriptor returns a MutableTypeDescriptor from the
// given type descriptor with the cluster version being the zero type. This
// is for a type that is created in the same transaction.
func NewMutableCreatedTypeDescriptor(desc TypeDescriptor) *MutableTypeDescriptor {
	return &MutableTypeDescriptor{
		ImmutableTypeDescriptor: makeImmutableTypeDescriptor(desc),
	}
}

// NewMutableExistingTypeDescriptor returns a MutableTypeDescriptor from the
// given type descriptor with the cluster version also set to the descriptor.
// This is for types that already exist.
func NewMutableExistingTypeDescriptor(desc TypeDescriptor) *MutableTypeDescriptor {
	return &MutableTypeDescriptor{
		ImmutableTypeDescriptor: makeImmutableTypeDescriptor(*protoutil.Clone(&desc).(*TypeDescriptor)),
		ClusterVersion:          NewImmutableTypeDescriptor(desc),
	}
}

// NewImmutableTypeDescriptor returns an ImmutableTypeDescriptor from the
// given TypeDescriptor.
func NewImmutableTypeDescriptor(desc TypeDescriptor) *ImmutableTypeDescriptor {
	m := makeImmutableTypeDescriptor(desc)
	return &m
}

func makeImmutableTypeDescriptor(desc TypeDescriptor) ImmutableTypeDescriptor {
	return ImmutableTypeDescriptor{TypeDescriptor: desc}
}

// DatabaseDesc implements the ObjectDescriptor interface.
func (desc *ImmutableTypeDescriptor) DatabaseDesc() *DatabaseDescriptor {
	return nil
}

// SchemaDesc implements the ObjectDescriptor interface.
func (desc *ImmutableTypeDescriptor) SchemaDesc() *SchemaDescriptor {
	return nil
}

// TableDesc implements the ObjectDescriptor interface.
func (desc *ImmutableTypeDescriptor) TableDesc() *TableDescriptor {
	return nil
}

// TypeDesc implements the ObjectDescriptor interface.
func (desc *ImmutableTypeDescriptor) TypeDesc() *TypeDescriptor {
	return &desc.TypeDescriptor
}

// Adding implements the BaseDescriptorInterface interface.
func (desc *TypeDescriptor) Adding() bool {
	return false
}

// Dropped implements the BaseDescriptorInterface interface.
func (desc *TypeDescriptor) Dropped() bool {
	return desc.State == TypeDescriptor_DROP
}

// Offline implements the BaseDescriptorInterface interface.
func (desc *TypeDescriptor) Offline() bool {
	return false
}

// GetOfflineReason implements the BaseDescriptorInterface interface.
func (desc *TypeDescriptor) GetOfflineReason() string {
	return ""
}

// DescriptorProto returns a Descriptor for serialization.
func (desc *TypeDescriptor) DescriptorProto() *Descriptor {
	return &Descriptor{
		Union: &Descriptor_Type{
			Type: desc,
		},
	}
}

// SetDrainingNames implements the MutableDescriptor interface.
func (desc *MutableTypeDescriptor) SetDrainingNames(names []NameInfo) {
	desc.DrainingNames = names
}

// GetAuditMode implements the DescriptorProto interface.
func (desc *TypeDescriptor) GetAuditMode() TableDescriptor_AuditMode {
	return TableDescriptor_DISABLED
}

// GetPrivileges implements the DescriptorProto interface.
//
// Types do not carry privileges.
func (desc *TypeDescriptor) GetPrivileges() *PrivilegeDescriptor {
	return nil
}

// TypeName implements the DescriptorProto interface.
func (desc *TypeDescriptor) TypeName() string {
	return "type"
}

// MaybeIncrementVersion implements the MutableDescriptor interface.
func (desc *MutableTypeDescriptor) MaybeIncrementVersion() {
	// Already incremented, no-op.
	if desc.ClusterVersion == nil || desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ModificationTime = hlc.Timestamp{}
}

// OriginalName implements the MutableDescriptor interface.
func (desc *MutableTypeDescriptor) OriginalName() string {
	if desc.ClusterVersion == nil {
		return ""
	}
	return desc.ClusterVersion.Name
}

// OriginalID implements the MutableDescriptor interface.
func (desc *MutableTypeDescriptor) OriginalID() ID {
	if desc.ClusterVersion == nil {
		return InvalidID
	}
	return desc.ClusterVersion.ID
}

// OriginalVersion implements the MutableDescriptor interface.
func (desc *MutableTypeDescriptor) OriginalVersion() DescriptorVersion {
	if desc.ClusterVersion == nil {
		return 0
	}
	return desc.ClusterVersion.Version
}

// Immutable implements the MutableDescriptor interface.
func (desc *MutableTypeDescriptor) Immutable() DescriptorInterface {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	return NewImmutableTypeDescriptor(*protoutil.Clone(desc.TypeDesc()).(*TypeDescriptor))
}

// IsNew implements the MutableDescriptor interface.
func (desc *MutableTypeDescriptor) IsNew() bool {
	return desc.ClusterVersion == nil
}

// AddEnumValue adds an enum member to the type.
func (desc *MutableTypeDescriptor) AddEnumValue(node *tree.AlterTypeAddValue) error {
	if desc.Kind != TypeDescriptor_ENUM {
		return pgerror.Newf(pgcode.WrongObjectType, "%q is not an enum", desc.Name)
	}
	// See if the value already exists in the enum or not.
	found := false
	for _, member := range desc.EnumMembers {
		if member.LogicalRepresentation == node.NewVal {
			found = true
			break
		}
	}
	if found {
		if node.IfNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateObject, "enum label %q already exists", node.NewVal)
	}

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
		existing := node.Placement.ExistingVal
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
	newMember := TypeDescriptor_EnumMember{
		LogicalRepresentation:  node.NewVal,
		PhysicalRepresentation: newPhysicalRep,
		Capability:             TypeDescriptor_EnumMember_READ_ONLY,
	}

	// Now, insert the new member.
	if len(desc.EnumMembers) == 0 {
		desc.EnumMembers = []TypeDescriptor_EnumMember{newMember}
	} else {
		if pos < 0 {
			// Insert the element in the front of the slice.
			desc.EnumMembers = append([]TypeDescriptor_EnumMember{newMember}, desc.EnumMembers...)
		} else {
			// Insert the element in front of pos.
			desc.EnumMembers = append(desc.EnumMembers, TypeDescriptor_EnumMember{})
			copy(desc.EnumMembers[pos+2:], desc.EnumMembers[pos+1:])
			desc.EnumMembers[pos+1] = newMember
		}
	}
	return nil
}

// AddReferencingDescriptorID adds a new referencing descriptor ID to the
// TypeDescriptor. It ensures that duplicates are not added.
func (desc *MutableTypeDescriptor) AddReferencingDescriptorID(new ID) {
	for _, id := range desc.ReferencingDescriptorIDs {
		if new == id {
			return
		}
	}
	desc.ReferencingDescriptorIDs = append(desc.ReferencingDescriptorIDs, new)
}

// RemoveReferencingDescriptorID removes the desired referencing descriptor ID
// from the TypeDescriptor. It has no effect if the requested ID is not present.
func (desc *MutableTypeDescriptor) RemoveReferencingDescriptorID(remove ID) {
	for i, id := range desc.ReferencingDescriptorIDs {
		if id == remove {
			desc.ReferencingDescriptorIDs = append(desc.ReferencingDescriptorIDs[:i], desc.ReferencingDescriptorIDs[i+1:]...)
			return
		}
	}
}

// EnumMembers is a sortable list of TypeDescriptor_EnumMember, sorted by the
// physical representation.
type EnumMembers []TypeDescriptor_EnumMember

func (e EnumMembers) Len() int { return len(e) }
func (e EnumMembers) Less(i, j int) bool {
	return bytes.Compare(e[i].PhysicalRepresentation, e[j].PhysicalRepresentation) < 0
}
func (e EnumMembers) Swap(i, j int) { e[i], e[j] = e[j], e[i] }

// Validate performs validation on the TypeDescriptor.
func (desc *TypeDescriptor) Validate(ctx context.Context, txn *kv.Txn, codec keys.SQLCodec) error {
	// Validate local properties of the descriptor.
	if err := validateName(desc.Name, "type"); err != nil {
		return err
	}

	if desc.ID == InvalidID {
		return errors.AssertionFailedf("invalid ID %d", errors.Safe(desc.ID))
	}
	if desc.ParentID == InvalidID {
		return errors.AssertionFailedf("invalid parentID %d", errors.Safe(desc.ParentID))
	}

	switch desc.Kind {
	case TypeDescriptor_ENUM:
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
	case TypeDescriptor_ALIAS:
		if desc.Alias == nil {
			return errors.AssertionFailedf("ALIAS type desc has nil alias type")
		}
	default:
		return errors.AssertionFailedf("invalid desc kind %s", desc.Kind.String())
	}

	// Validate all cross references on the descriptor.

	// Buffer all the requested requests and error checks together to run at once.
	b := txn.NewBatch()
	var opChecks []func(kv.KeyValue) error

	// Validate the parentID.
	b.Get(MakeDescMetadataKey(codec, desc.ParentID))
	opChecks = append(opChecks, func(k kv.KeyValue) error {
		if !k.Exists() {
			return errors.AssertionFailedf("parentID %d does not exist", errors.Safe(desc.ParentID))
		}
		return nil
	})

	// Validate the parentSchemaID.
	if desc.ParentSchemaID != keys.PublicSchemaID {
		b.Get(MakeDescMetadataKey(codec, desc.ParentSchemaID))
		opChecks = append(opChecks, func(k kv.KeyValue) error {
			if !k.Exists() {
				return errors.AssertionFailedf("parentSchemaID %d does not exist", errors.Safe(desc.ParentSchemaID))
			}
			return nil
		})
	}

	switch desc.Kind {
	case TypeDescriptor_ENUM:
		// Ensure that the referenced array type exists.
		b.Get(MakeDescMetadataKey(codec, desc.ArrayTypeID))
		opChecks = append(opChecks, func(k kv.KeyValue) error {
			if !k.Exists() {
				return errors.AssertionFailedf("arrayTypeID %d does not exist", errors.Safe(desc.ArrayTypeID))
			}
			return nil
		})
	case TypeDescriptor_ALIAS:
		if desc.ArrayTypeID != InvalidID {
			return errors.AssertionFailedf("ALIAS type desc has array type ID %d", desc.ArrayTypeID)
		}
	}

	// Validate that all of the referencing descriptors exist.
	if !desc.Dropped() {
		for _, id := range desc.ReferencingDescriptorIDs {
			b.Get(MakeDescMetadataKey(codec, id))
			opChecks = append(opChecks, func(k kv.KeyValue) error {
				if !k.Exists() {
					return errors.AssertionFailedf("referencing descriptor %d does not exist", id)
				}
				return nil
			})
		}
	}

	if err := txn.Run(ctx, b); err != nil {
		return err
	}

	// For each result in the batch, apply the corresponding check.
	for i := range opChecks {
		check := opChecks[i]
		res := b.Results[i]
		if res.Err != nil {
			return res.Err
		}
		if err := check(res.Rows[0]); err != nil {
			return err
		}
	}

	return nil
}

// TypeDescriptorResolver is an interface used during hydration of type
// metadata in types.T's. It is similar to tree.TypeReferenceResolver, except
// that it has the power to return TypeDescriptorInterface, rather than only a
// types.T. Implementers of tree.TypeReferenceResolver should implement this
// interface as well.
type TypeDescriptorResolver interface {
	// GetTypeDescriptor returns the type descriptor for the input ID.
	GetTypeDescriptor(ctx context.Context, id ID) (*tree.TypeName, TypeDescriptorInterface, error)
}

// TypeLookupFunc is a type alias for a function that looks up a type by ID.
type TypeLookupFunc func(ctx context.Context, id ID) (*tree.TypeName, TypeDescriptorInterface, error)

// GetTypeDescriptor implements the TypeDescriptorResolver interface.
func (t TypeLookupFunc) GetTypeDescriptor(
	ctx context.Context, id ID,
) (*tree.TypeName, TypeDescriptorInterface, error) {
	return t(ctx, id)
}

// MakeTypesT creates a types.T from the input type descriptor.
func (desc *ImmutableTypeDescriptor) MakeTypesT(
	ctx context.Context, name *tree.TypeName, res TypeDescriptorResolver,
) (*types.T, error) {
	switch t := desc.Kind; t {
	case TypeDescriptor_ENUM:
		typ := types.MakeEnum(uint32(desc.GetID()), uint32(desc.ArrayTypeID))
		if err := desc.HydrateTypeInfoWithName(ctx, typ, name, res); err != nil {
			return nil, err
		}
		return typ, nil
	case TypeDescriptor_ALIAS:
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
	ctx context.Context, desc *TableDescriptor, res TypeDescriptorResolver,
) error {
	hydrateCol := func(col *ColumnDescriptor) error {
		if col.Type.UserDefined() {
			// Look up its type descriptor.
			name, typDesc, err := res.GetTypeDescriptor(ctx, ID(col.Type.StableTypeID()))
			if err != nil {
				return err
			}
			// TODO (rohany): This should be a noop if the hydrated type
			//  information present in the descriptor has the same version as
			//  the resolved type descriptor we found here.
			if err := typDesc.HydrateTypeInfoWithName(ctx, col.Type, name, res); err != nil {
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
func (desc *ImmutableTypeDescriptor) HydrateTypeInfoWithName(
	ctx context.Context, typ *types.T, name *tree.TypeName, res TypeDescriptorResolver,
) error {
	typ.TypeMeta.Name = types.MakeUserDefinedTypeName(name.Catalog(), name.Schema(), name.Object())
	typ.TypeMeta.Version = uint32(desc.Version)
	switch desc.Kind {
	case TypeDescriptor_ENUM:
		if typ.Family() != types.EnumFamily {
			return errors.New("cannot hydrate a non-enum type with an enum type descriptor")
		}
		logical := make([]string, len(desc.EnumMembers))
		physical := make([][]byte, len(desc.EnumMembers))
		isReadOnly := make([]bool, len(desc.EnumMembers))
		for i := range desc.EnumMembers {
			member := &desc.EnumMembers[i]
			logical[i] = member.LogicalRepresentation
			physical[i] = member.PhysicalRepresentation
			isReadOnly[i] = member.Capability == TypeDescriptor_EnumMember_READ_ONLY
		}
		typ.TypeMeta.EnumData = &types.EnumMetadata{
			LogicalRepresentations:  logical,
			PhysicalRepresentations: physical,
			IsMemberReadOnly:        isReadOnly,
		}
		return nil
	case TypeDescriptor_ALIAS:
		if typ.UserDefined() {
			switch typ.Family() {
			case types.ArrayFamily:
				// Hydrate the element type.
				elemType := typ.ArrayContents()
				elemTypName, elemTypDesc, err := res.GetTypeDescriptor(ctx, ID(elemType.StableTypeID()))
				if err != nil {
					return err
				}
				if err := elemTypDesc.HydrateTypeInfoWithName(ctx, elemType, elemTypName, res); err != nil {
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
func (desc *TypeDescriptor) IsCompatibleWith(other *TypeDescriptor) error {
	switch desc.Kind {
	case TypeDescriptor_ENUM:
		if other.Kind != TypeDescriptor_ENUM {
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

// GetIDClosure returns all type descriptor IDs that are referenced by this
// type descriptor.
func (desc *TypeDescriptor) GetIDClosure() map[ID]struct{} {
	ret := make(map[ID]struct{})
	// Collect the descriptor's own ID.
	ret[desc.ID] = struct{}{}
	if desc.Kind == TypeDescriptor_ALIAS {
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
func GetTypeDescriptorClosure(typ *types.T) map[ID]struct{} {
	if !typ.UserDefined() {
		return map[ID]struct{}{}
	}
	// Collect the type's descriptor ID.
	ret := map[ID]struct{}{ID(typ.StableTypeID()): {}}
	if typ.Family() == types.ArrayFamily {
		// If we have an array type, then collect all types in the contents.
		children := GetTypeDescriptorClosure(typ.ArrayContents())
		for id := range children {
			ret[id] = struct{}{}
		}
	} else {
		// Otherwise, take the array type ID.
		ret[ID(typ.StableArrayTypeID())] = struct{}{}
	}
	return ret
}
