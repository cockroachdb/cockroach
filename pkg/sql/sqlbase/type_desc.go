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

	"github.com/cockroachdb/cockroach/pkg/keys"
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
	HydrateTypeInfoWithName(typ *types.T, name *tree.TypeName, typeLookup TypeLookupFunc) error
	MakeTypesT(name *tree.TypeName, typeLookup TypeLookupFunc) (*types.T, error)
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

// DescriptorProto returns a Descriptor for serialization.
func (desc *TypeDescriptor) DescriptorProto() *Descriptor {
	return &Descriptor{
		Union: &Descriptor_Type{
			Type: desc,
		},
	}
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
	if desc.Version == desc.ClusterVersion.Version+1 {
		return
	}
	desc.Version++
	desc.ModificationTime = hlc.Timestamp{}
}

// Immutable implements the MutableDescriptor interface.
func (desc *MutableTypeDescriptor) Immutable() DescriptorInterface {
	// TODO (lucy): Should the immutable descriptor constructors always make a
	// copy, so we don't have to do it here?
	return NewImmutableTypeDescriptor(*protoutil.Clone(desc.TypeDesc()).(*TypeDescriptor))
}

// MakeTypesT creates a types.T from the input type descriptor.
func (desc *ImmutableTypeDescriptor) MakeTypesT(
	name *tree.TypeName, typeLookup TypeLookupFunc,
) (*types.T, error) {
	switch t := desc.Kind; t {
	case TypeDescriptor_ENUM:
		typ := types.MakeEnum(uint32(desc.GetID()), uint32(desc.ArrayTypeID))
		if err := desc.HydrateTypeInfoWithName(typ, name, typeLookup); err != nil {
			return nil, err
		}
		return typ, nil
	case TypeDescriptor_ALIAS:
		// Hydrate the alias and return it.
		if err := desc.HydrateTypeInfoWithName(desc.Alias, name, typeLookup); err != nil {
			return nil, err
		}
		return desc.Alias, nil
	default:
		return nil, errors.AssertionFailedf("unknown type kind %s", t.String())
	}
}

// TypeLookupFunc is a type alias for a function that looks up a type by ID.
type TypeLookupFunc func(id ID) (*tree.TypeName, TypeDescriptorInterface, error)

// HydrateTypesInTableDescriptor uses typeLookup to install metadata in the
// types present in a table descriptor. typeLookup retrieves the fully
// qualified name and descriptor for a particular ID.
func HydrateTypesInTableDescriptor(desc *TableDescriptor, typeLookup TypeLookupFunc) error {
	hydrateCol := func(col *ColumnDescriptor) error {
		if col.Type.UserDefined() {
			// Look up its type descriptor.
			name, typDesc, err := typeLookup(ID(col.Type.StableTypeID()))
			if err != nil {
				return err
			}
			// TODO (rohany): This should be a noop if the hydrated type
			//  information present in the descriptor has the same version as
			//  the resolved type descriptor we found here.
			if err := typDesc.HydrateTypeInfoWithName(col.Type, name, typeLookup); err != nil {
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
	typ *types.T, name *tree.TypeName, typeLookup TypeLookupFunc,
) error {
	typ.TypeMeta.Name = types.MakeUserDefinedTypeName(name.Catalog(), name.Schema(), name.Object())
	switch desc.Kind {
	case TypeDescriptor_ENUM:
		if typ.Family() != types.EnumFamily {
			return errors.New("cannot hydrate a non-enum type with an enum type descriptor")
		}
		logical := make([]string, len(desc.EnumMembers))
		physical := make([][]byte, len(desc.EnumMembers))
		for i := range desc.EnumMembers {
			member := &desc.EnumMembers[i]
			logical[i] = member.LogicalRepresentation
			physical[i] = member.PhysicalRepresentation
		}
		typ.TypeMeta.EnumData = &types.EnumMetadata{
			LogicalRepresentations:  logical,
			PhysicalRepresentations: physical,
		}
		return nil
	case TypeDescriptor_ALIAS:
		if typ.UserDefined() {
			switch typ.Family() {
			case types.ArrayFamily:
				// Hydrate the element type.
				elemType := typ.ArrayContents()
				elemTypName, elemTypDesc, err := typeLookup(ID(elemType.StableTypeID()))
				if err != nil {
					return err
				}
				if err := elemTypDesc.HydrateTypeInfoWithName(elemType, elemTypName, typeLookup); err != nil {
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
