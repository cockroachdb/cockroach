// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package typedesc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// HydrateTypesInDescriptor ensures that all types that the descriptor is
// depending on are hydrated.
func HydrateTypesInDescriptor(
	ctx context.Context, desc catalog.Descriptor, res catalog.TypeDescriptorResolver,
) error {
	return desc.ForEachUDTDependentForHydration(func(t *types.T) error {
		return EnsureTypeIsHydrated(ctx, t, res)
	})
}

// ResolveHydratedTByOID is a convenience function which delegates to
// HydratedTFromDesc after resolving a type descriptor by its OID.
func ResolveHydratedTByOID(
	ctx context.Context, oid oid.Oid, res catalog.TypeDescriptorResolver,
) (*types.T, error) {
	id := UserDefinedTypeOIDToID(oid)
	name, desc, err := res.GetTypeDescriptor(ctx, id)
	if err != nil {
		return nil, err
	}
	return HydratedTFromDesc(ctx, &name, desc, res)
}

// HydratedTFromDesc returns a types.T corresponding to the type descriptor.
// The types.T return value is hydrated using the provided name if possible.
func HydratedTFromDesc(
	ctx context.Context,
	name *tree.TypeName,
	desc catalog.TypeDescriptor,
	res catalog.TypeDescriptorResolver,
) (*types.T, error) {
	t := desc.AsTypesT()
	if err := ensureTypeIsHydratedRecursive(ctx, t, name, desc, res); err != nil {
		return nil, err
	}
	return t, nil
}

// EnsureTypeIsHydrated makes sure that t is a fully-hydrated type.
func EnsureTypeIsHydrated(
	ctx context.Context, t *types.T, res catalog.TypeDescriptorResolver,
) error {
	return ensureTypeIsHydratedRecursive(ctx, t, nil /* maybeName */, nil /* maybeDesc */, res)
}

func ensureTypeIsHydratedRecursive(
	ctx context.Context,
	t *types.T,
	maybeName *tree.TypeName,
	maybeDesc catalog.TypeDescriptor,
	res catalog.TypeDescriptorResolver,
) error {
	switch t.Family() {
	case types.ArrayFamily:
		e := t.ArrayContents()
		if err := ensureTypeIsHydratedRecursive(ctx, e, maybeName, maybeDesc, res); err != nil {
			return err
		}
	case types.TupleFamily:
		for _, e := range t.TupleContents() {
			if err := ensureTypeIsHydratedRecursive(ctx, e, maybeName, maybeDesc, res); err != nil {
				return err
			}
		}
	}
	// Ensure that we have the descriptor for a user-defined type.
	// Note that non-user-defined types may or may not have descriptors
	// but still need to be hydrated using the name.
	if t.UserDefined() {
		id := GetUserDefinedTypeDescID(t)
		if maybeDesc == nil || maybeDesc.GetID() != id {
			if res == nil {
				return errors.AssertionFailedf("expected non-nil catalog.TypeDescriptorResolver")
			}
			var err error
			var name tree.TypeName
			name, maybeDesc, err = res.GetTypeDescriptor(ctx, id)
			if err != nil {
				return err
			}
			maybeName = &name
		}
	}
	ensureTypeMetadataIsHydrated(&t.TypeMeta, maybeName, maybeDesc)
	return nil
}

func ensureTypeMetadataIsHydrated(
	tm *types.UserDefinedTypeMetadata, maybeName *tree.TypeName, maybeDesc catalog.TypeDescriptor,
) {
	var version uint32
	if maybeDesc != nil {
		version = uint32(maybeDesc.GetVersion())
	} else if maybeName == nil {
		// Return early because there's nothing to hydrate with.
		return
	}
	if *tm != (types.UserDefinedTypeMetadata{}) && tm.Version == version {
		return
	}
	tm.Version = version
	if maybeName != nil {
		tm.Name = &types.UserDefinedTypeName{
			Catalog:        maybeName.Catalog(),
			ExplicitSchema: maybeName.ExplicitSchema,
			Schema:         maybeName.Schema(),
			Name:           maybeName.Object(),
		}
	}
	if maybeDesc == nil {
		return
	}
	if maybeDesc.AsTableImplicitRecordTypeDescriptor() != nil {
		tm.ImplicitRecordType = true
		return
	}
	if e := maybeDesc.AsEnumTypeDescriptor(); e != nil {
		if imm, ok := e.(*immutable); ok {
			// Fast-path for immutable enum descriptors. We can use a pointer into the
			// immutable descriptor's slices, since the TypMetadata is immutable too.
			tm.EnumData = &types.EnumMetadata{
				LogicalRepresentations:  imm.logicalReps,
				PhysicalRepresentations: imm.physicalReps,
				IsMemberReadOnly:        imm.readOnlyMembers,
			}
		} else {
			n := e.NumEnumMembers()
			tm.EnumData = &types.EnumMetadata{
				LogicalRepresentations:  make([]string, n),
				PhysicalRepresentations: make([][]byte, n),
				IsMemberReadOnly:        make([]bool, n),
			}
			for i := 0; i < n; i++ {
				tm.EnumData.LogicalRepresentations[i] = e.GetMemberLogicalRepresentation(i)
				tm.EnumData.PhysicalRepresentations[i] = e.GetMemberPhysicalRepresentation(i)
				tm.EnumData.IsMemberReadOnly[i] = e.IsMemberReadOnly(i)
			}
		}
	}
}
