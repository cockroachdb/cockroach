// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// TypeReferenceResolver is the interface that will provide the ability
// to actually lookup type metadata and transform references into
// *types.T's. In practice, this will probably be implemented by
// the planner, but for now it is a dummy interface.
type TypeReferenceResolver interface {
	ResolveType() (*types.T, error)
}

// ResolvableTypeReference represents a type that is possibly unknown
// until type-checking/type name resolution is performed.
type ResolvableTypeReference interface {
	SQLString() string
}

func ResolveType(ref ResolvableTypeReference, resolver TypeReferenceResolver) (*types.T, error) {
	switch t := ref.(type) {
	case *types.T:
		return t, nil
	case *ArrayTypeReference:
		typ, err := ResolveType(t.InternalType, resolver)
		if err != nil {
			return nil, err
		}
		return types.MakeArray(typ), nil
	}
	return nil, errors.AssertionFailedf("Hiya!")
}

// GetStaticallyKnownType possibly promotes a ResolvableTypeReference into a *types.T
// if the reference is a statically known type. It returns nil otherwise.
func GetStaticallyKnownType(ref ResolvableTypeReference) *types.T {
	if typ, ok := ref.(*types.T); ok {
		return typ
	}
	return nil
}

// MustBeStaticallyKnownType does the same thing as GetStaticallyKnownType but panics
// in the case that the reference is not statically known. This function
// is intended to be used in tests or in cases where it is not possible
// to have any unresolved type references.
func MustBeStaticallyKnownType(ref ResolvableTypeReference) *types.T {
	if typ, ok := ref.(*types.T); ok {
		return typ
	}
	panic("type reference was not a statically known type")
}

// ArrayTypeReference represents an array of possibly unknown type references.
type ArrayTypeReference struct {
	InternalType ResolvableTypeReference
}

// SQLString implements the ResolvableTypeReference interface.
func (node *ArrayTypeReference) SQLString() string {
	var ctx FmtCtx
	if typ := GetStaticallyKnownType(node.InternalType); typ != nil {
		ctx.WriteString(types.MakeArray(typ).SQLString())
	} else {
		ctx.WriteString(node.InternalType.SQLString())
		ctx.WriteString("[]")
	}
	return ctx.String()
}

// SQLString implements the ResolvableTypeReference interface.
func (name *UnresolvedObjectName) SQLString() string {
	return name.String()
}

// IsReferenceSerialType returns whether the input reference is a known
// serial type. It should only be used during parsing.
func IsReferenceSerialType(ref ResolvableTypeReference) bool {
	if typ := GetStaticallyKnownType(ref); typ != nil {
		return types.IsSerialType(typ)
	}
	return false
}

var _ ResolvableTypeReference = &UnresolvedObjectName{}
var _ ResolvableTypeReference = &ArrayTypeReference{}
var _ ResolvableTypeReference = &types.T{}
