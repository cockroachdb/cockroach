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
	"fmt"

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
	fmt.Stringer
	NodeFormatter
	typeReference()
	Resolve(resolver TypeReferenceResolver) (*types.T, error)
	SQLString() string
}

// KnownType is a wrapper around *types.T that represents a type reference
// that has a statically known type.
type KnownType struct {
	*types.T
}

// Format implements the NodeFormatter interface.
func (node *KnownType) Format(ctx *FmtCtx) {
	ctx.WriteString(node.String())
}

// Resolve implements the ResolvableTypeReference interface.
func (node *KnownType) Resolve(resolver TypeReferenceResolver) (*types.T, error) {
	return node.T, nil
}

// MakeKnownType demotes a *types.T into a ResolvableTypeReference.
func MakeKnownType(typ *types.T) ResolvableTypeReference {
	return &KnownType{typ}
}

// GetKnownType possibly promotes a ResolvableTypeReference into a *types.T
// if the reference is a statically known type. It returns nil otherwise.
func GetKnownType(ref ResolvableTypeReference) *types.T {
	if typ, ok := ref.(*KnownType); ok {
		return typ.T
	}
	return nil
}

// GetKnownTypeOrPanic does the same thing as GetKnownType but panics
// in the case that the reference is not statically known. This function
// is intended to be used in tests or in cases where it is not possible
// to have any unresolved type references.
func GetKnownTypeOrPanic(ref ResolvableTypeReference) *types.T {
	if typ, ok := ref.(*KnownType); ok {
		return typ.T
	}
	panic("type reference was not a statically known type")
}

// ArrayTypeReference represents an array of possibly unknown type references.
type ArrayTypeReference struct {
	InternalType ResolvableTypeReference
}

// Format implements the NodeFormatter interface.
func (node *ArrayTypeReference) Format(ctx *FmtCtx) {
	if typ := GetKnownType(node.InternalType); typ != nil {
		ctx.WriteString(types.MakeArray(typ).SQLString())
	} else {
		ctx.FormatNode(node.InternalType)
		ctx.WriteString("[]")
	}
}

// String implements the Stringer interface.
func (node *ArrayTypeReference) String() string {
	return AsString(node)
}

// SQLString implements the ResolvableTypeReference interface.
func (node *ArrayTypeReference) SQLString() string {
	return node.String()
}

// Resolve implements the ResolvableTypeReference interface.
func (node *ArrayTypeReference) Resolve(resolver TypeReferenceResolver) (*types.T, error) {
	typ, err := node.InternalType.Resolve(resolver)
	if err != nil {
		return nil, err
	}
	return types.MakeArray(typ), nil
}

// Resolve implements the ResolvableTypeReference interface.
func (name *UnresolvedObjectName) Resolve(resolver TypeReferenceResolver) (*types.T, error) {
	return nil, errors.AssertionFailedf("resolution of unresolved names currently unsupported")
}

// SQLString implements the ResolvableTypeReference interface.
func (name *UnresolvedObjectName) SQLString() string {
	return name.String()
}

// IsReferenceSerialType returns whether the input reference is a known
// serial type. It should only be used during parsing.
func IsReferenceSerialType(ref ResolvableTypeReference) bool {
	if typ := GetKnownType(ref); typ != nil {
		return types.IsSerialType(typ)
	}
	return false
}

var _ ResolvableTypeReference = &KnownType{}
var _ ResolvableTypeReference = &UnresolvedObjectName{}
var _ ResolvableTypeReference = &ArrayTypeReference{}

func (*KnownType) typeReference()            {}
func (*UnresolvedObjectName) typeReference() {}
func (*ArrayTypeReference) typeReference()   {}
