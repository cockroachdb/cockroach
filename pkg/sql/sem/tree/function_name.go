// Copyright 2016 The Cockroach Authors.
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
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Function names are used in expressions in the FuncExpr node.
// General syntax:
//    [ <context-prefix> . ] <function-name>
//
// The other syntax nodes hold a mutable ResolvableFunctionReference
// attribute.  This is populated during parsing with an
// UnresolvedObjectName, and gets assigned a FunctionDefinition upon the
// first call to its Resolve() method.

// ResolvableFunctionReference implements the editable reference cell
// of a FuncExpr. The FunctionRerence is updated by the Normalize()
// method.
type ResolvableFunctionReference struct {
	FunctionReference FunctionReference
}

// Format implements the NodeFormatter interface.
func (fn *ResolvableFunctionReference) Format(ctx *FmtCtx) {
	ctx.FormatNode(fn.FunctionReference)
}
func (fn *ResolvableFunctionReference) String() string { return AsString(fn) }

// Resolve checks if the function name is already resolved and
// resolves it as necessary.
func (fn *ResolvableFunctionReference) Resolve(
	ctx context.Context, resolver FuncReferenceResolver, searchPath sessiondata.SearchPath,
) (*FunctionDefinition, error) {
	switch t := fn.FunctionReference.(type) {
	case *FunctionDefinition:
		return t, nil
	case *UnresolvedObjectName:
		var fd *FunctionDefinition
		var err error
		if resolver == nil {
			fd, err = t.ResolveFunction(searchPath)
		} else {
			fd, err = resolver.ResolveFunc(ctx, searchPath, t)
		}
		if err != nil {
			return nil, err
		}
		fn.FunctionReference = fd
		return fd, nil
	default:
		return nil, errors.AssertionFailedf("unknown function name type: %+v (%T)",
			fn.FunctionReference, fn.FunctionReference,
		)
	}
}

// WrapFunction creates a new ResolvableFunctionReference
// holding a pre-resolved function. Helper for grammar rules.
func WrapFunction(n string) ResolvableFunctionReference {
	fd, ok := FunDefs[n]
	if !ok {
		panic(errors.AssertionFailedf("function %s() not defined", log.Safe(n)))
	}
	return ResolvableFunctionReference{fd}
}

// FunctionReference is the common interface to UnresolvedName and QualifiedFunctionName.
type FunctionReference interface {
	fmt.Stringer
	NodeFormatter
	functionReference()
}

func (*UnresolvedName) functionReference()       {}
func (*UnresolvedObjectName) functionReference() {}
func (*FunctionDefinition) functionReference()   {}

// FuncName corresponds to the name of a type in a CREATE FUNCTION statement.
type FuncName struct {
	objName
}

var _ ObjectName = &FuncName{}

// Satisfy the linter.
var _ = (*TypeName).Type
var _ = (*TypeName).FQString

func NewUnqualifiedFuncName(fnc Name) *FuncName {
	return &FuncName{objName{ObjectName: fnc}}
}

// MakeNewQualifiedTypeName creates a fully qualified type name.
func MakeNewQualifiedFuncName(db, schema, typ string) FuncName {
	return FuncName{objName{
		ObjectNamePrefix: ObjectNamePrefix{
			ExplicitCatalog: true,
			ExplicitSchema:  true,
			CatalogName:     Name(db),
			SchemaName:      Name(schema),
		},
		ObjectName: Name(typ),
	}}
}

// Format implements the NodeFormatter interface.
func (t *FuncName) Format(ctx *FmtCtx) {
	t.ObjectNamePrefix.Format(ctx)
	if t.ExplicitSchema || ctx.alwaysFormatTablePrefix() {
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&t.ObjectName)
}

// String implements the Stringer interface.
func (t *FuncName) String() string {
	return AsString(t)
}

// FQString renders the type name in full, not omitting the prefix
// schema and catalog names. Suitable for logging, etc.
func (t *FuncName) FQString() string {
	ctx := NewFmtCtx(FmtSimple)
	ctx.FormatNode(&t.CatalogName)
	ctx.WriteByte('.')
	ctx.FormatNode(&t.SchemaName)
	ctx.WriteByte('.')
	ctx.FormatNode(&t.ObjectName)
	return ctx.CloseAndGetString()
}

func (t *FuncName) objectName() {}

// TypeReferenceResolver is the interface that will provide the ability
// to actually look up type metadata and transform references into
// *types.T's. Implementers of TypeReferenceResolver should also implement
// descpb.TypeDescriptorResolver is sqlbase.TypeDescriptorInterface is the
// underlying representation of a user defined type.
type FuncReferenceResolver interface {
	ResolveFunc(ctx context.Context,
		searchPath sessiondata.SearchPath,
		name *UnresolvedObjectName,
	) (*FunctionDefinition, error)
}
