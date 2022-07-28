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
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Function names are used in expressions in the FuncExpr node.
// General syntax:
//    [ <context-prefix> . ] <function-name>
//
// The other syntax nodes hold a mutable ResolvableFunctionReference
// attribute.  This is populated during parsing with an
// UnresolvedName, and gets assigned a FunctionDefinition upon the
// first call to its ResolveFunction() method.

// FunctionReferenceResolver is the interface that provides the ability to
// resolve built-in or user-defined function definitions from unresolved names.
type FunctionReferenceResolver interface {
	// ResolveFunction resolves a group of overloads with the given function name
	// within a search path.
	// TODO(Chengxiong): Consider adding an optional slice of argument types to
	// the input of this method, so that we can try to narrow down the scope of
	// overloads a bit earlier and decrease the possibility of ambiguous error
	// on function properties.
	ResolveFunction(name *UnresolvedName, path SearchPath) (*FunctionDefinition, error)
}

// ResolvableFunctionReference implements the editable reference call of a
// FuncExpr.
type ResolvableFunctionReference struct {
	FunctionReference
}

// Resolve converts a ResolvableFunctionReference into a *FunctionDefinition. If
// the reference has already been resolved, it simply returns the definition. If
// a FunctionReferenceResolver is provided, it will be used to resolve the
// function definition. Otherwise, the default resolution of
// UnresolvedName.ResolveFunction is used.
func (ref *ResolvableFunctionReference) Resolve(
	path SearchPath, resolver FunctionReferenceResolver,
) (*FunctionDefinition, error) {
	switch t := ref.FunctionReference.(type) {
	case *FunctionDefinition:
		return t, nil
	case *UnresolvedName:
		var fd *FunctionDefinition
		var err error
		if resolver == nil {
			// Use the default resolution logic if there is no resolver.
			fd, err = t.ResolveFunction(path)
		} else {
			fd, err = resolver.ResolveFunction(t, path)
		}
		if err != nil {
			return nil, err
		}
		ref.FunctionReference = fd
		return fd, nil
	default:
		return nil, errors.AssertionFailedf("unknown resolvable function reference type %s", t)
	}
}

// WrapFunction creates a new ResolvableFunctionReference holding a pre-resolved
// function from a built-in function name. Helper for grammar rules and
// execbuilder.
func WrapFunction(n string) ResolvableFunctionReference {
	fd, ok := FunDefs[n]
	if !ok {
		panic(errors.AssertionFailedf("function %s() not defined", redact.Safe(n)))
	}
	return ResolvableFunctionReference{fd}
}

// FunctionReference is the common interface to UnresolvedName and QualifiedFunctionName.
type FunctionReference interface {
	fmt.Stringer
	NodeFormatter
	functionReference()
}

var _ FunctionReference = &UnresolvedName{}
var _ FunctionReference = &FunctionDefinition{}

func (*UnresolvedName) functionReference()     {}
func (*FunctionDefinition) functionReference() {}
