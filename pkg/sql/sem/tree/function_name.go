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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
)

// ErrFunctionUndefined indicates that the required function is not found. It is
// used as the cause of the errors thrown when function resolution cannot find a
// required function.
var ErrFunctionUndefined = pgerror.Newf(pgcode.UndefinedFunction, "function undefined")

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
	// within a search path. An error with ErrFunctionUndefined cause is returned
	// if function does not exist.
	//
	// TODO(Chengxiong): Consider adding an optional slice of argument types to
	// the input of this method, so that we can try to narrow down the scope of
	// overloads a bit earlier and decrease the possibility of ambiguous error
	// on function properties.
	ResolveFunction(
		ctx context.Context, name *UnresolvedName, path SearchPath,
	) (*ResolvedFunctionDefinition, error)

	// ResolveFunctionByOID looks up a function overload by using a given oid.
	// Function name is returned together with the overload. Error is thrown if
	// there is no function with the same oid.
	ResolveFunctionByOID(
		ctx context.Context, oid oid.Oid,
	) (string, *Overload, error)
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
	ctx context.Context, path SearchPath, resolver FunctionReferenceResolver,
) (*ResolvedFunctionDefinition, error) {
	switch t := ref.FunctionReference.(type) {
	case *ResolvedFunctionDefinition:
		return t, nil
	case *FunctionDefinition:
		// TODO(Chengxiong): get rid of FunctionDefinition entirely.
		parts := strings.Split(t.Name, ".")
		if len(parts) > 2 {
			// In theory, this should not happen since all builtin functions are
			// defined within virtual schema and don't belong to any database catalog.
			return nil, errors.AssertionFailedf("invalid builtin function name: %q", t.Name)
		}
		fullName := t.Name
		if len(parts) == 1 {
			fullName = catconstants.PgCatalogName + "." + t.Name
		}
		fd := ResolvedBuiltinFuncDefs[fullName]
		ref.FunctionReference = fd
		return fd, nil
	case *UnresolvedName:
		if resolver == nil {
			// If a resolver is not provided, just try to fetch a builtin function.
			fn, err := t.ToFunctionName()
			if err != nil {
				return nil, err
			}
			fd, err := GetBuiltinFuncDefinitionOrFail(fn, path)
			if err != nil {
				return nil, err
			}
			ref.FunctionReference = fd
			return fd, nil
		}
		// Use the resolver if it is provided.
		fd, err := resolver.ResolveFunction(ctx, t, path)
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
//
// TODO(Chengxiong): get rid of FunctionDefinition entirely and use
// ResolvedFunctionDefinition instead.
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
var _ FunctionReference = &ResolvedFunctionDefinition{}

func (*UnresolvedName) functionReference()             {}
func (*FunctionDefinition) functionReference()         {}
func (*ResolvedFunctionDefinition) functionReference() {}
