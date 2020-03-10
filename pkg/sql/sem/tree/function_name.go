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
// UnresolvedName, and gets assigned a FunctionDefinition upon the
// first call to its Resolve() method.

// ResolvableFunctionReference implements the editable reference cell
// of a FuncExpr.
type ResolvableFunctionReference struct {
	Name             *UnresolvedName
	ResolvedFunction *FunctionDefinition
}

// Format implements the NodeFormatter interface.
func (fn *ResolvableFunctionReference) Format(ctx *FmtCtx) {
	// This Format method tries to use the UnresolvedName component first, because
	// ResolvableFunctionReference is unfortunately mutable at the current time,
	// which leads to data races when formatting ASTs for SHOW QUERIES and the
	// like, because the Optimizer might be mutating this node while someone is
	// asking to read it. To get around this, we just print out the UnresolvedName
	// if it exists. If it doesn't, which sometimes happen when we synthesize an
	// RFR internally, we fall back to formatting the resolved function itself,
	// which in that case will be immutable as well.
	if fn.Name != nil {
		ctx.FormatNode(fn.Name)
	} else {
		ctx.FormatNode(fn.ResolvedFunction)
	}
}
func (fn *ResolvableFunctionReference) String() string { return AsString(fn) }

// Resolve checks if the function name is already resolved and
// resolves it as necessary.
func (fn *ResolvableFunctionReference) Resolve(
	searchPath sessiondata.SearchPath,
) (*FunctionDefinition, error) {
	if fn.ResolvedFunction == nil {
		fd, err := fn.Name.ResolveFunction(searchPath)
		if err != nil {
			return nil, err
		}
		fn.ResolvedFunction = fd
	}
	return fn.ResolvedFunction, nil
}

// WrapFunction creates a new ResolvableFunctionReference
// holding a pre-resolved function. Helper for grammar rules.
func WrapFunction(n string) ResolvableFunctionReference {
	fd, ok := FunDefs[n]
	if !ok {
		panic(errors.AssertionFailedf("function %s() not defined", log.Safe(n)))
	}
	return ResolvableFunctionReference{ResolvedFunction: fd}
}

func (*UnresolvedName) functionReference()     {}
func (*FunctionDefinition) functionReference() {}
