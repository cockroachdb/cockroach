// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
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
// of a FuncExpr. The FunctionRerence is updated by the Normalize()
// method.
type ResolvableFunctionReference struct {
	FunctionReference
}

// Format implements the NodeFormatter interface.
func (fn *ResolvableFunctionReference) Format(ctx *FmtCtx) {
	ctx.FormatNode(fn.FunctionReference)
}
func (fn *ResolvableFunctionReference) String() string { return AsString(fn) }

// Resolve checks if the function name is already resolved and
// resolves it as necessary.
func (fn *ResolvableFunctionReference) Resolve(
	searchPath sessiondata.SearchPath,
) (*FunctionDefinition, error) {
	switch t := fn.FunctionReference.(type) {
	case *FunctionDefinition:
		return t, nil
	case *UnresolvedName:
		fd, err := t.ResolveFunction(searchPath)
		if err != nil {
			return nil, err
		}
		fn.FunctionReference = fd
		return fd, nil
	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError,
			"programming error: unknown function name type: %+v (%T)",
			fn.FunctionReference, fn.FunctionReference,
		)
	}
}

// WrapFunction creates a new ResolvableFunctionReference
// holding a pre-resolved function. Helper for grammar rules.
func WrapFunction(n string) ResolvableFunctionReference {
	fd, ok := FunDefs[n]
	if !ok {
		panic(fmt.Sprintf("function %s() not defined", n))
	}
	return ResolvableFunctionReference{fd}
}

// FunctionReference is the common interface to UnresolvedName and QualifiedFunctionName.
type FunctionReference interface {
	fmt.Stringer
	NodeFormatter
	functionReference()
}

func (*UnresolvedName) functionReference()     {}
func (*FunctionDefinition) functionReference() {}
