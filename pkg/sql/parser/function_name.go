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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
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
func (fn ResolvableFunctionReference) Format(buf *bytes.Buffer, f FmtFlags) {
	fn.FunctionReference.Format(buf, f)
}
func (fn ResolvableFunctionReference) String() string { return AsString(fn) }

// Resolve checks if the function name is already resolved and
// resolves it as necessary.
func (fn *ResolvableFunctionReference) Resolve(searchPath SearchPath) (*FunctionDefinition, error) {
	switch t := fn.FunctionReference.(type) {
	case *FunctionDefinition:
		return t, nil
	case UnresolvedName:
		fd, err := t.ResolveFunction(searchPath)
		if err != nil {
			return nil, err
		}
		fn.FunctionReference = fd
		return fd, nil
	default:
		panic(fmt.Sprintf("unsupported function name: %+v (%T)",
			fn.FunctionReference, fn.FunctionReference,
		))
	}
}

// wrapFunction creates a new ResolvableFunctionReference
// holding a pre-resolved function. Helper for grammar rules.
func wrapFunction(n string) ResolvableFunctionReference {
	fd, ok := funDefs[n]
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

func (UnresolvedName) functionReference()      {}
func (*FunctionDefinition) functionReference() {}

// functionName implements a structured function name. It is an
// intermediate step between an UnresolvedName and a
// FunctionDefinition.
type functionName struct {
	prefixName   Name
	functionName Name
	selector     NameParts
}

// normalizeFunctionName transforms an UnresolvedName to a functionName.
func (n UnresolvedName) normalizeFunctionName() (functionName, error) {
	if len(n) == 0 {
		return functionName{}, fmt.Errorf("invalid function name: %s", n)
	}

	// Find the first array subscript, if any.
	i := len(n)
	for j, p := range n {
		if _, ok := p.(*ArraySubscript); ok {
			i = j
			break
		}
	}

	// There must be something before the array subscript.
	if i == 0 {
		return functionName{}, fmt.Errorf("invalid function name: %s", n)
	}

	// The function name, together with its prefix, must /look/ like a
	// table name. (We don't support record types yet.)  Reuse the
	// existing normalization code.
	tn, err := n[:i].normalizeTableNameAsValue()
	if err != nil {
		// Override the error, so as to not confuse the user.
		return functionName{}, fmt.Errorf("invalid function name: %s", n)
	}

	// Everything afterwards is the selector.
	return functionName{
		prefixName:   tn.DatabaseName,
		functionName: tn.TableName,
		selector:     NameParts(n[i:]),
	}, nil
}

// Function retrieves the unqualified function name.
func (fn *functionName) function() string {
	return string(fn.functionName)
}

// Prefix retrieves the unqualified prefix.
func (fn *functionName) prefix() string {
	return string(fn.prefixName)
}
