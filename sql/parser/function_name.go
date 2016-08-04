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
// The other syntax nodes hold a mutable NormalizableFunctionName
// attribute.  This is populated during parsing with an
// UnresolvedName, and gets assigned a QualifiedFunctionName upon the
// first call to its Normalize() method.

// NormalizableFunctionName implements the editable name cell of a
// FuncExpr. The FunctionName reference is updated by the Normalize()
// method.
type NormalizableFunctionName struct {
	FunctionName FunctionName
}

// Format implements the NodeFormatter interface.
func (fn NormalizableFunctionName) Format(buf *bytes.Buffer, f FmtFlags) {
	fn.FunctionName.Format(buf, f)
}
func (fn NormalizableFunctionName) String() string { return AsString(fn) }

// Normalize checks if the function name is already normalized and
// normalizes it as necessary.
func (fn *NormalizableFunctionName) Normalize() (*QualifiedFunctionName, error) {
	switch t := fn.FunctionName.(type) {
	case *QualifiedFunctionName:
		return t, nil
	case UnresolvedName:
		qfn, err := t.NormalizeFunctionName()
		if err != nil {
			return nil, err
		}
		fn.FunctionName = qfn
		return qfn, nil
	default:
		panic(fmt.Sprintf("unsupported function name: %+v (%T)", fn.FunctionName, fn.FunctionName))
	}
}

// WrapQualifiedFunctionName creates a new NormalizableFunctionName
// holding a pre-normalized function name. Helper for grammar rules.
func WrapQualifiedFunctionName(n string) NormalizableFunctionName {
	return NormalizableFunctionName{&QualifiedFunctionName{FunctionName: Name(n)}}
}

// FunctionName is the common interface to UnresolvedName and QualifiedFunctionName.
type FunctionName interface {
	fmt.Stringer
	NodeFormatter
	functionName()
}

func (UnresolvedName) functionName()            {}
func (fn *QualifiedFunctionName) functionName() {}

// QualifiedFunctionName implements a normalized function name.
type QualifiedFunctionName struct {
	FunctionName Name
	Context      NameParts
}

// NormalizeFunctionName transforms an UnresolvedName to a QualifiedFunctionName.
func (n UnresolvedName) NormalizeFunctionName() (*QualifiedFunctionName, error) {
	if len(n) == 0 {
		return nil, fmt.Errorf("invalid function name: %q", n)
	}

	name, ok := n[len(n)-1].(Name)
	if !ok {
		return nil, fmt.Errorf("invalid function name: %q", n)
	}

	if len(name) == 0 {
		return nil, fmt.Errorf("empty function name: %q", n)
	}

	return &QualifiedFunctionName{FunctionName: name, Context: NameParts(n[:len(n)-1])}, nil
}

// Format implements the NodeFormatter interface.
func (fn *QualifiedFunctionName) Format(buf *bytes.Buffer, f FmtFlags) {
	if len(fn.Context) > 0 {
		FormatNode(buf, f, fn.Context)
		buf.WriteByte('.')
	}
	FormatNode(buf, f, fn.FunctionName)
}

func (fn *QualifiedFunctionName) String() string { return AsString(fn) }

// Function retrieves the unqualified function name.
func (fn *QualifiedFunctionName) Function() string {
	return string(fn.FunctionName)
}
