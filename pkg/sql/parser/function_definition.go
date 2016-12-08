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
	"strings"
)

// FunctionDefinition implements a reference to one or more function
// overloads for a built-in function.
type FunctionDefinition struct {
	// Name is the short name of the function.
	Name string
	// HasRowDependentOverloads is true if one or more of the overload
	// definitions has RowDependent set.
	HasRowDependentOverloads bool
	// Definition is the set of overloads for this function name.
	Definition []Builtin
}

func newFunctionDefinition(name string, def []Builtin) *FunctionDefinition {
	hasRowDependentOverloads := false
	for _, d := range def {
		if d.RowDependent {
			hasRowDependentOverloads = true
			break
		}
	}
	return &FunctionDefinition{
		Name: name,
		HasRowDependentOverloads: hasRowDependentOverloads,
		Definition:               def,
	}
}

// funDefs holds pre-allocated FunctionDefinition instances
// for every builtin function. Initialized by setupBuiltins().
var funDefs map[string]*FunctionDefinition

// Format implements the NodeFormatter interface.
func (fd *FunctionDefinition) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString(fd.Name)
}

func (fd *FunctionDefinition) String() string { return AsString(fd) }

// SearchPath represents a list of namespaces to search builtins in.
// The names must be normalized (as per Name.Normalize) already.
type SearchPath []string

// ResolveFunction transforms an UnresolvedName to a FunctionDefinition.
func (n UnresolvedName) ResolveFunction(searchPath SearchPath) (*FunctionDefinition, error) {
	fn, err := n.normalizeFunctionName()
	if err != nil {
		return nil, err
	}

	if len(fn.selector) > 0 {
		// We do not support selectors at this point.
		return nil, fmt.Errorf("invalid function name: %s", n)
	}

	if d, ok := funDefs[fn.function()]; ok && fn.prefix() == "" {
		// Fast path: return early.
		return d, nil
	}

	// Although the conversion from Name to string should go via
	// Name.Normalize(), functions are special in that they are
	// guaranteed to not contain special Unicode characters. So we can
	// use ToLower directly.
	prefix := strings.ToLower(fn.prefix())
	smallName := strings.ToLower(fn.function())
	fullName := smallName
	if prefix != "" {
		fullName = prefix + "." + smallName
	}
	def, ok := funDefs[fullName]
	if !ok {
		found := false
		if prefix == "" {
			// The function wasn't qualified, so we must search for it via
			// the search path first.
			for _, alt := range searchPath {
				fullName = alt + "." + smallName
				if def, ok = funDefs[fullName]; ok {
					found = true
					break
				}
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown function: %s()", n)
		}
	}

	return def, nil
}
