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

// FunctionDefinition implements a reference to the (possibly several)
// overloads for a built-in function.
type FunctionDefinition struct {
	// Name is the short name of the function.
	Name string
	// HasOverloadsNeedingRepeatedEvaluation is true if one or more of
	// the overload definitions has needsRepeatedEvaluation set.
	// Set e.g. for aggregate functions.
	HasOverloadsNeedingRepeatedEvaluation bool
	// Definition is the set of overloads for this function name.
	Definition []overloadImpl
}

// NewFunctionDefinition allocates a function definition corresponding
// to the given built-in definition.
func NewFunctionDefinition(name string, def []Builtin) *FunctionDefinition {
	hasRowDependentOverloads := false
	overloads := make([]overloadImpl, len(def))
	for i := range def {
		overloads[i] = &def[i]
		if def[i].NeedsRepeatedEvaluation {
			hasRowDependentOverloads = true
		}
	}
	return &FunctionDefinition{
		Name: name,
		HasOverloadsNeedingRepeatedEvaluation: hasRowDependentOverloads,
		Definition:                            overloads,
	}
}

// FunDefs holds pre-allocated FunctionDefinition instances
// for every builtin function. Initialized by builtins.init().
var FunDefs map[string]*FunctionDefinition

// Format implements the NodeFormatter interface.
func (fd *FunctionDefinition) Format(ctx *FmtCtx) {
	ctx.WriteString(fd.Name)
}
func (fd *FunctionDefinition) String() string { return AsString(fd) }
