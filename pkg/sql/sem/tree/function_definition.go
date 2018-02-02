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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

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
	for i, d := range def {
		overloads[i] = d
		if d.NeedsRepeatedEvaluation {
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

// ResolveFunction transforms an UnresolvedName to a FunctionDefinition.
func (n *UnresolvedName) ResolveFunction(
	searchPath sessiondata.SearchPath,
) (*FunctionDefinition, error) {
	if n.NumParts > 2 || len(n.Parts[0]) == 0 {
		// n.NumParts > 2 would be catalog.schema.funname().
		// We don't support those. (yet?)
		return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError,
			"invalid function name: %q", ErrString(n))
	}
	function, prefix := n.Parts[0], n.Parts[1]

	if d, ok := FunDefs[function]; ok && prefix == "" {
		// Fast path: return early.
		return d, nil
	}

	// Although the conversion from Name to string should go via
	// Name.Normalize(), functions are special in that they are
	// guaranteed to not contain special Unicode characters. So we can
	// use ToLower directly.
	// TODO(knz): this will need to be revisited once we allow
	// function names to exist in custom namespaces, whose names
	// may contain special characters.
	prefix = strings.ToLower(prefix)
	smallName := strings.ToLower(function)
	fullName := smallName

	if prefix == "pg_catalog" {
		// If the user specified e.g. `pg_catalog.max()` we want to find
		// it in the global namespace.
		prefix = ""
	}

	if prefix != "" {
		fullName = prefix + "." + smallName
	}
	def, ok := FunDefs[fullName]
	if !ok {
		found := false
		if prefix == "" {
			// The function wasn't qualified, so we must search for it via
			// the search path first.
			iter := searchPath.Iter()
			for alt, ok := iter(); ok; alt, ok = iter() {
				fullName = alt + "." + smallName
				if def, ok = FunDefs[fullName]; ok {
					found = true
					break
				}
			}
		}
		if !found {
			return nil, pgerror.NewErrorf(
				pgerror.CodeUndefinedFunctionError, "unknown function: %s()", ErrString(n))
		}
	}

	return def, nil
}
