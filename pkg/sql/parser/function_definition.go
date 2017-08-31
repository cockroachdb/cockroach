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

package parser

import (
	"bytes"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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

func newFunctionDefinition(name string, def []Builtin) *FunctionDefinition {
	hasRowDependentOverloads := false
	overloads := make([]overloadImpl, len(def))
	for i, d := range def {
		overloads[i] = d
		if d.needsRepeatedEvaluation {
			hasRowDependentOverloads = true
		}
	}
	return &FunctionDefinition{
		Name: name,
		HasOverloadsNeedingRepeatedEvaluation: hasRowDependentOverloads,
		Definition:                            overloads,
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

// PgCatalogName is the name of the pg_catalog system database.
const PgCatalogName = "pg_catalog"

// SearchPath represents a list of namespaces to search builtins in.
// The names must be normalized (as per Name.Normalize) already.
type SearchPath struct {
	paths             []string
	containsPgCatalog bool
}

// MakeSearchPath returns a new SearchPath struct.
func MakeSearchPath(paths []string) SearchPath {
	containsPgCatalog := false
	for _, e := range paths {
		if e == PgCatalogName {
			containsPgCatalog = true
			break
		}
	}
	return SearchPath{
		paths:             paths,
		containsPgCatalog: containsPgCatalog,
	}
}

// Iter returns an iterator through the search path. We must include the
// implicit pg_catalog at the beginning of the search path, unless it has been
// explicitly set later by the user.
// "The system catalog schema, pg_catalog, is always searched, whether it is
// mentioned in the path or not. If it is mentioned in the path then it will be
// searched in the specified order. If pg_catalog is not in the path then it
// will be searched before searching any of the path items."
// - https://www.postgresql.org/docs/9.1/static/runtime-config-client.html
func (s SearchPath) Iter() func() (next string, ok bool) {
	i := -1
	if s.containsPgCatalog {
		i = 0
	}
	return func() (next string, ok bool) {
		if i == -1 {
			i++
			return PgCatalogName, true
		}
		if i < len(s.paths) {
			i++
			return s.paths[i-1], true
		}
		return "", false
	}
}

// IterWithoutImplicitPGCatalog is the same as Iter, but does not include the implicit pg_catalog.
func (s SearchPath) IterWithoutImplicitPGCatalog() func() (next string, ok bool) {
	i := 0
	return func() (next string, ok bool) {
		if i < len(s.paths) {
			i++
			return s.paths[i-1], true
		}
		return "", false
	}
}

func (s SearchPath) String() string {
	return strings.Join(s.paths, ", ")
}

// ResolveFunction transforms an UnresolvedName to a FunctionDefinition.
func (n UnresolvedName) ResolveFunction(searchPath SearchPath) (*FunctionDefinition, error) {
	fn, err := n.normalizeFunctionName()
	if err != nil {
		return nil, err
	}

	if len(fn.selector) > 0 {
		// We do not support selectors at this point.
		return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError, "invalid function name: %s", n)
	}

	if d, ok := funDefs[fn.function()]; ok && fn.prefix() == "" {
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
	prefix := strings.ToLower(fn.prefix())
	smallName := strings.ToLower(fn.function())
	fullName := smallName

	if prefix == "pg_catalog" {
		// If the user specified e.g. `pg_catalog.max()` we want to find
		// it in the global namespace.
		prefix = ""
	}

	if prefix != "" {
		fullName = prefix + "." + smallName
	}
	def, ok := funDefs[fullName]
	if !ok {
		found := false
		if prefix == "" {
			// The function wasn't qualified, so we must search for it via
			// the search path first.
			iter := searchPath.Iter()
			for alt, ok := iter(); ok; alt, ok = iter() {
				fullName = alt + "." + smallName
				if def, ok = funDefs[fullName]; ok {
					found = true
					break
				}
			}
		}
		if !found {
			return nil, pgerror.NewErrorf(
				pgerror.CodeUndefinedFunctionError, "unknown function: %s()", n)
		}
	}

	return def, nil
}
