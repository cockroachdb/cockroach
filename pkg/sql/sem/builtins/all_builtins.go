// Copyright 2017 The Cockroach Authors.
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

package builtins

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// AllBuiltinNames is an array containing all the built-in function
// names, sorted in alphabetical order. This can be used for a
// deterministic walk through the Builtins map.
var AllBuiltinNames []string

func init() {
	initAggregateBuiltins()
	initWindowBuiltins()
	initGeneratorBuiltins()
	initPGBuiltins()

	AllBuiltinNames = make([]string, 0, len(Builtins))
	tree.FunDefs = make(map[string]*tree.FunctionDefinition)
	for name, def := range Builtins {
		fDef := tree.NewFunctionDefinition(name, &def.FunctionProperties, def.Overloads)
		tree.FunDefs[name] = fDef
		if fDef.Private {
			// Avoid listing help for private functions.
			continue
		}
		AllBuiltinNames = append(AllBuiltinNames, name)
	}

	// Generate missing categories.
	for _, name := range AllBuiltinNames {
		def := Builtins[name]
		if def.Category == "" {
			def.Category = getCategory(def.Overloads)
			Builtins[name] = def
		}
	}

	sort.Strings(AllBuiltinNames)
}

func getCategory(b []tree.OverloadDefinition) string {
	// If single argument attempt to categorize by the type of the argument.
	for _, ovl := range b {
		switch typ := ovl.Types.(type) {
		case tree.ArgTypes:
			if len(typ) == 1 {
				return categorizeType(typ[0].Typ)
			}
		}
		// Fall back to categorizing by return type.
		if retType := ovl.FixedReturnType(); retType != nil {
			return categorizeType(retType)
		}
	}
	return ""
}

func collectOverloads(
	props tree.FunctionProperties, types []types.T, gens ...func(types.T) tree.OverloadDefinition,
) BuiltinDefinition {
	r := make([]tree.OverloadDefinition, 0, len(types)*len(gens))
	for _, f := range gens {
		for _, t := range types {
			r = append(r, f(t))
		}
	}
	return BuiltinDefinition{
		FunctionProperties: props,
		Overloads:          r,
	}
}
