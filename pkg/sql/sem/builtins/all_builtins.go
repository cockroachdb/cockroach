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
		tree.FunDefs[name] = tree.NewFunctionDefinition(name, def)
		AllBuiltinNames = append(AllBuiltinNames, name)
	}

	// Generate missing categories.
	for _, name := range AllBuiltinNames {
		def := Builtins[name]
		for i := range def {
			if def[i].Category == "" {
				def[i].Category = getCategory(&def[i])
			}
		}
	}

	sort.Strings(AllBuiltinNames)
}

func getCategory(b *tree.Builtin) string {
	// If single argument attempt to categorize by the type of the argument.
	switch typ := b.Types.(type) {
	case tree.ArgTypes:
		if len(typ) == 1 {
			return categorizeType(typ[0].Typ)
		}
	}
	// Fall back to categorizing by return type.
	if retType := b.FixedReturnType(); retType != nil {
		return categorizeType(retType)
	}
	return ""
}

func collectBuiltins(f func(types.T) tree.Builtin, types ...types.T) []tree.Builtin {
	r := make([]tree.Builtin, len(types))
	for i := range types {
		r[i] = f(types[i])
	}
	return r
}
