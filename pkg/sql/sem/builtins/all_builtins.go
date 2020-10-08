// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// AllBuiltinNames is an array containing all the built-in function
// names, sorted in alphabetical order. This can be used for a
// deterministic walk through the Builtins map.
var AllBuiltinNames []string

// AllAggregateBuiltinNames is an array containing the subset of
// AllBuiltinNames that corresponds to aggregate functions.
var AllAggregateBuiltinNames []string

// AllWindowBuiltinNames is an array containing the subset of
// AllBuiltinNames that corresponds to window functions.
var AllWindowBuiltinNames []string

func init() {
	initAggregateBuiltins()
	initWindowBuiltins()
	initGeneratorBuiltins()
	initGeoBuiltins()
	initPGBuiltins()
	initMathBuiltins()

	AllBuiltinNames = make([]string, 0, len(builtins))
	AllAggregateBuiltinNames = make([]string, 0, len(aggregates))
	tree.FunDefs = make(map[string]*tree.FunctionDefinition)
	for name, def := range builtins {
		fDef := tree.NewFunctionDefinition(name, &def.props, def.overloads)
		tree.FunDefs[name] = fDef
		if !fDef.ShouldDocument() {
			// Avoid listing help for undocumented functions.
			continue
		}
		AllBuiltinNames = append(AllBuiltinNames, name)
		if def.props.Class == tree.AggregateClass {
			AllAggregateBuiltinNames = append(AllAggregateBuiltinNames, name)
		} else if def.props.Class == tree.WindowClass {
			AllWindowBuiltinNames = append(AllWindowBuiltinNames, name)
		}
	}

	// Generate missing categories.
	for _, name := range AllBuiltinNames {
		def := builtins[name]
		if def.props.Category == "" {
			def.props.Category = getCategory(def.overloads)
			builtins[name] = def
		}
	}

	sort.Strings(AllBuiltinNames)
	sort.Strings(AllAggregateBuiltinNames)
	sort.Strings(AllWindowBuiltinNames)
}

func getCategory(b []tree.Overload) string {
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
	props tree.FunctionProperties, types []*types.T, gens ...func(*types.T) tree.Overload,
) builtinDefinition {
	r := make([]tree.Overload, 0, len(types)*len(gens))
	for _, f := range gens {
		for _, t := range types {
			r = append(r, f(t))
		}
	}
	return builtinDefinition{
		props:     props,
		overloads: r,
	}
}
