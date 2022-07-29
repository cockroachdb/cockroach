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
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
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
	// Note: changing the order of these init functions will cause changes to OIDs
	// of builtin functions. In general, it won't cause internal problems other
	// than causing failures in tests which make assumption of OIDs.
	initRegularBuiltins()
	initAggregateBuiltins()
	initWindowBuiltins()
	initGeneratorBuiltins()
	initGeoBuiltins()
	initTrigramBuiltins()
	initPGBuiltins()
	initMathBuiltins()
	initOverlapsBuiltins()
	initReplicationBuiltins()
	initPgcryptoBuiltins()
	initProbeRangesBuiltins()

	tree.FunDefs = make(map[string]*tree.FunctionDefinition)
	builtinsregistry.Iterate(func(name string, props *tree.FunctionProperties, overloads []tree.Overload) {
		fDef := tree.NewFunctionDefinition(name, props, overloads)
		tree.FunDefs[name] = fDef
		if !fDef.ShouldDocument() {
			// Avoid listing help for undocumented functions.
			return
		}
		AllBuiltinNames = append(AllBuiltinNames, name)
		if props.Class == tree.AggregateClass {
			AllAggregateBuiltinNames = append(AllAggregateBuiltinNames, name)
		} else if props.Class == tree.WindowClass {
			AllWindowBuiltinNames = append(AllWindowBuiltinNames, name)
		}
	})

	sort.Strings(AllBuiltinNames)
	sort.Strings(AllAggregateBuiltinNames)
	sort.Strings(AllWindowBuiltinNames)
}

func registerBuiltin(name string, def builtinDefinition) {
	for i, overload := range def.overloads {
		fnCount := 0
		if overload.Fn != nil {
			fnCount++
		}
		if overload.FnWithExprs != nil {
			fnCount++
		}
		if overload.Generator != nil {
			overload.Fn = unsuitableUseOfGeneratorFn
			overload.FnWithExprs = unsuitableUseOfGeneratorFnWithExprs
			fnCount++
		}
		if overload.GeneratorWithExprs != nil {
			overload.Fn = unsuitableUseOfGeneratorFn
			overload.FnWithExprs = unsuitableUseOfGeneratorFnWithExprs
			fnCount++
		}
		if fnCount > 1 {
			panic(fmt.Sprintf(
				"builtin %s: at most 1 of Fn, FnWithExprs, Generator, and GeneratorWithExprs"+
					"must be set on overloads; (found %d)",
				name, fnCount,
			))
		}
		c := sqltelemetry.BuiltinCounter(name, overload.Signature(false))
		def.overloads[i].OnTypeCheck = func() {
			telemetry.Inc(c)
		}
	}
	if def.props.ShouldDocument() && def.props.Category == "" {
		def.props.Category = getCategory(def.overloads)
	}
	builtinsregistry.Register(name, &def.props, def.overloads)
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
	return makeBuiltin(props, r...)
}
