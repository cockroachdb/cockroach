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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

var allBuiltinNames orderedStrings

// AllBuiltinNames returns a slice containing all the built-in function
// names, sorted in alphabetical order. This can be used for a
// deterministic walk through the Builtins map.
func AllBuiltinNames() []string {
	allBuiltinNames.sort()
	return allBuiltinNames.strings
}

var allAggregateBuiltinNames orderedStrings

// AllAggregateBuiltinNames returns a slice containing the subset of
// AllBuiltinNames that corresponds to aggregate functions.
func AllAggregateBuiltinNames() []string {
	allAggregateBuiltinNames.sort()
	return allAggregateBuiltinNames.strings
}

var allWindowBuiltinNames orderedStrings

// AllWindowBuiltinNames returns a slice containing the subset of
// AllBuiltinNames that corresponds to window functions.
func AllWindowBuiltinNames() []string {
	allWindowBuiltinNames.sort()
	return allWindowBuiltinNames.strings
}

func init() {
	tree.FunDefs = make(map[string]*tree.FunctionDefinition)
	tree.ResolvedBuiltinFuncDefs = make(map[string]*tree.ResolvedFunctionDefinition)

	builtinsregistry.AddSubscription(func(name string, overloads []tree.Overload) {
		for i, fn := range overloads {
			signature := name + fn.Signature(true)
			overloads[i].Oid = signatureMustHaveHardcodedOID(signature)
		}
		fDef := tree.NewFunctionDefinition(name, overloads)
		addResolvedFuncDef(tree.ResolvedBuiltinFuncDefs, fDef)
		tree.FunDefs[name] = fDef
		addNameToAllBuiltinNames := false
		addNameToAggregateBuiltinNames := false
		addNameToWindowBuiltinNames := false
		for i := range overloads {
			o := &overloads[i]
			if !o.ShouldDocument() {
				// Avoid listing help for undocumented functions.
				continue
			}
			addNameToAllBuiltinNames = true
			if o.Class == tree.AggregateClass {
				addNameToAggregateBuiltinNames = true
			} else if o.Class == tree.WindowClass {
				addNameToWindowBuiltinNames = true
			}
		}
		if addNameToAllBuiltinNames {
			allBuiltinNames.add(name)
		}
		if addNameToAggregateBuiltinNames {
			allAggregateBuiltinNames.add(name)
		}
		if addNameToWindowBuiltinNames {
			allWindowBuiltinNames.add(name)
		}
	})
}

func addResolvedFuncDef(
	resolved map[string]*tree.ResolvedFunctionDefinition, def *tree.FunctionDefinition,
) {
	parts := strings.Split(def.Name, ".")
	if len(parts) > 2 || len(parts) == 0 {
		// This shouldn't happen in theory.
		panic(errors.AssertionFailedf("invalid builtin function name: %s", def.Name))
	}

	if len(parts) == 2 {
		resolved[def.Name] = tree.QualifyBuiltinFunctionDefinition(def, parts[0])
		return
	}

	// TODO(mgartner): If one overload is available on the public schema, then
	// all overloads with the same name are. They should be on the public schema
	// on per-overload basis.
	availableOnPublicSchema := false
	for i := range def.Definition {
		if def.Definition[i].AvailableOnPublicSchema {
			availableOnPublicSchema = true
			break
		}
	}

	resolvedName := catconstants.PgCatalogName + "." + def.Name
	resolved[resolvedName] = tree.QualifyBuiltinFunctionDefinition(def, catconstants.PgCatalogName)
	if availableOnPublicSchema {
		resolvedName = catconstants.PublicSchemaName + "." + def.Name
		resolved[resolvedName] = tree.QualifyBuiltinFunctionDefinition(def, catconstants.PublicSchemaName)
	}
}

func registerBuiltin(name string, def builtinDefinition) {
	for _, overload := range def.overloads {
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
	}
	builtinsregistry.Register(name, def.overloads)
}

func collectOverloads(types []*types.T, gens ...func(*types.T) tree.Overload) builtinDefinition {
	r := make([]tree.Overload, 0, len(types)*len(gens))
	for _, f := range gens {
		for _, t := range types {
			r = append(r, f(t))
		}
	}
	return makeBuiltin(r...)
}

// orderedStrings sorts a slice of strings lazily
// for better performance.
type orderedStrings struct {
	strings []string
	sorted  bool
}

// add a string without changing whether or not
// the strings are sorted yet.
func (o *orderedStrings) add(s string) {
	if o.sorted {
		o.insert(s)
	} else {
		o.strings = append(o.strings, s)
	}
}

func (o *orderedStrings) sort() {
	if !o.sorted {
		sort.Strings(o.strings)
	}
	o.sorted = true
}

// insert assumes the strings are already sorted
// and inserts s in the right place.
func (o *orderedStrings) insert(s string) {
	i := sort.SearchStrings(o.strings, s)
	o.strings = append(o.strings, "")
	copy(o.strings[i+1:], o.strings[i:])
	o.strings[i] = s
}
