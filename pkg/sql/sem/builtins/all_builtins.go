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

var allBuiltinNames stringSet

// AllBuiltinNames returns a slice containing all the built-in function
// names, sorted in alphabetical order. This can be used for a
// deterministic walk through the Builtins map.
func AllBuiltinNames() []string {
	return allBuiltinNames.Ordered()
}

var allAggregateBuiltinNames stringSet

// AllAggregateBuiltinNames returns a slice containing the subset of
// AllBuiltinNames that corresponds to aggregate functions.
func AllAggregateBuiltinNames() []string {
	return allAggregateBuiltinNames.Ordered()
}

var allWindowBuiltinNames stringSet

// AllWindowBuiltinNames returns a slice containing the subset of
// AllBuiltinNames that corresponds to window functions.
func AllWindowBuiltinNames() []string {
	return allWindowBuiltinNames.Ordered()
}

func init() {
	tree.FunDefs = make(map[string]*tree.FunctionDefinition)
	tree.ResolvedBuiltinFuncDefs = make(map[string]*tree.ResolvedFunctionDefinition)

	builtinsregistry.AddSubscription(func(name string, props *tree.FunctionProperties, overloads []tree.Overload) {
		for i, fn := range overloads {
			signature := name + fn.Signature(true)
			overloads[i].Oid = signatureMustHaveHardcodedOID(signature)
		}
		fDef := tree.NewFunctionDefinition(name, props, overloads)
		addResolvedFuncDef(tree.ResolvedBuiltinFuncDefs, fDef)
		tree.FunDefs[name] = fDef
		if !fDef.ShouldDocument() {
			// Avoid listing help for undocumented functions.
			return
		}
		allBuiltinNames.Add(name)
		for _, fn := range overloads {
			if fn.Class == tree.AggregateClass {
				allAggregateBuiltinNames.Add(name)
			} else if fn.Class == tree.WindowClass {
				allWindowBuiltinNames.Add(name)
			}
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

	resolvedName := catconstants.PgCatalogName + "." + def.Name
	resolved[resolvedName] = tree.QualifyBuiltinFunctionDefinition(def, catconstants.PgCatalogName)
	if def.AvailableOnPublicSchema {
		resolvedName = catconstants.PublicSchemaName + "." + def.Name
		resolved[resolvedName] = tree.QualifyBuiltinFunctionDefinition(def, catconstants.PublicSchemaName)
	}
}

func registerBuiltin(name string, def builtinDefinition) {
	for i := range def.overloads {
		overload := &def.overloads[i]
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
	if def.props.ShouldDocument() && def.props.Category == "" {
		def.props.Category = getCategory(def.overloads)
	}
	builtinsregistry.Register(name, &def.props, def.overloads)
}

func getCategory(b []tree.Overload) string {
	// If single argument attempt to categorize by the type of the argument.
	for _, ovl := range b {
		switch typ := ovl.Types.(type) {
		case tree.ParamTypes:
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

// stringSet is a set of strings that can be ordered.
type stringSet struct {
	set     map[string]struct{}
	ordered []string
}

// Add adds a string to the set.
func (s *stringSet) Add(str string) {
	if s.set == nil {
		s.set = make(map[string]struct{})
	}
	s.set[str] = struct{}{}
	s.ordered = nil
}

// Ordered returns an ordered slice of the strings in the set.
func (s stringSet) Ordered() []string {
	if s.ordered == nil {
		s.ordered = make([]string, 0, len(s.set))
		for str := range s.set {
			s.ordered = append(s.ordered, str)
		}
		sort.Strings(s.ordered)
	}
	return s.ordered
}
