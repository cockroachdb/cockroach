// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
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

// GetBuiltinFunctionProperties returns the FunctionProperties common to all
// overloads of the builtin function with the given name. It returns nil if no
// such builtin function was found.
//
// Callers that need access to builtin function properties at init-time can use
// this function to ensure the builtin function definitions have been
// initialized first.
func GetBuiltinFunctionProperties(name string) *tree.FunctionProperties {
	def, ok := tree.FunDefs[name]
	if !ok {
		return nil
	}
	return &def.FunctionProperties
}

func init() {
	tree.FunDefs = make(map[string]*tree.FunctionDefinition)
	tree.ResolvedBuiltinFuncDefs = make(map[string]*tree.ResolvedFunctionDefinition)
	tree.OidToQualifiedBuiltinOverload = make(map[oid.Oid]tree.QualifiedOverload)
	tree.OidToBuiltinName = make(map[oid.Oid]string)

	builtinsregistry.AddSubscription(func(name string, props *tree.FunctionProperties, overloads []tree.Overload) {
		for i, fn := range overloads {
			signature := name + fn.Signature(true)
			overloads[i].Oid = signatureMustHaveHardcodedOID(signature)
			tree.OidToBuiltinName[overloads[i].Oid] = name
			if _, ok := CastBuiltinNames[name]; ok {
				retOid := fn.ReturnType(nil).Oid()
				if _, ok := CastBuiltinOIDs[retOid]; !ok {
					CastBuiltinOIDs[retOid] = make(map[types.Family]oid.Oid, len(overloads))
				}
				CastBuiltinOIDs[retOid][fn.Types.GetAt(0).Family()] = overloads[i].Oid
			}
		}
		fDef := tree.NewFunctionDefinition(name, props, overloads)
		addResolvedFuncDef(tree.ResolvedBuiltinFuncDefs, tree.OidToQualifiedBuiltinOverload, fDef)
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
	resolved map[string]*tree.ResolvedFunctionDefinition,
	oidToOl map[oid.Oid]tree.QualifiedOverload,
	def *tree.FunctionDefinition,
) {
	parts := strings.Split(def.Name, ".")
	if len(parts) > 2 || len(parts) == 0 {
		// This shouldn't happen in theory.
		panic(errors.AssertionFailedf("invalid builtin function name: %s", def.Name))
	}

	var fd *tree.ResolvedFunctionDefinition
	if len(parts) == 2 {
		fd = tree.QualifyBuiltinFunctionDefinition(def, parts[0])
		resolved[def.Name] = fd
		return
	} else {
		resolvedName := catconstants.PgCatalogName + "." + def.Name
		fd = tree.QualifyBuiltinFunctionDefinition(def, catconstants.PgCatalogName)
		resolved[resolvedName] = fd
		if def.AvailableOnPublicSchema {
			resolvedName = catconstants.PublicSchemaName + "." + def.Name
			resolved[resolvedName] = tree.QualifyBuiltinFunctionDefinition(def, catconstants.PublicSchemaName)
		}
	}
	for _, o := range fd.Overloads {
		oidToOl[o.Oid] = o
	}
}

// registerBuiltin adds the given builtin to the builtins registry. All
// overloads of the Generator class are updated to have Fn and FnWithExprs
// fields to be functions that return assertion errors upon execution (to
// prevent misuse).
//
// If enforceClass is true, then it panics if at least one overload is not of
// the expected class.
//
// Note that additional sanity checks are also performed in eval/overload.go.
func registerBuiltin(
	name string, def builtinDefinition, expectedClass tree.FunctionClass, enforceClass bool,
) {
	for i := range def.overloads {
		overload := &def.overloads[i]
		if enforceClass {
			if overload.Class != expectedClass {
				panic(errors.AssertionFailedf("%s: expected to be marked with class %q, found %q",
					name, expectedClass, overload.Class))
			}
		}
		if overload.Class == tree.GeneratorClass {
			overload.Fn = unsuitableUseOfGeneratorFn
			overload.FnWithExprs = unsuitableUseOfGeneratorFnWithExprs
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
