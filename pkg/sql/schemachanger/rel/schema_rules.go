// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel

import (
	"reflect"
	"sort"

	"github.com/cockroachdb/errors"
)

// RuleDef describes a rule.
type RuleDef struct {
	Name    string
	Params  []Var
	Clauses Clauses
	Func    interface{} `yaml:"-"`

	sc *Schema
}

// ForEachRule iterates the schema's rules.
func (sc *Schema) ForEachRule(f func(def RuleDef)) {
	for _, r := range sc.rules {
		f(*r)
	}
}

type (
	// Rule1 is a rule with one variable.
	Rule1 = func(a Var) Clause
	// Rule2 is a rule with two variables.
	Rule2 = func(a, b Var) Clause
	// Rule3 is a rule with three variables.
	Rule3 = func(a, b, c Var) Clause
	// Rule4 is a rule with four variables.
	Rule4 = func(a, b, c, d Var) Clause
	// Rule5 is a rule with five variables.
	Rule5 = func(a, b, c, d, e Var) Clause
	// Rule6 is a rule with six variables.
	Rule6 = func(a, b, c, d, e, f Var) Clause
)

// Def1 defines a Rule1.
func (sc *Schema) Def1(name string, a Var, def func(a Var) Clauses) Rule1 {
	return sc.rule(name, def, a).(Rule1)
}

// Def2 defines a Rule2.
func (sc *Schema) Def2(name string, a, b Var, def func(a, b Var) Clauses) Rule2 {
	return sc.rule(name, def, a, b).(Rule2)
}

// Def3 defines a Rule3.
func (sc *Schema) Def3(name string, a, b, c Var, def func(a, b, c Var) Clauses) Rule3 {
	return sc.rule(name, def, a, b, c).(Rule3)
}

// Def4 defines a Rule4.
func (sc *Schema) Def4(name string, a, b, c, d Var, def func(a, b, c, d Var) Clauses) Rule4 {
	return sc.rule(name, def, a, b, c, d).(Rule4)
}

// Def5 defines a Rule5.
func (sc *Schema) Def5(name string, a, b, c, d, e Var, def func(a, b, c, d, e Var) Clauses) Rule5 {
	return sc.rule(name, def, a, b, c, d, e).(Rule5)
}

// Def6 defines a Rule6.
func (sc *Schema) Def6(
	name string, a, b, c, d, e, f Var, def func(a, b, c, d, e, f Var) Clauses,
) Rule6 {
	return sc.rule(name, def, a, b, c, d, e, f).(Rule6)
}

var (
	varType     = reflect.TypeOf(Var(""))
	clauseType  = reflect.TypeOf((*Clause)(nil)).Elem()
	clausesType = reflect.TypeOf((*Clauses)(nil)).Elem()
)

// rule is used to define a pattern of clauses for reuse.
func (sc *Schema) rule(name string, inFunc interface{}, vars ...Var) interface{} {
	if _, exists := sc.rulesByName[name]; exists {
		panic(errors.AssertionFailedf("already registered rule with name %s", name))
	}
	inT, clauses := buildRuleClauses(vars, inFunc)
	clauses = flattened(clauses)
	validateRuleClauses(name, clauses, vars)
	rd := &RuleDef{
		Name:    name,
		Params:  vars,
		Clauses: clauses,
		sc:      sc,
	}
	rd.Func = makeRuleFunc(inT, rd)
	sc.rules = append(sc.rules, rd)
	sc.rulesByName[name] = rd
	return rd.Func
}

func buildRuleClauses(vars []Var, f interface{}) ([]reflect.Type, Clauses) {
	inT, in := makeInTypesAndValues(vars)
	fv := validateBuildRuleFunctionValue(inT, f)
	clausesI := fv.Call(in)
	clauses := clausesI[0].Interface().(Clauses)
	return inT, clauses
}

func makeInTypesAndValues(vars []Var) ([]reflect.Type, []reflect.Value) {
	inT := make([]reflect.Type, len(vars))
	in := make([]reflect.Value, len(vars))
	for i, v := range vars {
		inT[i] = varType
		if v == Blank {
			panic(errors.AssertionFailedf("cannot use _ as a parameter"))
		}
		in[i] = reflect.ValueOf(v)
	}
	return inT, in
}

func validateBuildRuleFunctionValue(inT []reflect.Type, f interface{}) reflect.Value {
	inFuncType := reflect.FuncOf(
		inT, []reflect.Type{clausesType}, false, /* variadic */
	)
	fv := reflect.ValueOf(f)
	if !fv.IsValid() {
		panic(errors.AssertionFailedf("input function must not be nil"))
	}
	fvt := fv.Type()
	if fvt != inFuncType {
		panic(errors.AssertionFailedf("input function must be %v, got %v",
			inFuncType, fvt))
	}
	return fv
}

func validateRuleClauses(name string, clauses Clauses, vars []Var) {
	vs := varsUsedInClauses(clauses)
	if missing := vs.removed(vars...); len(missing) != 0 {
		panic(errors.Errorf(
			"invalid rule %s: %v are not defined variables", name, missing.ordered(),
		))
	}
	if unused := makeVarSet(vars...).removed(vs.ordered()...); len(unused) > 0 {
		panic(errors.Errorf(
			"invalid rule %s: %v input variable are not used", name, unused.ordered(),
		))
	}
}

func makeRuleFunc(inT []reflect.Type, rd *RuleDef) interface{} {
	return reflect.MakeFunc(reflect.FuncOf(
		inT, []reflect.Type{clauseType}, false, /* variadic */
	), func(args []reflect.Value) []reflect.Value {
		argVars := make([]Var, len(inT))
		for i, v := range args {
			argVars[i] = v.Interface().(Var)
		}
		return []reflect.Value{reflect.ValueOf(ruleInvocation{
			args: argVars,
			rule: rd,
		}).Convert(clauseType)}
	}).Interface()
}

func varsUsedInClauses(clauses Clauses) varSet {
	vs := varSet{}
	walkVars(and(clauses), func(v Var) (_ Var, replaced bool) {
		if v != Blank {
			vs.add(v)
		}
		return v, false
	})
	return vs
}

type varSet map[Var]struct{}

func (vs varSet) clone() varSet {
	clone := make(varSet, len(vs))
	for k := range vs {
		clone[k] = struct{}{}
	}
	return clone
}

func (vs varSet) removed(vars ...Var) varSet {
	clone := vs.clone()
	for _, v := range vars {
		delete(clone, v)
	}
	return clone
}

func makeVarSet(vars ...Var) varSet {
	vs := make(varSet, len(vars))
	vs.add(vars...)
	return vs
}

func (vs varSet) add(vars ...Var) {
	for _, v := range vars {
		vs[v] = struct{}{}
	}
}

func (vs varSet) ordered() []Var {
	ret := make([]Var, 0, len(vs))
	for s := range vs {
		ret = append(ret, s)
	}
	sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })
	return ret
}
