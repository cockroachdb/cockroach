// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"
)

// Clauses exists to handle flattening of a slice of clauses before marshaling.
type Clauses []Clause

func replaceVars(replacements, before []Var, cl Clause) Clause {
	maybeReplace := func(in Var) (_ Var, replaced bool) {
		for i, v := range before {
			if in == v && replacements[i] != v {
				return replacements[i], true
			}
		}
		return in, false
	}
	replaced, _ := walkVars(cl, maybeReplace)
	return replaced
}

func expanded(c Clauses) Clauses {
	needsExpansion := func() bool {
		for _, cl := range c {
			switch cl := cl.(type) {
			case and:
				return true
			case ruleInvocation:
				if !cl.rule.isNotJoin {
					return true
				}
			}
		}
		return false
	}

	if !needsExpansion() {
		return c
	}

	var ret Clauses
	for _, cl := range c {
		switch cl := cl.(type) {
		case and:
			ret = append(ret, expanded(Clauses(cl))...)
		case ruleInvocation:
			if cl.rule.isNotJoin {
				ret = append(ret, cl)
				continue
			}
			for _, clause := range expanded(cl.rule.clauses) {
				ret = append(ret, replaceVars(cl.args, cl.rule.paramVars, clause))
			}
		default:
			ret = append(ret, cl)
		}
	}
	return ret
}

func flattened(c Clauses) Clauses {
	needsFlatten := func() bool {
		for _, cl := range c {
			if _, isAnd := cl.(and); isAnd {
				return true
			}
		}
		return false
	}
	if !needsFlatten() {
		return c
	}
	var ret Clauses
	for _, cl := range c {
		if andCl, ok := cl.(and); ok {
			ret = append(ret, flattened(Clauses(andCl))...)
		} else {
			ret = append(ret, cl)
		}
	}
	return ret
}

// MarshalYAML marshals clauses to yaml.
func (c Clauses) MarshalYAML() (interface{}, error) {
	fc := flattened(c)
	var n yaml.Node
	if err := n.Encode([]Clause(fc)); err != nil {
		return nil, err
	}
	n.Style = yaml.LiteralStyle
	return &n, nil
}

type replaceVarFunc = func(Var) (_ Var, replaced bool)

func walkVars(cl Clause, f replaceVarFunc) (_ Clause, replaced bool) {
	switch cl := cl.(type) {
	case and:
		for i := range cl {
			child, ok := walkVars(cl[i], f)
			if ok {
				cl[i], replaced = child, true
			}
		}
		if replaced {
			return cl, true
		}
	case eqDecl:
		if v, ok := f(cl.v); ok {
			cl.v, replaced = v, true
		}
		if varExpr, isVar := cl.expr.(Var); isVar {
			if v, ok := f(varExpr); ok {
				cl.expr, replaced = v, true
			}
		}
		if replaced {
			return cl, true
		}
	case filterDecl:
		for i := range cl.vars {
			if v, ok := f(cl.vars[i]); ok {
				cl.vars[i], replaced = v, true
			}
		}
		if replaced {
			return cl, true
		}
	case ruleInvocation:
		for i := range cl.args {
			if v, ok := f(cl.args[i]); ok {
				cl.args[i], replaced = v, true
			}
		}
		if replaced {
			return cl, true
		}
	case tripleDecl:
		if v, ok := f(cl.entity); ok {
			cl.entity, replaced = v, true
		}
		if v, isVar := cl.value.(Var); isVar {
			if newV, ok := f(v); ok {
				cl.value, replaced = newV, true
			}
		}
		if replaced {
			return cl, true
		}
	default:
		panic(errors.AssertionFailedf("unhandled clause type %T", cl))
	}
	return cl, false
}
