// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"
)

func valueForYAML(v interface{}) interface{} {
	switch v := v.(type) {
	case fmt.Stringer:
		return v.String()
	default:
		return v
	}
}

func (v valueExpr) encoded() interface{} {
	return valueForYAML(v.value)
}

func (v notValueExpr) encoded() interface{} {
	return valueForYAML(v.value)
}

func (c containsExpr) encoded() interface{} {
	return c.v.encoded()
}

func (a anyExpr) encoded() interface{} {
	ret := make([]interface{}, 0, len(a))
	for _, v := range a {
		ret = append(ret, valueForYAML(v))
	}
	return ret
}

func (v Var) encoded() interface{} {
	return "$" + string(v)
}

func (e eqDecl) MarshalYAML() (interface{}, error) {
	return clauseStr("$"+string(e.v), e.expr)
}

func exprToString(e expr) (string, error) {
	var expr yaml.Node
	if err := expr.Encode(e.encoded()); err != nil {
		return "", err
	}
	expr.Style = yaml.FlowStyle
	out, err := yaml.Marshal(&expr)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), err
}

func (f tripleDecl) MarshalYAML() (interface{}, error) {
	return clauseStr(fmt.Sprintf("$%s[%s]", f.entity, f.attribute), f.value)
}

func (r ruleInvocation) MarshalYAML() (interface{}, error) {
	return ruleInvocationStr(r.rule.Name, r.args), nil
}

func ruleInvocationStr(name string, args []Var) string {
	var buf strings.Builder
	buf.WriteString(name)
	writeArgsList(&buf, args)
	return buf.String()
}

// MarshalYAML marshals a rule to YAML.
func (r RuleDef) MarshalYAML() (interface{}, error) {
	var cl yaml.Node
	if err := cl.Encode(r.Clauses()); err != nil {
		return nil, err
	}
	content := &cl
	if r.isNotJoin {
		content = &yaml.Node{
			Kind: yaml.MappingNode,
			Content: []*yaml.Node{
				{Kind: yaml.ScalarNode, Value: "not-join"},
				&cl,
			},
		}
	}
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: ruleInvocationStr(r.Name, r.Params())},
			content,
		},
	}, nil
}

func clauseStr(lhs string, rhs expr) (string, error) {
	rhsStr, err := exprToString(rhs)
	if err != nil {
		return "", err
	}
	var op string
	switch rhs.(type) {
	case valueExpr, Var:
		op = "="
	case anyExpr:
		op = "IN"
	case notValueExpr:
		op = "!="
	case containsExpr:
		op = "CONTAINS"
	default:
		return "", errors.AssertionFailedf("unknown expression type %T", rhs)
	}
	return fmt.Sprintf("%s %s %s", lhs, op, rhsStr), nil
}

func (f filterDecl) MarshalYAML() (interface{}, error) {
	var buf strings.Builder
	buf.WriteString(f.name)
	buf.WriteString("(")
	ft := reflect.TypeOf(f.predicateFunc)
	for i := 0; i < ft.NumIn(); i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(ft.In(i).String())
	}
	buf.WriteString(")")
	writeArgsList(&buf, f.vars)
	return buf.String(), nil
}

func writeArgsList(buf *strings.Builder, vars []Var) {
	buf.WriteString("(")
	for i, v := range vars {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("$")
		buf.WriteString(string(v))
	}
	buf.WriteString(")")
}
