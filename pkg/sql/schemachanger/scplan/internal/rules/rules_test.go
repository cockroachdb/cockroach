// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rules

import (
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"gopkg.in/yaml.v3"
)

// TestRulesYAML outputs the rules to yaml as a way to visualize changes.
// Rules are sorted by name to ensure stable output.
func TestRulesYAML(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "rules":
				var m yaml.Node
				m.Kind = yaml.MappingNode
				var s []rel.RuleDef
				screl.Schema.ForEachRule(func(def rel.RuleDef) {
					s = append(s, def)
				})
				sort.SliceStable(s, func(i, j int) bool {
					return s[i].Name < s[j].Name
				})
				for _, def := range s {
					var rule yaml.Node
					if err := rule.Encode(def); err != nil {
						panic(err)
					}
					m.Content = append(m.Content, rule.Content...)
				}
				out, err := yaml.Marshal(m)
				if err != nil {
					d.Fatalf(t, "failed to marshal rules: %v", err)
				}
				return string(out)
			case "deprules":
				s := append(([]registeredDepRule)(nil), registry.depRules...)
				sort.SliceStable(s, func(i, j int) bool {
					return s[i].name < s[j].name
				})
				out, err := yaml.Marshal(s)
				if err != nil {
					d.Fatalf(t, "failed to marshal deprules: %v", err)
				}
				return string(out)
			case "oprules":
				s := append(([]registeredOpRule)(nil), registry.opRules...)
				sort.SliceStable(s, func(i, j int) bool {
					return s[i].name < s[j].name
				})
				out, err := yaml.Marshal(s)
				if err != nil {
					d.Fatalf(t, "failed to marshal oprules: %v", err)
				}
				return string(out)
			}
			d.Fatalf(t, "deprules, oprules, and rules are the only commands, got %s", d.Cmd)
			return ""
		})
	})
}

func (r registeredDepRule) MarshalYAML() (interface{}, error) {
	var query yaml.Node
	if err := query.Encode(r.q.Clauses()); err != nil {
		return nil, err
	}
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "name"},
			{Kind: yaml.ScalarNode, Value: string(r.name)},
			{Kind: yaml.ScalarNode, Value: "from"},
			{Kind: yaml.ScalarNode, Value: string(r.from)},
			{Kind: yaml.ScalarNode, Value: "kind"},
			{Kind: yaml.ScalarNode, Value: r.kind.String()},
			{Kind: yaml.ScalarNode, Value: "to"},
			{Kind: yaml.ScalarNode, Value: string(r.to)},
			{Kind: yaml.ScalarNode, Value: "query"},
			&query,
		},
	}, nil
}

func (r registeredOpRule) MarshalYAML() (interface{}, error) {
	var query yaml.Node
	if err := query.Encode(r.q.Clauses()); err != nil {
		return nil, err
	}
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "name"},
			{Kind: yaml.ScalarNode, Value: string(r.name)},
			{Kind: yaml.ScalarNode, Value: "from"},
			{Kind: yaml.ScalarNode, Value: string(r.from)},
			{Kind: yaml.ScalarNode, Value: "query"},
			&query,
		},
	}, nil
}
