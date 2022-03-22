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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/datadriven"
	"gopkg.in/yaml.v3"
)

// TestRulesYAML outputs the rules to yaml as a way to visualize changes.
func TestRulesYAML(t *testing.T) {
	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "deprules":
				out, err := yaml.Marshal(registry.depRules)
				if err != nil {
					d.Fatalf(t, "failed to marshal deprules: %v", err)
				}
				return string(out)
			case "oprules":
				out, err := yaml.Marshal(registry.opRules)
				if err != nil {
					d.Fatalf(t, "failed to marshal oprules: %v", err)
				}
				return string(out)
			}
			d.Fatalf(t, "deprules and oprules are the only commands")
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
			{Kind: yaml.ScalarNode, Value: r.name},
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
			{Kind: yaml.ScalarNode, Value: r.name},
			{Kind: yaml.ScalarNode, Value: "from"},
			{Kind: yaml.ScalarNode, Value: string(r.from)},
			{Kind: yaml.ScalarNode, Value: "query"},
			&query,
		},
	}, nil
}
