// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package current

import (
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
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
				out, err := registry.MarshalDepRules()
				require.NoError(t, err)
				return out
			case "oprules":
				out, err := registry.MarshalOpRules()
				require.NoError(t, err)
				return out
			}
			d.Fatalf(t, "deprules, oprules, and rules are the only commands, got %s", d.Cmd)
			return ""
		})
	})
}
