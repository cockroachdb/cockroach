// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package reltest

import (
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// ComparisonTests exercise the comparison logic of the rel package.
type ComparisonTests struct {
	Entities []string
	Tests    []ComparisonTest
}

// ComparisonTest tests that the given entities order as expected given
// the attributes on which they are being compared. The expected order is
// provided in terms of groups which sort equally.
type ComparisonTest struct {
	Attrs []rel.Attr
	Order [][]string
}

func (ct ComparisonTests) run(t *testing.T, s Suite) {
	for _, tc := range ct.Tests {
		strs := make([]string, len(tc.Attrs))
		for i, a := range tc.Attrs {
			strs[i] = a.String()
		}
		t.Run(strings.Join(strs, ","), func(t *testing.T) {
			tc.run(t, s, ct.Entities)
		})
	}
}

func (ct ComparisonTest) run(t *testing.T, s Suite, entityNames []string) {
	entities := entitiesForComparison{
		schema:    s.Schema,
		attrs:     ct.Attrs,
		names:     entityNames,
		entities:  make([]interface{}, len(entityNames)),
		nameOrder: make(map[string]int, len(entityNames)),
	}
	for i, name := range entityNames {
		entities.entities[i] = s.Registry.MustGetByName(t, name)
		entities.nameOrder[name] = i
	}
	sort.Sort(entities)
	groups := [][]string{
		{entities.names[0]},
	}
	for i := 1; i < len(entities.entities); i++ {
		if s.Schema.EqualOn(ct.Attrs, entities.entities[i-1], entities.entities[i]) {
			last := len(groups) - 1
			groups[last] = append(groups[last], entities.names[i])
		} else {
			groups = append(groups, []string{entities.names[i]})
		}
	}
	require.Equal(t, ct.Order, groups)
}

type entitiesForComparison struct {
	schema    *rel.Schema
	attrs     []rel.Attr
	names     []string
	entities  []interface{}
	nameOrder map[string]int // tie break on insertion order
}

func (efc entitiesForComparison) Less(i, j int) bool {
	less, eq := efc.schema.CompareOn(efc.attrs, efc.entities[i], efc.entities[j])
	if !eq {
		return less
	}
	return efc.nameOrder[efc.names[i]] < efc.nameOrder[efc.names[j]]
}

func (efc entitiesForComparison) Len() int { return len(efc.entities) }
func (efc entitiesForComparison) Swap(i, j int) {
	efc.entities[i], efc.entities[j] = efc.entities[j], efc.entities[i]
	efc.names[i], efc.names[j] = efc.names[j], efc.names[i]
}

func (ct ComparisonTests) encode(t *testing.T) *yaml.Node {
	var entities yaml.Node
	require.NoError(t, entities.Encode(ct.Entities))
	entities.Style = yaml.FlowStyle
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			scalarYAML("entities"),
			&entities,
			scalarYAML("tests"),
			ct.encodeTests(t),
		},
	}
}

func (ct ComparisonTests) encodeTests(t *testing.T) *yaml.Node {
	var tests yaml.Node
	tests.Kind = yaml.SequenceNode
	for _, subtest := range ct.Tests {
		tests.Content = append(tests.Content, subtest.encode(t))
	}
	return &tests
}

func (ct ComparisonTest) encode(t *testing.T) *yaml.Node {
	var exp yaml.Node
	require.NoError(t, exp.Encode(ct.Order))
	exp.Style = yaml.FlowStyle
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			scalarYAML("attrs"),
			encodeAttrs(ct.Attrs),
			scalarYAML("order"),
			&exp,
		},
	}
}
