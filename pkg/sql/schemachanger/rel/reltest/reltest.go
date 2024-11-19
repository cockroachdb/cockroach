// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package reltest provides tools for testing the rel package.
package reltest

import (
	"flag"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var rewrite bool

func init() {
	flag.BoolVar(&rewrite, "rewrite", false, "set to rewrite the test output")
}

// Suite represents a set of tests for rel.
//
// It is exposed like this to make it easier to write tests with
// custom types in a separate package without polluting the package
// namespace with those types.
//
// The tests contained in this package contain a special property
// of being able to serialize themselves to an easier-to-read
// format which will be written out to testdata. In that way, they
// also serve as a test of the serialization logic of the rel library.
type Suite struct {

	// Name of the suite.
	Name string

	// Schema used by the suite.
	Schema *rel.Schema

	// Registry which stores the entities used in tests.
	Registry *Registry

	// AttributeTests test the extraction of attributes from
	// entities.
	AttributeTests []AttributeTestCase

	// DatabaseTests test queries executed over subsets of data
	// being inserted into a database.
	DatabaseTests []DatabaseTest

	// ComparisonTests test the comparison logic.
	ComparisonTests []ComparisonTests
}

// Run will run the suite.
func (s Suite) Run(t *testing.T) {
	for _, tc := range s.DatabaseTests {
		t.Run("database", func(t *testing.T) {
			tc.run(t, s)
		})
	}
	t.Run("attributes", func(t *testing.T) {
		for _, tc := range s.AttributeTests {
			t.Run(tc.Entity, func(t *testing.T) {
				tc.run(t, s)
			})
		}
	})
	t.Run("comparison", func(t *testing.T) {
		for _, tc := range s.ComparisonTests {
			t.Run(strings.Join(tc.Entities, ","), func(t *testing.T) {
				tc.run(t, s)
			})
		}
	})
	t.Run("yaml", func(t *testing.T) {
		s.writeYAML(t)
	})
}

func (s Suite) writeYAML(t *testing.T) {
	out, err := yaml.Marshal(s.toYAML(t))
	require.NoError(t, err)
	tdp := datapathutils.TestDataPath(t, s.Name)
	if rewrite {
		require.NoError(t, os.WriteFile(tdp, out, 0777))
	} else {
		exp, err := os.ReadFile(tdp)
		require.NoError(t, err)
		require.Equal(t, exp, out)
	}
}

func (s Suite) toYAML(t *testing.T) *yaml.Node {
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			scalarYAML("name"),
			scalarYAML(s.Name),
			scalarYAML("data"),
			s.encodeData(t),
			scalarYAML("attributes"),
			s.encodeAttributes(t),
			scalarYAML("rules"),
			s.encodeRules(t),
			scalarYAML("queries"),
			s.encodeQueries(t),
			scalarYAML("comparisons"),
			s.encodeComparisons(t),
		},
	}
}

func (s Suite) encodeData(t *testing.T) *yaml.Node {
	encodeValue := func(name string) *yaml.Node {
		return s.Registry.valueToYAML(t, name)
	}
	n := yaml.Node{Kind: yaml.MappingNode}
	for _, name := range s.Registry.names {
		n.Content = append(n.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: name},
			encodeValue(name),
		)
	}
	return &n
}

func (s Suite) encodeQueries(t *testing.T) *yaml.Node {
	queries := &yaml.Node{Kind: yaml.SequenceNode}
	for _, q := range s.DatabaseTests {
		queries.Content = append(queries.Content,
			q.encode(t, s.Registry),
		)
	}
	return queries
}

func (s Suite) encodeAttributes(t *testing.T) *yaml.Node {
	n := yaml.Node{Kind: yaml.MappingNode}
	for _, tc := range s.AttributeTests {
		n.Content = append(n.Content,
			scalarYAML(tc.Entity),
			tc.encode(t, s),
		)
	}
	return &n
}

func (s Suite) encodeComparisons(t *testing.T) *yaml.Node {
	n := yaml.Node{Kind: yaml.SequenceNode}
	for _, ct := range s.ComparisonTests {
		n.Content = append(n.Content, ct.encode(t))
	}
	return &n
}

func (s Suite) encodeRules(t *testing.T) *yaml.Node {
	n := yaml.Node{Kind: yaml.SequenceNode}
	s.Schema.ForEachRule(func(def rel.RuleDef) {
		var r yaml.Node
		require.NoError(t, r.Encode(def))
		n.Content = append(n.Content, &r)
	})
	return &n
}

func scalarYAML(value string) *yaml.Node {
	return &yaml.Node{
		Kind:  yaml.ScalarNode,
		Value: value,
	}
}
