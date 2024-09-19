// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package entitynodetest exposes a reltest.Suite.
package entitynodetest

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/reltest"
	"gopkg.in/yaml.v3"
)

type entity struct {
	I8      int8
	PI8     *int8
	I16     int16
	I16Refs []int16
}

type node struct {
	Value       *entity
	Left, Right *node
}

func (n *node) EncodeToYAML(t *testing.T, r *reltest.Registry) interface{} {
	yn := yaml.Node{Kind: yaml.MappingNode, Style: yaml.FlowStyle}
	for _, f := range []struct {
		name  string
		field interface{}
		ok    bool
	}{
		{"value", n.Value, n.Value != nil},
		{"left", n.Left, n.Left != nil},
		{"right", n.Right, n.Right != nil},
	} {
		if !f.ok {
			continue
		}
		yn.Content = append(yn.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: f.name},
			&yaml.Node{Kind: yaml.ScalarNode, Value: r.MustGetName(t, f.field)},
		)
	}
	return &yn
}

var _ reltest.RegistryYAMLEncoder = (*node)(nil)

// testAttr is a rel.Attr used for testing.
type testAttr int8

var _ rel.Attr = testAttr(0)

//go:generate stringer --type testAttr  --tags test
const (
	i8 testAttr = iota
	pi8
	i16
	value
	left
	right
	i16ref
)

var schema = rel.MustSchema("testschema",
	rel.EntityMapping(reflect.TypeOf((*entity)(nil)),
		rel.EntityAttr(i8, "I8"),
		rel.EntityAttr(pi8, "PI8"),
		rel.EntityAttr(i16, "I16"),
		rel.EntityAttr(i16ref, "I16Refs"),
	),
	rel.EntityMapping(reflect.TypeOf((*node)(nil)),
		rel.EntityAttr(value, "Value"),
		rel.EntityAttr(left, "Left"),
		rel.EntityAttr(right, "Right"),
	),
)
