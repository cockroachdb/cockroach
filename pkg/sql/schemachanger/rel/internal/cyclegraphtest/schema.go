// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package cyclegraphtest contains test utilities and a "Suite" for reltest.
package cyclegraphtest

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/reltest"
	"gopkg.in/yaml.v3"
)

// testAttr is a rel.Attr used for testing.
type testAttr int8

var _ rel.Attr = testAttr(0)

//go:generate stringer --type testAttr  --tags test
const (
	s testAttr = iota
	s1
	s2
	c
	name
)

// struct1 is a struct which holds a bunch of potentially circular references.
type struct1 struct {
	// Name identifies the struct.
	Name string
	// M1 points to some other struct1, maybe itself.
	S1 *struct1
	// M2 points to some other struct2, maybe itself.
	S2 *struct2
	// C points to some container which may contain itself.
	C *container
}

// struct2 is like struct1 but has a different type.
type struct2 struct1

// container is a oneOf of struct1 or struct2.
type container struct {
	S1 *struct1
	S2 *struct2
}

// message is an interface to capture struct1 and struct2.
type message interface{ message() }

func (s *struct1) message() {}
func (s *struct2) message() {}

// This schema exercises cyclic references.
var schema = rel.MustSchema(
	"testschema",
	rel.AttrType(
		s, reflect.TypeOf((*message)(nil)).Elem(),
	),
	rel.EntityMapping(
		reflect.TypeOf((*struct1)(nil)),
		rel.EntityAttr(c, "C"),
		rel.EntityAttr(s1, "S1"),
		rel.EntityAttr(s2, "S2"),
		rel.EntityAttr(name, "Name"),
	),
	rel.EntityMapping(
		reflect.TypeOf((*struct2)(nil)),
		rel.EntityAttr(c, "C"),
		rel.EntityAttr(s1, "S1"),
		rel.EntityAttr(s2, "S2"),
		rel.EntityAttr(name, "Name"),
	),
	rel.EntityMapping(
		reflect.TypeOf((*container)(nil)),
		rel.EntityAttr(s, "S1", "S2"),
	),
)

// String helps ensure that serialization does not infinitely recurse.
func (s *struct1) String() string { return fmt.Sprintf("struct1(%s)", s.Name) }

// String helps ensure that serialization does not infinitely recurse.
func (s *struct2) String() string { return fmt.Sprintf("struct2(%s)", s.Name) }

// String helps ensure that serialization does not infinitely recurse.
func (c *container) String() string {
	var name string
	if c.S1 != nil {
		name = c.S1.Name
	} else {
		name = c.S2.Name
	}
	return fmt.Sprintf("container(%s)", name)
}

func (s *struct1) EncodeToYAML(t *testing.T, r *reltest.Registry) interface{} {
	yn := &yaml.Node{
		Kind:  yaml.MappingNode,
		Style: yaml.FlowStyle,
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Value: "name"},
			{Kind: yaml.ScalarNode, Value: s.Name},
		},
	}
	if s.S1 != nil {
		yn.Content = append(yn.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "s1"},
			&yaml.Node{Kind: yaml.ScalarNode, Value: r.MustGetName(t, s.S1)},
		)
	}
	if s.S2 != nil {
		yn.Content = append(yn.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "s2"},
			&yaml.Node{Kind: yaml.ScalarNode, Value: r.MustGetName(t, s.S2)},
		)
	}
	if s.C != nil {
		yn.Content = append(yn.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "c"},
			&yaml.Node{Kind: yaml.ScalarNode, Value: r.MustGetName(t, s.C)},
		)
	}
	return yn
}

func (s *struct2) EncodeToYAML(t *testing.T, r *reltest.Registry) interface{} {
	return (*struct1)(s).EncodeToYAML(t, r)
}

func (c *container) EncodeToYAML(t *testing.T, r *reltest.Registry) interface{} {
	yn := &yaml.Node{
		Kind:  yaml.MappingNode,
		Style: yaml.FlowStyle,
	}
	if c.S1 != nil {
		yn.Content = append(yn.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "s1"},
			&yaml.Node{Kind: yaml.ScalarNode, Value: r.MustGetName(t, c.S1)},
		)
	}
	if c.S2 != nil {
		yn.Content = append(yn.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "s2"},
			&yaml.Node{Kind: yaml.ScalarNode, Value: r.MustGetName(t, c.S2)},
		)
	}
	return yn
}
