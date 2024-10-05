// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cyclegraphtest

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/reltest"
)

// Suite is the suite for testproto.
var Suite = reltest.Suite{
	Name:          "cyclegraph",
	Schema:        schema,
	Registry:      reg,
	DatabaseTests: databaseTests,
}

var (
	reg = reltest.NewRegistry()

	// Here we create a bunch of cyclic relationships between these 4 entities.
	message1, message2, container1, container2 = func() (*struct1, *struct2, *container, *container) {
		s1, s2, c1, c2 := &struct1{}, &struct2{}, &container{}, &container{}
		*c1 = container{S1: s1}
		*c2 = container{S2: s2}
		*s1 = struct1{Name: "message1", S1: s1, S2: s2, C: c2}
		*s2 = struct2{Name: "message2", S1: s1, S2: s2, C: c1}

		reg.Register("message1", s1)
		reg.Register("message2", s2)
		reg.Register("container1", c1)
		reg.Register("container2", c2)

		return s1, s2, c1, c2
	}()

	databaseTests = []reltest.DatabaseTest{
		{
			Data: []string{"container1"}, // recursively will add it all, test that
			Indexes: [][]rel.Index{
				{{}},
				{
					{Attrs: []rel.Attr{s}},
					{Attrs: []rel.Attr{c}},
					{Attrs: []rel.Attr{name}},
				},
			},
			QueryCases: []reltest.QueryTest{
				{
					Name: "oneOf member",
					Query: rel.Clauses{
						rel.Var("c").Type((*container)(nil)),
						rel.Var("c").AttrEqVar(s, "s"),
					},
					ResVars:  []rel.Var{"c", "s"},
					Entities: []rel.Var{"c"},
					Results: [][]interface{}{
						{container1, message1},
						{container2, message2},
					},
					UnsatisfiableIndexes: []int{1},
				},
				{
					Name: "oneOf member",
					Query: rel.Clauses{
						rel.Var("c").AttrEq(s, message1),
					},
					ResVars:  []rel.Var{"c"},
					Entities: []rel.Var{"c"},
					Results: [][]interface{}{
						{container1},
					},
				},
			},
		},
	}
)
