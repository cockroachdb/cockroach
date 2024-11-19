// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package comparetest

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/reltest"
)

var (
	// Suite defines the comparetest test suite.
	Suite = reltest.Suite{
		Name:            "comparetest",
		Schema:          schema,
		Registry:        r,
		ComparisonTests: comparisonTests,
		AttributeTests:  attributesTests,
	}

	r = reltest.NewRegistry()

	_ = r.FromYAML("e1", `{i16: 1, i8: 1, pi8: 1}`, &entity{})
	_ = r.FromYAML("e2", `{i16: 2, i8: 2, pi16: 2}`, &entity{})
	_ = r.FromYAML("e3", `{i16: 3, i8: 3, pi16: 3}`, &entity{})
	_ = r.FromYAML("e4", `{uint: 4, puint: 4}`, &entity{})

	attributesTests = reltest.AttributeTestCases{
		{
			Entity: "e1",
			Expected: addToEmptyEntityMap(map[rel.Attr]interface{}{
				i16: int16(1),
				i8:  int8(1),
				pi8: int8(1),
			}),
		},
	}
	comparisonTests = []reltest.ComparisonTests{
		{
			Entities: []string{"e1", "e2", "e3"},
			Tests: []reltest.ComparisonTest{
				{
					Attrs: []rel.Attr{i8},
					Order: [][]string{
						{"e1"}, {"e2"}, {"e3"},
					},
				},
				{ // all nil
					Attrs: []rel.Attr{pstr},
					Order: [][]string{
						{"e1", "e2", "e3"},
					},
				},
				{
					Attrs: []rel.Attr{pi8},
					Order: [][]string{
						{"e1"}, {"e2", "e3"},
					},
				},
				{
					Attrs: []rel.Attr{pi8, pi16},
					Order: [][]string{
						{"e1"}, {"e2"}, {"e3"},
					},
				},
				{
					Attrs: []rel.Attr{pi16},
					Order: [][]string{
						{"e2"}, {"e3"}, {"e1"},
					},
				},
			},
		},
		{
			Entities: []string{"e1", "e2", "e3", "e4"},
			Tests: []reltest.ComparisonTest{
				{
					Attrs: []rel.Attr{ui},
					Order: [][]string{
						{"e1", "e2", "e3", "e4"},
					},
				},
				{ // all nil
					Attrs: []rel.Attr{pstr},
					Order: [][]string{
						{"e1", "e2", "e3", "e4"},
					},
				},
				{
					Attrs: []rel.Attr{pi8},
					Order: [][]string{
						{"e1"}, {"e2", "e3", "e4"},
					},
				},
				{
					Attrs: []rel.Attr{pi8, pi16},
					Order: [][]string{
						{"e1"}, {"e2"}, {"e3"}, {"e4"},
					},
				},
				{
					Attrs: []rel.Attr{pi16},
					Order: [][]string{
						{"e2"}, {"e3"}, {"e1", "e4"},
					},
				},
			},
		},
	}
)

func addToEmptyEntityMap(m map[rel.Attr]interface{}) map[rel.Attr]interface{} {
	base := map[rel.Attr]interface{}{
		i8:       int8(0),
		i16:      int16(0),
		i32:      int32(0),
		i64:      int64(0),
		ui8:      uint8(0),
		ui16:     uint16(0),
		ui32:     uint32(0),
		ui64:     uint64(0),
		i:        0,
		ui:       uint(0),
		str:      "",
		_uintptr: uintptr(0),
	}
	for k, v := range m {
		base[k] = v
	}
	return base
}
