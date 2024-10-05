// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cycle

import (
	"reflect"
	"testing"
)

type edge struct {
	from Vertex
	to   Vertex
}

func TestDetector(t *testing.T) {
	testCases := []struct {
		edges    []edge
		start    Vertex
		expected []Vertex
	}{
		// No cycles.
		{
			edges:    []edge{},
			start:    0,
			expected: nil,
		},
		{
			edges: []edge{
				{0, 1},
				{1, 2},
				{2, 3},
			},
			start:    0,
			expected: nil,
		},
		{
			edges: []edge{
				{0, 1},
				{0, 2},
				{0, 3},
				{1, 2},
				{2, 3},
			},
			start:    0,
			expected: nil,
		},
		{
			edges: []edge{
				{0, 1},
				{0, 2},
				{0, 3},
				{1, 2},
				{3, 1},
			},
			start:    0,
			expected: nil,
		},
		{
			edges: []edge{
				{0, 1},
				{0, 4},
				{1, 2},
				{1, 3},
				{4, 3},
				{4, 5},
				{5, 3},
			},
			start:    0,
			expected: nil,
		},
		{
			edges: []edge{
				{1, 2},
				{1, 3},
				{1, 4},
				{1, 7},
				{4, 5},
				{5, 6},
				{6, 7},
				{7, 4},
			},
			start:    0,
			expected: nil,
		},

		// Cycles.
		{
			edges: []edge{
				{0, 0},
			},
			start:    0,
			expected: []Vertex{0, 0},
		},
		{
			edges: []edge{
				{0, 1},
				{1, 0},
			},
			start:    0,
			expected: []Vertex{0, 1, 0},
		},
		{
			edges: []edge{
				{0, 1},
				{1, 0},
			},
			start:    1,
			expected: []Vertex{1, 0, 1},
		},
		{
			edges: []edge{
				{0, 1},
				{1, 2},
				{2, 3},
				{3, 4},
				{4, 1},
			},
			start:    0,
			expected: []Vertex{0, 1, 2, 3, 4, 1},
		},
		{
			edges: []edge{
				{0, 1},
				{1, 3},
				{3, 2},
				{2, 1},
			},
			start:    0,
			expected: []Vertex{0, 1, 3, 2, 1},
		},
		{
			edges: []edge{
				{0, 1},
				{0, 4},
				{1, 2},
				{1, 3},
				{3, 5},
				{4, 3},
				{4, 5},
				{5, 1},
			},
			start:    0,
			expected: []Vertex{0, 1, 3, 5, 1},
		},
		{
			edges: []edge{
				{1, 2},
				{1, 3},
				{1, 4},
				{1, 7},
				{4, 5},
				{5, 6},
				{6, 7},
				{7, 4},
			},
			start:    1,
			expected: []Vertex{1, 4, 5, 6, 7, 4},
		},
		{
			edges: []edge{
				{0, 1},
				{1, 2},
				{2, 3},
				{3, 3},
			},
			start:    1,
			expected: []Vertex{1, 2, 3, 3},
		},
	}
	for _, tc := range testCases {
		var d Detector
		for _, edge := range tc.edges {
			d.AddEdge(edge.from, edge.to)
		}
		res, _ := d.FindCycleStartingAtVertex(tc.start)
		if !reflect.DeepEqual(res, tc.expected) {
			t.Errorf("expected %v, got %v", tc.expected, res)
		}
	}
}
