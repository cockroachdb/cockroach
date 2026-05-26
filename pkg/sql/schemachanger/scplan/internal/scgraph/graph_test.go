// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scgraph_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestPlanGraphSort sanity checks sorting of the graph.
func TestGraphRanks(t *testing.T) {

	type depEdge struct {
		from, to int
	}

	type testCase struct {
		name     string
		addNode  []bool
		depEdges []depEdge
		hasCycle string
	}

	testCases := []testCase{

		// We will set up the dependency graph for basic ordering, so that
		// 2 depends on nothing.
		{
			name:    "simple dependency graph",
			addNode: []bool{true, true, true, true},
			depEdges: []depEdge{
				{0, 1},
				{3, 0},
			},
		},

		// We will set up the dependency graph, so that its intentionally cyclic,
		// which should result in an error.
		{
			name:    "cyclic graph",
			addNode: []bool{true, true, true, true},
			depEdges: []depEdge{
				{0, 1},
				{3, 0},
				{1, 3},
				{3, 1},
			},
			hasCycle: `cycle:
[[Table:{DescID: 0}, PUBLIC], PUBLIC] --> 0 to 1
[[Table:{DescID: 1}, PUBLIC], PUBLIC] --> 1 to 3
[[Table:{DescID: 3}, PUBLIC], PUBLIC] --> 3 to 0
[[Table:{DescID: 0}, PUBLIC], PUBLIC]
`,
		},

		// We will set up the dependency graph to have a swap, which won't affect
		// the fact that there's still a cycle.
		{
			name:    "dependency graph with a swap",
			addNode: []bool{true, false, true},
			depEdges: []depEdge{
				{0, 1},
				{1, 0},
				{2, 0},
			},
			hasCycle: `cycle:
[[Table:{DescID: 0}, PUBLIC], PUBLIC] --> 0 to 1
[[Table:{DescID: 1}, PUBLIC], PUBLIC] --> 1 to 0
[[Table:{DescID: 0}, PUBLIC], PUBLIC]
`,
		},
	}

	run := func(
		t *testing.T, tc testCase,
	) {
		// Setup a state based on if it is a add or drop.
		ts := scpb.TargetState{Targets: make([]scpb.Target, len(tc.addNode))}
		status := make([]scpb.Status, len(tc.addNode))
		for idx := range tc.addNode {
			ts.Targets[idx] = scpb.MakeTarget(
				scpb.ToPublic,
				&scpb.Table{
					TableID: descpb.ID(idx),
				},
				nil, /* metadata */
			)
			if tc.addNode[idx] {
				status[idx] = scpb.Status_ABSENT
			} else {
				status[idx] = scpb.Status_PUBLIC
			}
		}
		// Setup the nodes first.
		graph, err := scgraph.New(scpb.CurrentState{
			TargetState: ts,
			Initial:     status,
			Current:     status,
		})
		require.NoError(t, err)
		// Setup op edges for all the nodes.
		for idx := range tc.addNode {
			const revertible, canFail = true, false
			if tc.addNode[idx] {
				require.NoError(t, graph.AddOpEdges(
					&ts.Targets[idx],
					scpb.Status_ABSENT,
					scpb.Status_PUBLIC,
					revertible, canFail,
					&scop.MakeDeleteOnlyColumnAbsent{},
				))
			} else {
				require.NoError(t, graph.AddOpEdges(
					&ts.Targets[idx],
					scpb.Status_PUBLIC,
					scpb.Status_ABSENT,
					revertible, canFail,
					&scop.MakeDeleteOnlyColumnAbsent{},
				))
			}
		}
		// Add the dep edges next.
		for _, edge := range tc.depEdges {
			require.NoError(t, graph.AddDepEdge(
				scgraph.RuleName(fmt.Sprintf("%d to %d", edge.from, edge.to)),
				scgraph.Precedence,
				&ts.Targets[edge.from],
				scpb.Status_PUBLIC,
				&ts.Targets[edge.to],
				scpb.Status_PUBLIC,
			))
		}

		err = graph.Validate()
		if tc.hasCycle == "" {
			require.NoError(t, err)
		} else {
			require.Error(t, err, "graph is not acyclical")
			allDetails := errors.GetAllDetails(err)
			require.NotEmpty(t, allDetails)
			require.Equal(t, tc.hasCycle, allDetails[0])
		}
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}
