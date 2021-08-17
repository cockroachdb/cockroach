// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scgraph_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/stretchr/testify/require"
)

type depEdge struct {
	from, to int
}

func runRankTestForGraph(
	t *testing.T, addNode []bool, depEdges []depEdge, expectedOrder []int, expectedRankErr string,
) {
	// Setup a state based on if it is a add or drop.
	state := scpb.State{
		Nodes: make([]*scpb.Node, 0, len(addNode)),
	}
	dummyMetadata := &scpb.TargetMetadata{}
	for idx := range addNode {
		if addNode[idx] {
			state.Nodes = append(state.Nodes, &scpb.Node{
				Target: scpb.NewTarget(scpb.Target_ADD,
					&scpb.Table{
						TableID: descpb.ID(idx),
					},
					dummyMetadata),
				Status: scpb.Status_ABSENT,
			})
		} else {
			state.Nodes = append(state.Nodes, &scpb.Node{
				Target: scpb.NewTarget(scpb.Target_DROP,
					&scpb.Table{
						TableID: descpb.ID(idx),
					},
					dummyMetadata),
				Status: scpb.Status_PUBLIC,
			})
		}
	}
	// Setup the nodes first.
	graph, err := scgraph.New(state)
	require.NoError(t, err)
	// Setup op edges for all the nodes.
	for idx := range addNode {
		if addNode[idx] {
			require.NoError(t, graph.AddOpEdges(state.Nodes[idx].Target,
				scpb.Status_ABSENT,
				scpb.Status_PUBLIC,
				true,
				&scop.MakeColumnAbsent{}))
		} else {
			require.NoError(t, graph.AddOpEdges(state.Nodes[idx].Target,
				scpb.Status_PUBLIC,
				scpb.Status_ABSENT,
				true,
				&scop.MakeColumnAbsent{}))
		}
	}
	// Add the dep edges next.
	for _, edge := range depEdges {
		require.NoError(t, graph.AddDepEdge(
			fmt.Sprintf("%d to %d", edge.from, edge.to),
			state.Nodes[edge.from].Target,
			scpb.Status_PUBLIC,
			state.Nodes[edge.to].Target,
			scpb.Status_PUBLIC,
		))
	}

	// Validates the rank order for nodes.
	validateNodeRanks := func(graph *scgraph.Graph, expectedOrder []int) {
		rank, err := graph.GetNodeRanks()
		if expectedRankErr != "" {
			require.Errorf(t, err, expectedRankErr)
			return // Nothing else to validate
		} else {
			require.NoError(t, err)
		}
		unsortedNodes := make([]*scpb.Node, 0, len(state.Nodes))
		for _, node := range state.Nodes {
			publicNode, ok := graph.GetNode(node.Target, scpb.Status_PUBLIC)
			require.Truef(t, ok, "public node doesn't exist")
			unsortedNodes = append(unsortedNodes, publicNode)
		}
		sort.SliceStable(unsortedNodes, func(i, j int) bool {
			return rank[unsortedNodes[i]] > rank[unsortedNodes[j]]
		})
		sortedOrder := make([]int, 0, len(unsortedNodes))
		for _, node := range unsortedNodes {
			sortedOrder = append(sortedOrder, int(node.Table.TableID))
		}
		require.EqualValues(t, expectedOrder, sortedOrder, "ranks are not in expected order")
	}
	validateNodeRanks(graph, expectedOrder)
}

// TestPlanGraphSort sanity checks sorting of the graph.
func TestGraphRanks(t *testing.T) {
	// We will set up the dependency graph for basic ordering, so that:
	// 1) 0 depends on 1
	// 2) 3 depends on 0
	// 3) 2 depends on nothing
	t.Run("simple dependency graph", func(t *testing.T) {
		runRankTestForGraph(t,
			[]bool{true, true, true, true},
			[]depEdge{
				{0, 1},
				{3, 0},
			},
			[]int{1, 0, 2, 3},
			"",
		)
	})

	// We will set up the dependency graph, so that its
	// intentionally cyclic, which should panic:
	// 1) 0 depends on 1
	// 2) 3 depends on 0
	// 3) 1 depends on 3
	// 4) 3 depends on 1
	t.Run("cyclic graph", func(t *testing.T) {
		runRankTestForGraph(t,
			[]bool{true, true, true, true},
			[]depEdge{
				{0, 1},
				{3, 0},
				{1, 3},
				{3, 1},
			},
			nil, // Not expecting this to run.
			"graph is not a dag",
		)
	})

	// We will set up the dependency graph to have a swap
	// 1) 0 (adding) depends on 1 (dropping)
	// 2) 1 (dropping) depends on 0 (adding)
	// 3) 2 (adding) depends on 0 (adding)
	t.Run("dependency graph with a swap", func(t *testing.T) {
		runRankTestForGraph(t,
			[]bool{true, false, true},
			[]depEdge{
				{0, 1},
				{1, 0},
				{2, 0},
			},
			[]int{1, 0, 2}, // We expect the drop to be ordered first.
			"",
		)
	})
}
