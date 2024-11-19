// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scgraph

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestDepEdgeTree exercises the depEdgeTree data structure to ensure it works
// as expected.
func TestDepEdgeTree(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type nodeID int
	type edge [2]nodeID
	// queryCase runs a query to iterate all edges sources at the node with id q.
	type queryCase struct {
		q    nodeID
		take int    // if > 1, indicates a desire to stop early
		res  []edge // expected results
	}
	// testCase describes the edges to be added and the queries to run.
	type testCase struct {
		order   edgeTreeOrder
		edges   []edge
		queries []queryCase
	}
	testCases := []testCase{
		{
			order: fromTo,
			edges: []edge{
				{2, 4}, {2, 3}, {4, 5}, {1, 2},
			},
			queries: []queryCase{
				{q: 1, res: []edge{{1, 2}}},
				{q: 2, res: []edge{{2, 3}, {2, 4}}},
				{q: 2, take: 1, res: []edge{{2, 3}}},
			},
		},
		{
			order: toFrom,
			edges: []edge{
				{2, 4}, {2, 3}, {4, 5}, {1, 2}, {2, 5}, {1, 5},
			},
			queries: []queryCase{
				{q: 1, res: nil},
				{q: 2, res: []edge{{1, 2}}},
				{q: 5, res: []edge{{1, 5}, {2, 5}, {4, 5}}},
				{q: 5, take: 1, res: []edge{{1, 5}}},
			},
		},
	}

	// testCaseState is used for each queryCase in a testCase.
	type testCaseState struct {
		depEdges  depEdges
		nodes     []*screl.Node // nodes with lower indexes sort lower
		nodesToID map[*screl.Node]nodeID
		order     edgeTreeOrder
	}
	makeTestCaseState := func(tc testCase) testCaseState {
		tcs := testCaseState{
			nodesToID: make(map[*screl.Node]nodeID),
		}
		target := scpb.Target{}
		getNode := func(i nodeID) *screl.Node {
			if i > nodeID(len(tcs.nodes)-1) {
				for j := nodeID(len(tcs.nodes)); j <= i; j++ {
					tcs.nodes = append(tcs.nodes, &screl.Node{
						Target:        &target,
						CurrentStatus: scpb.Status(j),
					})
					tcs.nodesToID[tcs.nodes[j]] = j
				}
			}
			return tcs.nodes[i]
		}
		tcs.depEdges = makeDepEdges(func(n *screl.Node) targetIdx {
			return targetIdx(tcs.nodesToID[n])
		})
		for _, e := range tc.edges {
			require.NoError(t, tcs.depEdges.insertOrUpdate(
				Rule{Name: "test", Kind: Precedence},
				Precedence,
				getNode(e[0]),
				getNode(e[1]),
			))
		}
		tcs.order = tc.order
		return tcs
	}
	runQueryCase := func(t *testing.T, tcs testCaseState, qc queryCase) {
		i := 0
		var res []edge
		it := tcs.depEdges.iterateFromNode
		if tcs.order == toFrom {
			it = tcs.depEdges.iterateToNode
		}
		require.NoError(t, it(tcs.nodes[qc.q], func(de *DepEdge) error {
			if i++; qc.take > 0 && i > qc.take {
				return iterutil.StopIteration()
			}
			res = append(res, edge{
				tcs.nodesToID[de.From()],
				tcs.nodesToID[de.To()],
			})
			return nil
		}))
		require.Equal(t, qc.res, res)
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v,%v", tc.order, tc.edges), func(t *testing.T) {
			tcs := makeTestCaseState(tc)
			for _, qc := range tc.queries {
				t.Run(fmt.Sprintf("%d,%d", qc.q, qc.take), func(t *testing.T) {
					runQueryCase(t, tcs, qc)
				})
			}
		})
	}
}
