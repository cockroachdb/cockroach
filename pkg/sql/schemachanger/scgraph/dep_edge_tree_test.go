// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scgraph

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
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
		tree      *depEdgeTree
		nodes     []*scpb.Node // nodes with lower indexes sort lower
		nodesToID map[*scpb.Node]nodeID
	}
	makeTestCaseState := func(tc testCase) testCaseState {
		tcs := testCaseState{
			nodesToID: make(map[*scpb.Node]nodeID),
		}
		target := scpb.Target{}
		getNode := func(i nodeID) *scpb.Node {
			if i > nodeID(len(tcs.nodes)-1) {
				for j := nodeID(len(tcs.nodes)); j <= i; j++ {
					tcs.nodes = append(tcs.nodes, &scpb.Node{
						Target: &target,
						Status: scpb.Status(j),
					})
					tcs.nodesToID[tcs.nodes[j]] = j
				}
			}
			return tcs.nodes[i]
		}
		tcs.tree = newDepEdgeTree(tc.order, func(a, b *scpb.Node) (less, eq bool) {
			ai, bi := tcs.nodesToID[a], tcs.nodesToID[b]
			return ai < bi, ai == bi
		})
		for _, e := range tc.edges {
			tcs.tree.insert(&DepEdge{
				from: getNode(e[0]),
				to:   getNode(e[1]),
			})
		}
		return tcs
	}
	runQueryCase := func(t *testing.T, tcs testCaseState, qc queryCase) {
		i := 0
		var res []edge
		require.NoError(t, tcs.tree.iterateSourceNode(tcs.nodes[qc.q], func(de *DepEdge) error {
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
