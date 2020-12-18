// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSimplePaginate is a datadriven-based test for simplePaginate().
// Commands:
//
// paginate <limit> <offset>
// <input>
// ----
// result=<result>
// next=<next>
//
// Calls paginate().
// input args:
//  - limit: max number of elements to return.
//  - offset: index offset since the start of slice.
//  - input: comma-separated list of ints used as input to simplePaginate.
// output args:
//  - result: the sub-sliced input returned from simplePaginate.
//  - next: the next offset.
func TestSimplePaginate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.RunTest(t, "testdata/simple_paginate", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "paginate":
			var input interface{}
			if len(d.CmdArgs) != 2 {
				return "expected 2 args: paginate <limit> <offset>"
			}
			limit, err := strconv.Atoi(d.CmdArgs[0].Key)
			if err != nil {
				return err.Error()
			}
			offset, err := strconv.Atoi(d.CmdArgs[1].Key)
			if err != nil {
				return err.Error()
			}
			inputString := strings.TrimSpace(d.Input)
			if len(inputString) > 0 {
				var inputSlice []int
				for _, part := range strings.Split(inputString, ",") {
					val, err := strconv.Atoi(strings.TrimSpace(part))
					if err != nil {
						return err.Error()
					}
					inputSlice = append(inputSlice, val)
				}
				input = inputSlice
			}
			result, next := simplePaginate(input, limit, offset)
			return fmt.Sprintf("result=%v\nnext=%d", result, next)
		default:
			return fmt.Sprintf("unexpected command: %s", d.Cmd)
		}
	})
}

// TestPaginationState is a datadriven-based test for paginationState and
// related methods.
//
// Commands:
//
// define
// queried=<queried>
// in-progress=<in-progress>
// in-progress-index=<in-progress-index>
// to-query=<to-query>
// ----
// <printed-state>
//
// Resets and defines a new paginationState.
// input args:
//  - queried: list of queried nodeIDs, comma-separated
//  - in-progress: node ID of current cursor position's node
//  - in-progress-index: index of current cursor position within current node's
//    response
//  - to-query: list of node IDs yet to query, comma-separated
// output args:
//  - printed-state: textual representation of current pagination state.
//
//
// merge-node-ids
// <nodes>
// ----
// <printed-state>
//
// Calls mergeNodeIDs().
// input args:
//  - nodes: sorted node IDs to merge into pagination state, using mergeNodeIDs.
// output args:
//  - printed-state: textual representation of current pagination state.
//
//
// paginate
// limit=<limit>
// nodeID=<nodeID>
// length=<length>
// ----
// start: <start>
// end: <end>
// newLimit: <newlimit>
// state: <printed-state>
//
// Calls paginate()
// input args:
//  - limit: Max objects to return from paginate().
//  - nodeID: ID of node the response is coming from.
//  - length: length of values in current node's response.
// output args:
//  - start: Start idx of response slice.
//  - end: End idx of response slice.
//  - newLimit: Limit to be used on next call to paginate(), if current slice
//    doesn't have `limit` remaining items. 0 if `limit` was reached.
//  - printed-state: textual representation of current pagination state.
//
//
// unmarshal
// <input>
// ----
// <printed-state>
//
// Unmarshals base64-encoded string into a paginationState. Opposite of marshal.
// input args:
//  - input: base64-encoded string to unmarshal.
// output args:
//  - printed-state: textual representation of unmarshalled pagination state.
//
//
// marshal
// ----
// <text>
//
// Marshals current state to base64-encoded string.
// output args:
//  - text: base64-encoded string that can be passed to unmarshal.
func TestPaginationState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	parseNodesString := func(t *testing.T, nodesString string) []roachpb.NodeID {
		var res []roachpb.NodeID
		for _, node := range strings.Split(nodesString, ",") {
			i, err := strconv.Atoi(strings.TrimSpace(node))
			require.NoError(t, err)
			res = append(res, roachpb.NodeID(i))
		}
		return res
	}
	printState := func(state paginationState) string {
		var builder strings.Builder
		fmt.Fprintf(&builder, "nodesQueried:")
		for i, node := range state.nodesQueried {
			if i > 0 {
				fmt.Fprintf(&builder, ",")
			} else {
				fmt.Fprintf(&builder, " ")
			}
			fmt.Fprintf(&builder, "%d", node)
		}
		fmt.Fprintf(&builder, "\ninProgress: %d", state.inProgress)
		fmt.Fprintf(&builder, "\ninProgressIndex: %d", state.inProgressIndex)
		fmt.Fprintf(&builder, "\nnodesToQuery:")
		for i, node := range state.nodesToQuery {
			if i > 0 {
				fmt.Fprintf(&builder, ",")
			} else {
				fmt.Fprintf(&builder, " ")
			}
			fmt.Fprintf(&builder, "%d", node)
		}
		return builder.String()
	}

	var state paginationState
	datadriven.RunTest(t, "testdata/pagination_state", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "define":
			state = paginationState{}
			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Split(line, ":")
				switch parts[0] {
				case "queried":
					state.nodesQueried = parseNodesString(t, parts[1])
				case "to-query":
					state.nodesToQuery = parseNodesString(t, parts[1])
				case "in-progress":
					inProgress, err := strconv.Atoi(strings.TrimSpace(parts[1]))
					require.NoError(t, err)
					state.inProgress = roachpb.NodeID(inProgress)
				case "in-progress-index":
					inProgressIdx, err := strconv.Atoi(strings.TrimSpace(parts[1]))
					require.NoError(t, err)
					state.inProgressIndex = inProgressIdx
				default:
					return fmt.Sprintf("unexpected keyword: %s", parts[0])
				}
			}
			return "ok"

		case "merge-node-ids":
			state.mergeNodeIDs(parseNodesString(t, d.Input))
			return printState(state)

		case "paginate":
			var limit, nodeID, length int
			var err error
			for _, line := range strings.Split(d.Input, "\n") {
				fields := strings.Fields(line)
				if len(fields) != 2 {
					return "expected lines in the format <field> <value>"
				}
				switch fields[0] {
				case "limit":
					limit, err = strconv.Atoi(fields[1])
				case "nodeID":
					nodeID, err = strconv.Atoi(fields[1])
				case "length":
					length, err = strconv.Atoi(fields[1])
				default:
					return fmt.Sprintf("unexpected field: %s", fields[0])
				}
				require.NoError(t, err)
			}
			start, end, newLimit, err := state.paginate(limit, roachpb.NodeID(nodeID), length)
			if err != nil {
				return err.Error()
			}
			return fmt.Sprintf("start: %d\nend: %d\nnewLimit: %d\nstate:\n%s", start, end, newLimit, printState(state))

		case "marshal":
			textState, err := state.MarshalText()
			require.NoError(t, err)
			return string(textState)

		case "unmarshal":
			require.NoError(t, state.UnmarshalText([]byte(d.Input)))
			return printState(state)

		default:
			return fmt.Sprintf("unexpected command: %s", d.Cmd)
		}
	})
}

type testNodeResponse struct {
	nodeID roachpb.NodeID
	val    int
}

// TestRPCPaginator tests the rpcPaginator struct. It constructs a hypothetical
// underlying RPC response from nodes, then sends off fake RPCs to them in
// parallel, while letting rpcPaginator merge and truncate those replies
// according to set limits. The total elements returned should match across
// different limit values.
func TestRPCPaginator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Each test case consists of a set of limit values to be tested, and a
	// map of node IDs and the number of elements to be returned by the nodeFn's
	// of each of those nodes. The test repeatedly calls rpcPaginator with a limit
	// until there are no more values to return. The total set of values returned
	// by the paginator should match across all values of limits in a given test
	// case.
	//
	// If numResponses has a negative int for a particular node, that node returns
	// an error instead. The number of expected errors is stored in `errors`.
	testCases := []struct {
		limits       []int
		numResponses map[roachpb.NodeID]int
		errors       int
	}{
		{[]int{3, 1, 5, 7, 9}, map[roachpb.NodeID]int{1: 5, 2: 10, 3: 7, 5: 10}, 0},
		{[]int{1, 5, 10}, map[roachpb.NodeID]int{1: 5, 2: 0, 3: -1, 5: 2}, 1},
	}

	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	for i, tc := range testCases {
		tc := tc
		t.Run(fmt.Sprintf("testCase=%d", i), func(t *testing.T) {
			// Build a reference response first, to compare each potential limit with.
			var referenceResp []testNodeResponse
			for nodeID, numResponses := range tc.numResponses {
				for i := 0; i < numResponses; i++ {
					referenceResp = append(referenceResp, testNodeResponse{nodeID, i})
				}
			}
			// Build a reference response, consisting of all the fake node responses
			// sorted by node IDs and then vals. The paginator will be expected to
			// return a sub-slice of this larger response each time it's called, and
			// by the end, appending the paginator's responses to one another should
			// yield a response that matches it exactly.
			sort.Slice(referenceResp, func(i, j int) bool {
				if referenceResp[i].nodeID == referenceResp[j].nodeID {
					return referenceResp[i].val < referenceResp[j].val
				}
				return referenceResp[i].nodeID < referenceResp[j].nodeID
			})
			dialFn := func(ctx context.Context, id roachpb.NodeID) (client interface{}, err error) {
				return id, nil
			}
			nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (res interface{}, err error) {
				numResponses := tc.numResponses[nodeID]
				// If a negative value is stored, return an error instead.
				if numResponses < 0 {
					return nil, errors.New("injected")
				}
				var response []testNodeResponse
				// For positive values of numResponses, return slices of ints that go
				// [0, 1, 2, ..., numResponses-1].
				for i := 0; i < numResponses; i++ {
					response = append(response, testNodeResponse{nodeID, i})
				}
				return response, nil
			}

			// For each limit specified in tc.limits, run the paginator until
			// all values are exhausted.
			for _, limit := range tc.limits {
				t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
					var response []testNodeResponse
					errorsDetected := 0
					responseFn := func(nodeID roachpb.NodeID, resp interface{}) {
						if val, ok := resp.([]testNodeResponse); ok {
							response = append(response, val...)
						}
					}
					errorFn := func(nodeID roachpb.NodeID, nodeFnError error) {
						errorsDetected++
					}
					var pagState paginationState
					sortedNodeIDs := make([]roachpb.NodeID, 0, len(tc.numResponses))
					for nodeID := range tc.numResponses {
						sortedNodeIDs = append(sortedNodeIDs, nodeID)
					}
					sort.Slice(sortedNodeIDs, func(i, j int) bool {
						return sortedNodeIDs[i] < sortedNodeIDs[j]
					})
					pagState.mergeNodeIDs(sortedNodeIDs)
					for {
						nodesToQuery := []roachpb.NodeID{pagState.inProgress}
						nodesToQuery = append(nodesToQuery, pagState.nodesToQuery...)
						paginator := rpcNodePaginator{
							limit:        limit,
							numNodes:     len(nodesToQuery),
							errorCtx:     "test",
							pagState:     pagState,
							nodeStatuses: make(map[roachpb.NodeID]nodeStatusWithLiveness),
							dialFn:       dialFn,
							nodeFn:       nodeFn,
							responseFn:   responseFn,
							errorFn:      errorFn,
						}
						paginator.init()

						// Issue requests in parallel.
						for idx, nodeID := range nodesToQuery {
							go func(nodeID roachpb.NodeID, idx int) {
								paginator.queryNode(ctx, nodeID, idx)
							}(nodeID, idx)
						}

						var err error
						pagState, err = paginator.processResponses(ctx)
						require.NoError(t, err)
						// When no node is "in progress", we've gotten all values and can
						// break out of this loop.
						if pagState.inProgress == 0 {
							break
						}
					}
					// The chained paginated responses should match the reference response
					// that was built earlier.
					require.Equal(t, referenceResp, response)
					require.Equal(t, tc.errors, errorsDetected)
				})
			}
		})
	}

}
