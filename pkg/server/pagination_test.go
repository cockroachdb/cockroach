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
		fmt.Fprintf(&builder, "nodesQueried:", )
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
			start, end, newLimit := state.paginate(limit, roachpb.NodeID(nodeID), length)
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

func TestRPCPaginator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct{
		limits       []int
		numResponses map[roachpb.NodeID]int
		errors       int
	}{
		{[]int{3,1,5,7,9}, map[roachpb.NodeID]int{1: 5, 2: 10, 3: 7, 5: 10}, 0},
		{[]int{1,5,10}, map[roachpb.NodeID]int{1: 5, 2: 0, 3: -1, 5: 2}, 1},
	}

	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("testCase=%d", i), func(t *testing.T) {
			// Build a reference response first, to compare each potential limit with.
			var referenceResp []testNodeResponse
			for nodeID, numResponses := range tc.numResponses {
				for i := 0; i < numResponses; i++ {
					referenceResp = append(referenceResp, testNodeResponse{nodeID, i})
				}
			}
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
				if numResponses < 0 {
					return nil, errors.New("injected")
				}
				var response []testNodeResponse
				for i := 0; i < numResponses; i++ {
					response = append(response, testNodeResponse{nodeID, i})
				}
				return response, nil
			}

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
							errorFn: 			errorFn,
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
						if pagState.inProgress == 0 {
							break
						}
					}
					require.Equal(t, referenceResp, response)
					require.Equal(t, tc.errors, errorsDetected)
				})
			}
		})
	}

}
