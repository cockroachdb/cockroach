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
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
		fmt.Fprintf(&builder, "nodesQueried: ", )
		for i, node := range state.nodesQueried {
			if i > 0 {
				fmt.Fprintf(&builder, ",")
			}
			fmt.Fprintf(&builder, "%d", node)
		}
		fmt.Fprintf(&builder, "\ninProgress: %d", state.inProgress)
		fmt.Fprintf(&builder, "\ninProgressIndex: %d", state.inProgressIndex)
		fmt.Fprintf(&builder, "\nnodesToQuery: ")
		for i, node := range state.nodesToQuery {
			if i > 0 {
				fmt.Fprintf(&builder, ",")
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
