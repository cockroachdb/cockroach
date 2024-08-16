// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package node_rac2

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestPiggybacker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	p := NewAdmittedPiggybacker()
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "piggybacker"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "add":
				var nodeID, storeID, rangeID, match int
				d.ScanArgs(t, "node-id", &nodeID)
				d.ScanArgs(t, "store-id", &storeID)
				d.ScanArgs(t, "range-id", &rangeID)
				// Match is just a placeholder to differentiate messages in the test.
				d.ScanArgs(t, "match", &match)
				p.AddMsgAppRespForLeader(roachpb.NodeID(nodeID), roachpb.StoreID(storeID),
					roachpb.RangeID(rangeID), raftpb.Message{Match: uint64(match)})
				return ""

			case "nodes-with-msgs":
				ts := parseTime(t, d)
				nodes := p.NodesWithMsgs(ts)
				slices.Sort(nodes)
				var b strings.Builder
				for i, n := range nodes {
					sep := " "
					if i == 0 {
						sep = ""
					}
					fmt.Fprintf(&b, "%sn%s", sep, n)
				}
				if len(nodes) > 0 {
					fmt.Fprintf(&b, "\n")
				}
				fmt.Fprintf(&b, "map len: %d\n", len(p.mu.msgsForNode))
				return b.String()

			case "pop":
				ts := parseTime(t, d)
				var nodeID int
				d.ScanArgs(t, "node-id", &nodeID)
				msgs, remaining := p.PopMsgsForNode(ts, roachpb.NodeID(nodeID), math.MaxInt64)
				slices.SortFunc(msgs, func(a, b kvflowcontrolpb.AdmittedResponseForRange) int {
					return cmp.Compare(a.RangeID, b.RangeID)
				})
				var b strings.Builder
				fmt.Fprintf(&b, "msgs:\n")
				for _, msg := range msgs {
					fmt.Fprintf(&b, "s%s, r%s, match=%d\n", msg.LeaderStoreID, msg.RangeID, msg.Msg.Match)
				}
				fmt.Fprintf(&b, "remaining-msgs: %d\n", remaining)
				return b.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func parseTime(t *testing.T, d *datadriven.TestData) time.Time {
	var timeSec int64
	d.ScanArgs(t, "time-sec", &timeSec)
	return time.UnixMilli(timeSec * 1000)
}

func TestPiggybackerMaxBytes(t *testing.T) {
	// This is not a datadriven test due to non-determinism of map iteration.
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	p := NewAdmittedPiggybacker()
	p.AddMsgAppRespForLeader(1, 1, 1, raftpb.Message{})
	p.AddMsgAppRespForLeader(1, 1, 2, raftpb.Message{})
	// Both are popped.
	msgs, remaining := p.PopMsgsForNode(time.UnixMilli(1), 1, 60)
	require.Equal(t, 2, len(msgs))
	require.Equal(t, 0, remaining)

	p.AddMsgAppRespForLeader(1, 1, 1, raftpb.Message{})
	p.AddMsgAppRespForLeader(1, 1, 2, raftpb.Message{})
	// Only one is popped.
	msgs, remaining = p.PopMsgsForNode(time.UnixMilli(1), 1, 20)
	require.Equal(t, 1, len(msgs))
	require.Equal(t, 1, remaining)
}
