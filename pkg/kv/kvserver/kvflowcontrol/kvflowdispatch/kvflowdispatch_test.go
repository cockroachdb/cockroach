// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowdispatch

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestDispatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		var dispatch *Dispatch
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				nodeIDContainer := &base.NodeIDContainer{}
				if d.HasArg("node") {
					// Parse node=n<int>.
					var arg string
					d.ScanArgs(t, "node", &arg)
					ni, err := strconv.Atoi(strings.TrimPrefix(arg, "n"))
					require.NoError(t, err)
					nodeID := roachpb.NodeID(ni)
					nodeIDContainer.Set(ctx, nodeID)
				}
				dispatch = New(metric.NewRegistry(), dummyHandles{}, nodeIDContainer)
				return ""

			case "dispatch":
				require.NotNilf(t, dispatch, "uninitialized dispatch (did you use 'init'?)")

				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					require.Len(t, parts, 5, "expected form 'node=n<int> range=r<int> pri=<string> store=s<int> up-to-log-position=<int>/<int>'")

					var (
						entries kvflowcontrolpb.AdmittedRaftLogEntries
						nodeID  roachpb.NodeID
					)
					for i := range parts {
						parts[i] = strings.TrimSpace(parts[i])
						inner := strings.Split(parts[i], "=")
						require.Len(t, inner, 2)
						arg := strings.TrimSpace(inner[1])

						switch {
						case strings.HasPrefix(parts[i], "node="):
							// Parse node=n<int>.
							ni, err := strconv.Atoi(strings.TrimPrefix(arg, "n"))
							require.NoError(t, err)
							nodeID = roachpb.NodeID(ni)

						case strings.HasPrefix(parts[i], "range="):
							// Parse range=r<int>.
							ri, err := strconv.Atoi(strings.TrimPrefix(arg, "r"))
							require.NoError(t, err)
							entries.RangeID = roachpb.RangeID(ri)

						case strings.HasPrefix(parts[i], "store="):
							// Parse store=s<int>.
							si, err := strconv.Atoi(strings.TrimPrefix(arg, "s"))
							require.NoError(t, err)
							entries.StoreID = roachpb.StoreID(si)

						case strings.HasPrefix(parts[i], "pri="):
							// Parse pri=<string>.
							pri, found := admissionpb.TestingReverseWorkPriorityDict[arg]
							require.True(t, found)
							entries.AdmissionPriority = int32(pri)

						case strings.HasPrefix(parts[i], "up-to-log-position="):
							// Parse up-to-log-position=<int>/<int>.
							entries.UpToRaftLogPosition = parseLogPosition(t, arg)

						default:
							t.Fatalf("unrecognized prefix: %s", parts[i])
						}
					}
					dispatch.Dispatch(ctx, nodeID, entries)
				}
				return ""

			case "pending-dispatch":
				require.NotNilf(t, dispatch, "uninitialized dispatch (did you use 'init'?)")
				var buf strings.Builder
				nodes := dispatch.PendingDispatch()
				sort.Slice(nodes, func(i, j int) bool { // for determinism
					return nodes[i] < nodes[j]
				})
				for i, node := range nodes {
					if i != 0 {
						buf.WriteString("\n")
					}
					buf.WriteString(fmt.Sprintf("node=n%d", node))
				}
				return buf.String()

			case "pending-dispatch-for":
				require.NotNilf(t, dispatch, "uninitialized dispatch (did you use 'init'?)")
				var arg string
				d.ScanArgs(t, "node", &arg)
				ni, err := strconv.Atoi(strings.TrimPrefix(arg, "n"))
				require.NoError(t, err)
				var buf strings.Builder
				es, _ := dispatch.PendingDispatchFor(roachpb.NodeID(ni), math.MaxInt)
				sort.Slice(es, func(i, j int) bool { // for determinism
					if es[i].RangeID != es[j].RangeID {
						return es[i].RangeID < es[j].RangeID
					}
					if es[i].StoreID != es[j].StoreID {
						return es[i].StoreID < es[j].StoreID
					}
					if es[i].AdmissionPriority != es[j].AdmissionPriority {
						return es[i].AdmissionPriority < es[j].AdmissionPriority
					}
					return es[i].UpToRaftLogPosition.Less(es[j].UpToRaftLogPosition)
				})
				for i, entries := range es {
					if i != 0 {
						buf.WriteString("\n")
					}
					buf.WriteString(
						fmt.Sprintf("range=r%d pri=%s store=s%d up-to-log-position=%s",
							entries.RangeID,
							admissionpb.WorkPriority(entries.AdmissionPriority),
							entries.StoreID,
							entries.UpToRaftLogPosition,
						),
					)
				}
				return buf.String()

			case "metrics":
				return printMetrics(dispatch)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

// TestDispatchSize is used to estimate the size of a marshalled
// kvflowcontrolpb.AdmittedRaftLogEntries object. It provides an approximation
// to use as an upper limit for kvadmission.flow_control.dispatch.max_bytes.
func TestDispatchSize(t *testing.T) {
	entry, err := protoutil.Marshal(&kvflowcontrolpb.AdmittedRaftLogEntries{
		RangeID:           math.MaxInt64,
		AdmissionPriority: math.MaxInt32,
		UpToRaftLogPosition: kvflowcontrolpb.RaftLogPosition{
			Term:  math.MaxUint64,
			Index: math.MaxUint64,
		},
		StoreID: math.MaxInt32,
	})
	require.NoError(t, err)
	require.LessOrEqual(t, len(entry), AdmittedRaftLogEntriesBytes, "consider adjusting kvadmission.flow_control.dispatch.max_bytes")
}

func parseLogPosition(t *testing.T, input string) kvflowcontrolpb.RaftLogPosition {
	inner := strings.Split(input, "/")
	require.Len(t, inner, 2)
	term, err := strconv.Atoi(inner[0])
	require.NoError(t, err)
	index, err := strconv.Atoi(inner[1])
	require.NoError(t, err)
	return kvflowcontrolpb.RaftLogPosition{
		Term:  uint64(term),
		Index: uint64(index),
	}
}

func printMetrics(d *Dispatch) string {
	metrics := d.testingMetrics()
	var buf strings.Builder
	buf.WriteString(fmt.Sprintf(`pending-nodes=%d
[regular] pending=%d coalesced=%d dispatch{local=%d remote=%d}
[elastic] pending=%d coalesced=%d dispatch{local=%d remote=%d}
`,
		metrics.PendingNodes.Value(),
		metrics.PendingDispatches[admissionpb.RegularWorkClass].Value(),
		metrics.CoalescedDispatches[admissionpb.RegularWorkClass].Count(),
		metrics.LocalDispatch[admissionpb.RegularWorkClass].Count(),
		metrics.RemoteDispatch[admissionpb.RegularWorkClass].Count(),
		metrics.PendingDispatches[admissionpb.ElasticWorkClass].Value(),
		metrics.CoalescedDispatches[admissionpb.ElasticWorkClass].Count(),
		metrics.LocalDispatch[admissionpb.ElasticWorkClass].Count(),
		metrics.RemoteDispatch[admissionpb.ElasticWorkClass].Count(),
	))
	return buf.String()
}

type dummyHandles struct{}

func (d dummyHandles) Lookup(id roachpb.RangeID) (kvflowcontrol.Handle, bool) {
	return nil, false
}

func (d dummyHandles) LookupReplicationAdmissionHandle(
	rangeID roachpb.RangeID,
) (kvflowcontrol.ReplicationAdmissionHandle, bool) {
	return d.Lookup(rangeID)
}

func (d dummyHandles) ResetStreams(ctx context.Context) {}

func (d dummyHandles) Inspect() []roachpb.RangeID {
	return nil
}

func (d dummyHandles) LookupInspect(id roachpb.RangeID) (kvflowinspectpb.Handle, bool) {
	return kvflowinspectpb.Handle{}, false
}

var _ kvflowcontrol.Handles = dummyHandles{}
