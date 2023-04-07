// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowdispatch"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

// TestFlowTokenTransport tests the piggybacking of flow tokens on messages sent
// through the raft transport. It offers the following commands:
//
//   - "init"
//     Initializes the raft transport.
//
//   - "add" node=n<int> store=s<int>
//     Add a transport link to the given node, and registers a given store on said
//     node.
//
//   - "send" range=r<int> from=n<int>/<int>/s<int> to=n<int>/<int>/s<int> commit=<int>
//     Send a raft message with the given commit index for the named range, from
//     the given node+replica+store to the given node+replica+store
//
//   - "dispatch" from=n<int>
//     node=n<int> range=r<int> pri=<pri> store=s<int> up-to-log-position=<int>/<int>
//     ...
//     Dispatch flow tokens (identified by a log prefix) from a given node to
//     the named node+store, for the specific range and priority.
//
//   - "mark-idle" from=n<int> to=n<int>
//     Mark the transport connection between two nodes as idle. This is
//     equivalent to the underlying RaftTransport stream being torn down.
//
//   - "dispatch-idle" from=n2
//     Dispatch all pending flow tokens from the named node. This is akin to the
//     periodic dispatch the transport does to guarantee delivery even if
//     streams are idle or no messages are being sent to piggyback on top of.
//
//   - "pending-dispatch" from=n<int> to=n<int>
//     List the pending dispatches from one node to another.
//
//   - "metrics"
//     Print out transport metrics.
func TestFlowTokenTransport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvadmission.FlowTokenDispatchInterval.Override(ctx, &st.SV, 0) // we control this manually below

	datadriven.Walk(t, datapathutils.TestDataPath(t, "flow_token_transport"),
		func(t *testing.T, path string) {
			var rttc *raftTransportTestContext
			defer func() {
				if rttc != nil {
					rttc.Stop()
				}
			}()

			type transportControl struct {
				dispatch         *kvflowdispatch.Dispatch
				knobs            *kvserver.RaftTransportTestingKnobs
				workerTeardownCh chan roachpb.NodeID
				chanServer       channelServer
			}
			controlM := make(map[roachpb.NodeID]*transportControl)
			datadriven.RunTest(t, path,
				func(t *testing.T, d *datadriven.TestData) string {
					switch d.Cmd {
					case "init":
						if rttc != nil {
							rttc.Stop()
						}
						rttc = newRaftTransportTestContext(t, st)
						return ""

					case "add":
						nodeID := parseNodeID(t, d, "node")
						storeID := parseStoreID(t, d, "store")
						workerTeardownCh := make(chan roachpb.NodeID, 1)
						controlM[nodeID] = &transportControl{
							dispatch:         kvflowdispatch.New(metric.NewRegistry(), nil, &base.NodeIDContainer{}),
							workerTeardownCh: workerTeardownCh,
							knobs: &kvserver.RaftTransportTestingKnobs{
								MarkSendQueueAsIdleCh: make(chan roachpb.NodeID),
								OnWorkerTeardown: func(nodeID roachpb.NodeID) {
									workerTeardownCh <- nodeID
								},
							},
						}
						transport, addr := rttc.AddNodeWithoutGossip(
							nodeID,
							util.TestAddr,
							rttc.stopper,
							controlM[nodeID].dispatch,
							controlM[nodeID].knobs,
						)
						rttc.GossipNode(nodeID, addr)
						controlM[nodeID].chanServer = rttc.ListenStore(nodeID, storeID)
						require.NoError(t, transport.StartFlowTokenLoop(ctx))
						return ""

					case "send":
						// Parse range=r<int>.
						var arg string
						d.ScanArgs(t, "range", &arg)
						ri, err := strconv.Atoi(strings.TrimPrefix(arg, "r"))
						require.NoError(t, err)
						rangeID := roachpb.RangeID(ri)

						var from, to roachpb.ReplicaDescriptor
						{ // Parse from=n<int>/<int>/s<int>.
							d.ScanArgs(t, "from", &arg)
							parts := strings.Split(arg, "/")
							require.Len(t, parts, 3)
							ni, err := strconv.Atoi(strings.TrimPrefix(parts[0], "n"))
							require.NoError(t, err)
							repl, err := strconv.Atoi(parts[1])
							require.NoError(t, err)
							store, err := strconv.Atoi(strings.TrimPrefix(parts[2], "s"))
							require.NoError(t, err)

							from.NodeID = roachpb.NodeID(ni)
							from.ReplicaID = roachpb.ReplicaID(repl)
							from.StoreID = roachpb.StoreID(store)
						}

						{ // Parse to=n<int>/<int>/s<int>.
							d.ScanArgs(t, "to", &arg)
							parts := strings.Split(arg, "/")
							require.Len(t, parts, 3)
							ni, err := strconv.Atoi(strings.TrimPrefix(parts[0], "n"))
							require.NoError(t, err)
							repl, err := strconv.Atoi(parts[1])
							require.NoError(t, err)
							store, err := strconv.Atoi(strings.TrimPrefix(parts[2], "s"))
							require.NoError(t, err)

							to.NodeID = roachpb.NodeID(ni)
							to.ReplicaID = roachpb.ReplicaID(repl)
							to.StoreID = roachpb.StoreID(store)
						}

						// Parse commit=<int>.
						d.ScanArgs(t, "commit", &arg)
						c, err := strconv.Atoi(arg)
						require.NoError(t, err)

						testutils.SucceedsSoon(t, func() error {
							if !rttc.Send(from, to, rangeID, raftpb.Message{Commit: uint64(c)}) {
								rttc.transports[from.NodeID].GetCircuitBreaker(to.NodeID, rpc.DefaultClass).Reset()
							}
							select {
							case req := <-controlM[to.NodeID].chanServer.ch:
								if req.Message.Commit == uint64(c) {
									return nil
								}
							case <-time.After(time.Second):
							}
							return errors.Errorf("expected message commit=%d", c)
						})
						return ""

					case "dispatch": // cargo-culted from kvflowdispatch.TestDispatch
						// Parse node=n<int>.
						fromNodeID := parseNodeID(t, d, "from")
						control, found := controlM[fromNodeID]
						require.True(t, found, "uninitialized node, did you use 'add node=n%s'?", fromNodeID)

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
							control.dispatch.Dispatch(nodeID, entries)
						}
						return ""

					case "mark-idle":
						fromNodeID := parseNodeID(t, d, "from")
						toNodeID := parseNodeID(t, d, "to")

						control, found := controlM[fromNodeID]
						require.True(t, found, "uninitialized node, did you use 'add node=n%s'?", fromNodeID)
						select {
						case control.knobs.MarkSendQueueAsIdleCh <- toNodeID:
						case <-time.After(200 * time.Millisecond):
							return "timed out"
						}
						select {
						case gotNodeID := <-control.workerTeardownCh:
							require.Equal(t, gotNodeID, toNodeID)
						case <-time.After(200 * time.Millisecond):
							return "timed out"
						}
						return ""

					case "dispatch-idle":
						nodeID := parseNodeID(t, d, "from")
						transport, found := rttc.transports[nodeID]
						require.True(t, found, "uninitialized node, did you use 'add node=n%s'?", nodeID)
						transport.TestingDispatchPendingFlowTokens()
						return ""

					case "pending-dispatch": // cargo-culted from kvflowdispatch.TestDispatch
						fromNodeID := parseNodeID(t, d, "from")
						toNodeID := parseNodeID(t, d, "to")

						control, found := controlM[fromNodeID]
						require.True(t, found, "uninitialized node, did you use 'add node=n%s'?", fromNodeID)

						var buf strings.Builder
						es := control.dispatch.PendingDispatchFor(toNodeID)
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
							control.dispatch.Dispatch(toNodeID, entries) // re-add to dispatch
						}
						return buf.String()

					case "metrics":
						var buf strings.Builder
						var nodeIDs roachpb.NodeIDSlice
						for nodeID := range rttc.transports {
							nodeIDs = append(nodeIDs, nodeID)
						}
						sort.Sort(nodeIDs)

						for _, nodeID := range nodeIDs {
							transport := rttc.transports[nodeID]
							buf.WriteString(fmt.Sprintf("node=n%s: dispatches-delivered=%d dispatches-dropped=%d\n",
								nodeID,
								transport.Metrics().FlowTokenDispatchesSent.Count(),
								transport.Metrics().FlowTokenDispatchesDropped.Count(),
							))
						}
						return buf.String()

					default:
						return "unknown command"
					}
				})
		},
	)
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

func parseNodeID(t *testing.T, d *datadriven.TestData, key string) roachpb.NodeID {
	var arg string
	d.ScanArgs(t, key, &arg)
	ni, err := strconv.Atoi(strings.TrimPrefix(arg, "n"))
	require.NoError(t, err)
	return roachpb.NodeID(ni)
}

func parseStoreID(t *testing.T, d *datadriven.TestData, key string) roachpb.StoreID {
	var arg string
	d.ScanArgs(t, key, &arg)
	ni, err := strconv.Atoi(strings.TrimPrefix(arg, "s"))
	require.NoError(t, err)
	return roachpb.StoreID(ni)
}
