// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/node_rac2"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type transportControl struct {
	// Replication AC v2.
	piggybacker *node_rac2.AdmittedPiggybacker

	knobs                     *kvserver.RaftTransportTestingKnobs
	serverStreamDisconnected  atomic.Bool
	triggeredFallbackDispatch atomic.Bool
	workerTeardownCh          chan roachpb.NodeID
	chanServer                channelServer
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
	si, err := strconv.Atoi(strings.TrimPrefix(arg, "s"))
	require.NoError(t, err)
	return roachpb.StoreID(si)
}

func parseRangeID(t *testing.T, d *datadriven.TestData, key string) roachpb.RangeID {
	var arg string
	d.ScanArgs(t, key, &arg)
	si, err := strconv.Atoi(strings.TrimPrefix(arg, "r"))
	require.NoError(t, err)
	return roachpb.RangeID(si)
}

func parseReplicaDescriptor(
	t *testing.T, d *datadriven.TestData, key string,
) roachpb.ReplicaDescriptor {
	var arg string
	var desc roachpb.ReplicaDescriptor
	d.ScanArgs(t, key, &arg)
	parts := strings.Split(arg, "/")
	require.Len(t, parts, 3)
	ni, err := strconv.Atoi(strings.TrimPrefix(parts[0], "n"))
	require.NoError(t, err)
	store, err := strconv.Atoi(strings.TrimPrefix(parts[1], "s"))
	require.NoError(t, err)
	repl, err := strconv.Atoi(parts[2])
	require.NoError(t, err)

	desc.NodeID = roachpb.NodeID(ni)
	desc.StoreID = roachpb.StoreID(store)
	desc.ReplicaID = roachpb.ReplicaID(repl)
	return desc
}

// TestFlowControlRaftTransportV2 tests the integration of MsgAppResp
// piggybacking and RaftTransport. It offers the following commands:
//
//   - "init"
//     Initializes the raft transport test harness.
//
//   - "add" node=n<int> store=s<int>
//     Add a transport link on the given node, and registers a given store on
//     said node.
//
//   - "send" range=r<int> from=n<int>/s<int>/<int> to=n<int>/s<int>/<int> commit=<int>
//     Send a raft message with the given commit index for the named range, from
//     the given node+store+replica to the given node+store+replica.
//
//   - "piggyback" from=n<int> node=n<int> store=s<int> range=r<int>
//     Piggyback a message from a given node to the named node+store, for the
//     specific range.
//
//   - "fallback-piggyback" from=n<int>
//     Send all pending messages to be piggybacked from the named node. This
//     is equivalent to the periodic fallback piggyback the transport does to
//     guarantee delivery even if no messages are being sent to piggyback on
//     top of.
//
//   - "client-mark-idle" from=n<int> to=n<int>
//     Mark the transport connection between two nodes as idle. This is
//     equivalent to the underlying RaftTransport stream being torn down.
//     Specifically, the client initiated stream from the first node towards the
//     second. So this is a unidirectional breakage. Of course the server side
//     can observe it, and various tests showcase this fact.
//
//   - "drop-disconnected-piggybacks" from=n<int>
//     Drop messages to be piggybacked that are pending delivery to nodes
//     we're disconnected from. This is equivalent to the periodic memory
//     reclamation the transport does to ensure we don't accumulate messages.
//     unboundedly.
//
//   - "connection-tracker" from=n<int>
//     Print out the exact nodes and stores the RaftTransport on the given node
//     is connected to, as a server and as a client.
//
//   - "pending-piggybacks" from=n<int> to=n<int>
//     List the pending piggybacks from one node to another.
//
//   - "metrics"
//     Print out transport metrics.
func TestFlowControlRaftTransportV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvadmission.FlowTokenDispatchInterval.Override(ctx, &st.SV, time.Hour) // we control this manually below

	datadriven.Walk(t, datapathutils.TestDataPath(t, "flow_control_raft_transport_v2"),
		func(t *testing.T, path string) {
			var rttc *raftTransportTestContext
			defer func() {
				if rttc != nil {
					rttc.Stop()
				}
			}()

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
							piggybacker:      node_rac2.NewAdmittedPiggybacker(),
							workerTeardownCh: workerTeardownCh,
						}
						controlM[nodeID].knobs = &kvserver.RaftTransportTestingKnobs{
							TriggerFallbackDispatchCh: make(chan time.Time),
							OnFallbackDispatch: func() {
								controlM[nodeID].triggeredFallbackDispatch.Store(true)
							},
							MarkSendQueueAsIdleCh: make(chan roachpb.NodeID),
							OnWorkerTeardown: func(nodeID roachpb.NodeID) {
								workerTeardownCh <- nodeID
							},
							OnServerStreamDisconnected: func() {
								controlM[nodeID].serverStreamDisconnected.Store(true)
							},
						}

						transport, addr := rttc.AddNodeWithoutGossip(
							nodeID,
							util.TestAddr,
							rttc.stopper,
							controlM[nodeID].piggybacker,
							noopPiggybackedAdmittedResponseScheduler{},
							controlM[nodeID].knobs,
						)
						rttc.GossipNode(nodeID, addr)
						controlM[nodeID].chanServer = rttc.ListenStore(nodeID, storeID)
						require.NoError(t, transport.Start(ctx))
						return ""

					case "send":
						// Parse range=r<int>.
						var arg string
						d.ScanArgs(t, "range", &arg)
						ri, err := strconv.Atoi(strings.TrimPrefix(arg, "r"))
						require.NoError(t, err)
						rangeID := roachpb.RangeID(ri)

						from := parseReplicaDescriptor(t, d, "from") // parse from=n<int>/s<int>/<int>
						to := parseReplicaDescriptor(t, d, "to")     // parse to=n<int>/s<int>/<int>

						// Parse commit=<int>.
						d.ScanArgs(t, "commit", &arg)
						c, err := strconv.Atoi(arg)
						require.NoError(t, err)

						testutils.SucceedsSoon(t, func() error {
							if !rttc.Send(from, to, rangeID, raftpb.Message{Commit: uint64(c)}) {
								breaker, ok := rttc.transports[from.NodeID].GetCircuitBreaker(to.NodeID, rpc.DefaultClass)
								require.True(t, ok)
								breaker.Reset()
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

					case "piggyback":
						// Parse node=n<int>.
						fromNodeID := parseNodeID(t, d, "from")
						control, found := controlM[fromNodeID]
						require.True(t, found, "uninitialized node, did you use 'add node=n%s'?", fromNodeID)

						toNodeID := parseNodeID(t, d, "node")
						toStoreID := parseStoreID(t, d, "store")
						rangeID := parseRangeID(t, d, "range")
						// TODO(pav-kv): test that these messages are actually sent in
						// RaftMessageRequestBatch.
						control.piggybacker.Add(toNodeID, kvflowcontrolpb.PiggybackedAdmittedState{
							RangeID: rangeID, ToStoreID: toStoreID,
						})
						return ""

					case "fallback-piggyback":
						fromNodeID := parseNodeID(t, d, "from")
						control, found := controlM[fromNodeID]
						require.True(t, found, "uninitialized node, did you use 'add node=n%s'?", fromNodeID)
						select {
						case control.knobs.TriggerFallbackDispatchCh <- time.Time{}:
						case <-time.After(time.Second):
							return "timed out"
						}
						testutils.SucceedsSoon(t, func() error {
							if control.triggeredFallbackDispatch.Load() {
								return nil
							}
							return errors.Errorf("waiting for fallback mechanism to activate")
						})
						control.triggeredFallbackDispatch.Store(false) // reset
						return ""

					case "client-mark-idle":
						fromNodeID := parseNodeID(t, d, "from")
						toNodeID := parseNodeID(t, d, "to")

						control, found := controlM[fromNodeID]
						require.True(t, found, "uninitialized node, did you use 'add node=n%s'?", fromNodeID)
						select {
						case control.knobs.MarkSendQueueAsIdleCh <- toNodeID:
						case <-time.After(time.Second):
							return "timed out"
						}
						select {
						case gotNodeID := <-control.workerTeardownCh:
							require.Equal(t, gotNodeID, toNodeID)
						case <-time.After(time.Second):
							return "timed out"
						}

						toControl, found := controlM[toNodeID]
						require.True(t, found, "uninitialized node, did you use 'add node=n%s'?", toNodeID)
						testutils.SucceedsSoon(t, func() error {
							if toControl.serverStreamDisconnected.Load() {
								return nil
							}
							return errors.Errorf("waiting for server-side stream to disconnect")
						})
						toControl.serverStreamDisconnected.Store(false) // reset
						return ""

					case "drop-disconnected-piggybacks":
						nodeID := parseNodeID(t, d, "from")
						transport, found := rttc.transports[nodeID]
						require.True(t, found, "uninitialized node, did you use 'add node=n%s'?", nodeID)
						transport.TestingDropFlowTokensForDisconnectedNodes()
						return ""

					case "connection-tracker":
						fromNodeID := parseNodeID(t, d, "from")
						transport, found := rttc.transports[fromNodeID]
						require.True(t, found, "uninitialized node, did you use 'add node=n%s'?", fromNodeID)
						return transport.TestingPrintFlowControlConnectionTracker()

					case "pending-piggybacks":
						fromNodeID := parseNodeID(t, d, "from")
						toNodeID := parseNodeID(t, d, "to")

						control, found := controlM[fromNodeID]
						require.True(t, found, "uninitialized node, did you use 'add node=n%s'?", fromNodeID)

						var buf strings.Builder
						ranges := control.piggybacker.RangesWithMsgsForTesting(toNodeID)
						fmt.Fprintf(&buf, "ranges:")
						if len(ranges) == 0 {
							fmt.Fprintf(&buf, " none\n")
						} else {
							for _, r := range ranges {
								fmt.Fprintf(&buf, " r%s", r)
							}
							fmt.Fprintf(&buf, "\n")
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
							buf.WriteString(fmt.Sprintf("node=n%s: dispatches-dropped=%d\n",
								nodeID,
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

type noopPiggybackedAdmittedResponseScheduler struct{}

func (s noopPiggybackedAdmittedResponseScheduler) ScheduleAdmittedResponseForRangeRACv2(
	context.Context, []kvflowcontrolpb.PiggybackedAdmittedState,
) {
}
