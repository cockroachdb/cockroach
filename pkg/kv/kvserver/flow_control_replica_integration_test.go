// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestFlowControlReplicaIntegration tests the kvflowcontrol replica integration
// interfaces. It offers the following commands:
//
//   - "init" tenant=t<int> range=r<int> replid=<int>
//     ----
//     Initializes the flow control integration interface, using a replica for
//     the given range+tenant and the given replica ID.
//
//   - "state" [applied=<int>/<int>] [descriptor=(<int>[,<int]*)] \
//     [paused=([<int>][,<int>]*) [inactive=([<int>][,<int>]*) \
//     [progress=([<int>@<int>:[probe | replicate | snapshot]:[!,]active:[!,]paused]*] \
//     [disconnected=([<int>][,<int>]*)]
//     ----
//     Set up relevant state of the underlying replica and/or raft transport.
//
//     A. For replicas, we can control the applied state (term/index),
//     descriptor (set of replica IDs), paused and/or inactive replicas, and
//     per-replica raft progress. The raft progress syntax is structured as
//     follows: progress=(replid@match:<state>:<active>:<paused>,...).
//     <state> is one of {probe,replicate,snapshot}, <active> is
//     {active,!inactive}, and <paused> is {paused,!paused}. The latter controls
//     MsgAppProbesPaused in the raft library, not the CRDB-level follower
//     pausing.
//
//     B. For the raft transport, we can specify the set of replica IDs we're
//     not connected to.
//
//   - "integration" op=[became-leader | became-follower | desc-changed |
//     followers-paused |replica-destroyed |
//     proposal-quota-updated]
//     ----
//     Invoke the specific APIs integration interface, informing it of the
//     underlying replica acquire raft leadership, losing it, its range
//     descriptor changing, a change in the set of paused followers, it being
//     destroyed, and its proposal quota being updated
func TestFlowControlReplicaIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t, "flow_control_replica_integration"),
		func(t *testing.T, path string) {
			var mockReplica *mockReplicaForFlowControl
			var mockHandleFactory *mockFlowHandleFactory
			var integration replicaFlowControlIntegration
			var logger *testLogger
			datadriven.RunTest(t, path,
				func(t *testing.T, d *datadriven.TestData) string {
					if d.Cmd == "init" {
						require.Nil(t, mockReplica)
						require.Nil(t, mockHandleFactory)
						require.Nil(t, integration)
						require.Nil(t, logger)
					} else {
						require.NotNil(t, mockReplica)
						require.NotNil(t, mockHandleFactory)
						require.NotNil(t, integration)
						require.NotNil(t, logger)
					}

					switch d.Cmd {
					case "init":
						var arg string

						// Parse range=r<int>.
						d.ScanArgs(t, "range", &arg)
						ri, err := strconv.Atoi(strings.TrimPrefix(arg, "r"))
						require.NoError(t, err)
						rangeID := roachpb.RangeID(ri)

						// Parse tenant=t<int>.
						d.ScanArgs(t, "tenant", &arg)
						ti, err := strconv.Atoi(strings.TrimPrefix(arg, "t"))
						require.NoError(t, err)
						tenantID := roachpb.MustMakeTenantID(uint64(ti))

						// Parse replid=<int>.
						d.ScanArgs(t, "replid", &arg)
						repli, err := strconv.Atoi(arg)
						require.NoError(t, err)
						replID := roachpb.ReplicaID(repli)

						logger = newTestLogger()

						mockHandleFactory = newMockFlowHandleFactory(t, logger)
						mockReplica = newMockReplicaForFlowControl(t, rangeID, tenantID, replID)
						integration = newReplicaFlowControlIntegration(mockReplica, mockHandleFactory, nil /* knobs */)
						return ""

					case "state":
						for _, arg := range d.CmdArgs {
							replicas := roachpb.MakeReplicaSet(nil)
							progress := make(map[roachpb.ReplicaID]tracker.Progress)
							disconnected := make(map[roachpb.ReplicaID]struct{})
							for i := range arg.Vals {
								if arg.Vals[i] == "" {
									continue // we support syntax like inactive=(); there's nothing to do
								}
								switch arg.Key {
								case "progress":
									// Parse progress=(repl@match:<state>:<active>:<paused>,...).
									//  <state>  = one of probe, replicate, or snapshot
									//  <active> = one of active or !inactive
									//  <paused> = one of paused of !paused
									parts := strings.Split(arg.Vals[i], ":")
									require.Len(t, parts, 4)

									// Parse repl@match.
									match := strings.Split(parts[0], "@")
									require.Len(t, match, 2)
									repli, err := strconv.Atoi(match[0])
									require.NoError(t, err)
									replID := roachpb.ReplicaID(repli)
									index, err := strconv.Atoi(match[1])
									require.NoError(t, err)

									// Parse <state> (one of probe, replicate, or snapshot).
									var state tracker.StateType
									switch parts[1] {
									case "probe":
										state = tracker.StateProbe
									case "replicate":
										state = tracker.StateReplicate
									case "snapshot":
										state = tracker.StateSnapshot
									default:
										t.Fatalf("unknown <state>: %s", parts[1])
									}

									// Parse <active> (one of active or !inactive).
									require.True(t, parts[2] == "active" || parts[2] == "!active")
									active := parts[2] == "active"

									// Parse <paused> (one of paused or !paused).
									require.True(t, parts[3] == "paused" || parts[3] == "!paused")
									paused := parts[3] == "paused"

									progress[replID] = tracker.Progress{
										Match:              uint64(index),
										State:              state,
										RecentActive:       active,
										MsgAppProbesPaused: paused,
										Inflights:          tracker.NewInflights(1, 0), // avoid NPE
										IsLearner:          false,
									}

								case "descriptor", "paused", "inactive":
									// Parse key=(<int>,<int>,...).
									var id uint64
									arg.Scan(t, i, &id)
									replicas.AddReplica(
										roachpb.ReplicaDescriptor{
											NodeID:    roachpb.NodeID(id),
											StoreID:   roachpb.StoreID(id),
											ReplicaID: roachpb.ReplicaID(id),
											Type:      roachpb.VOTER_FULL,
										},
									)

								case "applied":
									// Fall through.

								case "disconnected":
									// Parse key=(<int>,<int>,...).
									var id uint64
									arg.Scan(t, i, &id)
									disconnected[roachpb.ReplicaID(id)] = struct{}{}

								default:
									t.Fatalf("unknown: %s", arg.Key)
								}
							}

							switch arg.Key {
							case "descriptor":
								mockReplica.descriptor.SetReplicas(replicas)

							case "paused":
								mockReplica.paused = make(map[roachpb.ReplicaID]struct{})
								for _, repl := range replicas.Descriptors() {
									mockReplica.paused[repl.ReplicaID] = struct{}{}
								}

							case "inactive":
								mockReplica.inactive = make(map[roachpb.ReplicaID]struct{})
								for _, repl := range replicas.Descriptors() {
									mockReplica.inactive[repl.ReplicaID] = struct{}{}
								}

							case "progress":
								mockReplica.progress = progress

							case "applied":
								// Parse applied=<int>/<int>.
								mockReplica.applied = parseLogPosition(t, arg.Vals[0])

							case "disconnected":
								mockReplica.disconnected = disconnected

							default:
								t.Fatalf("unknown: %s", arg.Key)
							}
						}
						return ""

					case "integration":
						var op string
						d.ScanArgs(t, "op", &op)
						switch op {
						case "became-leader":
							integration.onBecameLeader(ctx)
						case "became-follower":
							integration.onBecameFollower(ctx)
						case "desc-changed":
							integration.onDescChanged(ctx)
						case "followers-paused":
							integration.onFollowersPaused(ctx)
						case "destroyed":
							integration.onDestroyed(ctx)
						case "raft-ticked":
							integration.onRaftTicked(ctx)
						default:
							t.Fatalf("unknown op: %s", op)
						}
						return logger.output()

					default:
						return "unknown command"
					}
				})
		},
	)
}

type mockReplicaForFlowControl struct {
	t         *testing.T
	rangeID   roachpb.RangeID
	tenantID  roachpb.TenantID
	replicaID roachpb.ReplicaID

	paused       map[roachpb.ReplicaID]struct{}
	inactive     map[roachpb.ReplicaID]struct{}
	disconnected map[roachpb.ReplicaID]struct{}
	progress     map[roachpb.ReplicaID]tracker.Progress
	applied      kvflowcontrolpb.RaftLogPosition
	descriptor   *roachpb.RangeDescriptor
}

var _ replicaForFlowControl = &mockReplicaForFlowControl{}

func newMockReplicaForFlowControl(
	t *testing.T, rangeID roachpb.RangeID, tenantID roachpb.TenantID, replicaID roachpb.ReplicaID,
) *mockReplicaForFlowControl {
	repl := &mockReplicaForFlowControl{
		t:         t,
		rangeID:   rangeID,
		tenantID:  tenantID,
		replicaID: replicaID,

		paused:   make(map[roachpb.ReplicaID]struct{}),
		inactive: make(map[roachpb.ReplicaID]struct{}),
		progress: make(map[roachpb.ReplicaID]tracker.Progress),
	}
	repl.descriptor = roachpb.NewRangeDescriptor(
		rangeID, roachpb.RKeyMin, roachpb.RKeyMax,
		roachpb.MakeReplicaSet([]roachpb.ReplicaDescriptor{
			repl.getReplicaDescriptor(),
		}),
	)
	return repl
}

func (m *mockReplicaForFlowControl) assertLocked() {}

func (m *mockReplicaForFlowControl) annotateCtx(ctx context.Context) context.Context {
	return ctx
}

func (m *mockReplicaForFlowControl) getTenantID() roachpb.TenantID {
	return m.tenantID
}

func (m *mockReplicaForFlowControl) getReplicaID() roachpb.ReplicaID {
	return m.replicaID
}

func (m *mockReplicaForFlowControl) getRangeID() roachpb.RangeID {
	return m.rangeID
}

func (m *mockReplicaForFlowControl) getDescriptor() *roachpb.RangeDescriptor {
	return m.descriptor
}

func (m *mockReplicaForFlowControl) getPausedFollowers() map[roachpb.ReplicaID]struct{} {
	return m.paused
}

func (m *mockReplicaForFlowControl) getBehindFollowers() map[roachpb.ReplicaID]struct{} {
	// NB: Keep this identical to the canonical implementation of
	// getBehindFollowers.
	behindFollowers := make(map[roachpb.ReplicaID]struct{})
	for replID, progress := range m.progress {
		if progress.State == tracker.StateReplicate {
			continue
		}
		behindFollowers[replID] = struct{}{}
	}
	return behindFollowers
}

func (m *mockReplicaForFlowControl) getInactiveFollowers() map[roachpb.ReplicaID]struct{} {
	return m.inactive
}

func (m *mockReplicaForFlowControl) getDisconnectedFollowers() map[roachpb.ReplicaID]struct{} {
	return m.disconnected
}

func (m *mockReplicaForFlowControl) getAppliedLogPosition() kvflowcontrolpb.RaftLogPosition {
	return m.applied
}

func (m *mockReplicaForFlowControl) isScratchRange() bool {
	return false
}

func (m *mockReplicaForFlowControl) getReplicaDescriptor() roachpb.ReplicaDescriptor {
	return roachpb.ReplicaDescriptor{
		ReplicaID: m.replicaID,
		NodeID:    roachpb.NodeID(m.replicaID),
		StoreID:   roachpb.StoreID(m.replicaID),
		Type:      roachpb.VOTER_FULL,
	}
}

type mockFlowHandleFactory struct {
	t      *testing.T
	logger *testLogger
}

var _ kvflowcontrol.HandleFactory = &mockFlowHandleFactory{}

func newMockFlowHandleFactory(t *testing.T, logger *testLogger) *mockFlowHandleFactory {
	return &mockFlowHandleFactory{
		t:      t,
		logger: logger,
	}
}

// NewHandle implements the kvflowcontrol.HandleFactory interface.
func (m *mockFlowHandleFactory) NewHandle(
	rangeID roachpb.RangeID, tenantID roachpb.TenantID,
) kvflowcontrol.Handle {
	return newMockFlowHandle(m.t, rangeID, tenantID, m.logger)
}

type mockFlowHandle struct {
	t        *testing.T
	logger   *testLogger
	rangeID  roachpb.RangeID
	tenantID roachpb.TenantID
}

var _ kvflowcontrol.Handle = &mockFlowHandle{}

func newMockFlowHandle(
	t *testing.T, rangeID roachpb.RangeID, tenantID roachpb.TenantID, logger *testLogger,
) *mockFlowHandle {
	m := &mockFlowHandle{
		t:        t,
		logger:   logger,
		rangeID:  rangeID,
		tenantID: tenantID,
	}
	m.logger.log(fmt.Sprintf("initialized flow control handle for r%s/t%d",
		m.rangeID, m.tenantID.ToUint64()))
	return m
}

func (m *mockFlowHandle) Admit(
	ctx context.Context, pri admissionpb.WorkPriority, ct time.Time,
) (bool, error) {
	m.t.Fatal("unimplemented")
	return false, nil
}

func (m *mockFlowHandle) DeductTokensFor(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	pos kvflowcontrolpb.RaftLogPosition,
	tokens kvflowcontrol.Tokens,
) {
	m.t.Fatal("unimplemented")
}

func (m *mockFlowHandle) ReturnTokensUpto(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	pos kvflowcontrolpb.RaftLogPosition,
	stream kvflowcontrol.Stream,
) {
	m.t.Fatal("unimplemented")
}

func (m *mockFlowHandle) ConnectStream(
	ctx context.Context, pos kvflowcontrolpb.RaftLogPosition, stream kvflowcontrol.Stream,
) {
	m.logger.log(fmt.Sprintf("connected to replication stream %s starting at %s", stream, pos))
}

func (m *mockFlowHandle) DisconnectStream(ctx context.Context, stream kvflowcontrol.Stream) {
	m.logger.log(fmt.Sprintf("disconnected from replication stream %s", stream))
}

func (m *mockFlowHandle) ResetStreams(ctx context.Context) {
	m.logger.log("reset all replication streams")
}

func (m *mockFlowHandle) Inspect(ctx context.Context) kvflowinspectpb.Handle {
	m.t.Fatal("unimplemented")
	return kvflowinspectpb.Handle{}
}

func (m *mockFlowHandle) Close(ctx context.Context) {
	m.logger.log(fmt.Sprintf("closed flow control handle for r%s/t%d",
		m.rangeID, m.tenantID.ToUint64()))
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

type testLogger struct {
	buffer *strings.Builder
}

func newTestLogger() *testLogger {
	return &testLogger{
		buffer: &strings.Builder{},
	}
}

func (l *testLogger) log(s string) {
	l.buffer.WriteString(fmt.Sprintf("%s\n", s))
}

func (l *testLogger) output() string {
	output := l.buffer.String()
	l.buffer.Reset()
	return output
}
