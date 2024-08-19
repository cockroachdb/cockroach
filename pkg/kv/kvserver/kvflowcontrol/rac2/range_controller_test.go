// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

type testingRCEval struct {
	done      bool
	err       error
	cancel    context.CancelFunc
	refreshCh chan struct{}
}

type testingRCRange struct {
	rc    *rangeController
	r     testingRange
	evals map[string]*testingRCEval
}

func (r *testingRCRange) FollowerState(replicaID roachpb.ReplicaID) FollowerStateInfo {
	replica, ok := r.r.replicaSet[replicaID]
	if !ok {
		return FollowerStateInfo{}
	}
	return replica.info
}

func (r *testingRCRange) startWaitForEval(name string, pri admissionpb.WorkPriority) {
	ctx, cancel := context.WithCancel(context.Background())
	refreshCh := make(chan struct{})
	r.evals[name] = &testingRCEval{
		err:       nil,
		cancel:    cancel,
		refreshCh: refreshCh,
	}

	go func() {
		err := r.rc.WaitForEval(ctx, pri)
		r.evals[name].err = err
		r.evals[name].done = true
	}()
}

type testingRange struct {
	rangeID        roachpb.RangeID
	tenantID       roachpb.TenantID
	localReplicaID roachpb.ReplicaID
	replicaSet     map[roachpb.ReplicaID]testingReplica
}

func (t testingRange) replicas() ReplicaSet {
	replicas := make(ReplicaSet, len(t.replicaSet))
	for i, replica := range t.replicaSet {
		replicas[i] = replica.desc
	}
	return replicas
}

const invalidTrackerState = tracker.StateSnapshot + 1

type testingReplica struct {
	desc roachpb.ReplicaDescriptor
	info FollowerStateInfo
}

func scanRanges(t *testing.T, input string) []testingRange {
	replicas := []testingRange{}

	for _, line := range strings.Split(input, "\n") {
		parts := strings.Fields(line)
		parts[0] = strings.TrimSpace(parts[0])
		if strings.HasPrefix(parts[0], "range_id=") {
			// Create a new range, any replicas which follow until the next range_id
			// line will be added to this replica set.
			var rangeID, tenantID, localReplicaID int
			var err error

			require.True(t, strings.HasPrefix(parts[0], "range_id="))
			parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "range_id=")
			rangeID, err = strconv.Atoi(parts[0])
			require.NoError(t, err)

			parts[1] = strings.TrimSpace(parts[1])
			require.True(t, strings.HasPrefix(parts[1], "tenant_id="))
			parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "tenant_id=")
			tenantID, err = strconv.Atoi(parts[1])
			require.NoError(t, err)

			parts[2] = strings.TrimSpace(parts[2])
			require.True(t, strings.HasPrefix(parts[2], "local_replica_id="))
			parts[2] = strings.TrimPrefix(strings.TrimSpace(parts[2]), "local_replica_id=")
			localReplicaID, err = strconv.Atoi(parts[2])
			require.NoError(t, err)

			replicas = append(replicas, testingRange{
				rangeID:        roachpb.RangeID(rangeID),
				tenantID:       roachpb.MustMakeTenantID(uint64(tenantID)),
				localReplicaID: roachpb.ReplicaID(localReplicaID),
				replicaSet:     make(map[roachpb.ReplicaID]testingReplica),
			})
		} else {
			// Otherwise, add the replica to the last replica set created.
			replica := scanReplica(t, line)
			replicas[len(replicas)-1].replicaSet[replica.desc.ReplicaID] = replica
		}
	}

	return replicas
}

func scanReplica(t *testing.T, line string) testingReplica {
	var storeID, replicaID int
	var replicaType roachpb.ReplicaType
	// Default to an invalid state when no state is specified, this will be
	// converted to the prior state or StateReplicate if the replica doesn't yet
	// exist.
	var state tracker.StateType = invalidTrackerState
	var err error

	parts := strings.Fields(line)
	parts[0] = strings.TrimSpace(parts[0])

	require.True(t, strings.HasPrefix(parts[0], "store_id="))
	parts[0] = strings.TrimPrefix(strings.TrimSpace(parts[0]), "store_id=")
	storeID, err = strconv.Atoi(parts[0])
	require.NoError(t, err)

	parts[1] = strings.TrimSpace(parts[1])
	require.True(t, strings.HasPrefix(parts[1], "replica_id="))
	parts[1] = strings.TrimPrefix(strings.TrimSpace(parts[1]), "replica_id=")
	replicaID, err = strconv.Atoi(parts[1])
	require.NoError(t, err)

	parts[2] = strings.TrimSpace(parts[2])
	require.True(t, strings.HasPrefix(parts[2], "type="))
	parts[2] = strings.TrimPrefix(strings.TrimSpace(parts[2]), "type=")
	switch parts[2] {
	case "VOTER_FULL":
		replicaType = roachpb.VOTER_FULL
	case "VOTER_INCOMING":
		replicaType = roachpb.VOTER_INCOMING
	case "VOTER_DEMOTING_LEARNER":
		replicaType = roachpb.VOTER_DEMOTING_LEARNER
	case "LEARNER":
		replicaType = roachpb.LEARNER
	case "NON_VOTER":
		replicaType = roachpb.NON_VOTER
	case "VOTER_DEMOTING_NON_VOTER":
		replicaType = roachpb.VOTER_DEMOTING_NON_VOTER
	default:
		panic("unknown replica type")
	}

	// The fourth field is optional, if set it contains the tracker state of the
	// replica on the leader replica (localReplicaID). The valid states are
	// Probe, Replicate, and Snapshot.
	if len(parts) > 3 {
		parts[3] = strings.TrimSpace(parts[3])
		require.True(t, strings.HasPrefix(parts[3], "state="))
		parts[3] = strings.TrimPrefix(strings.TrimSpace(parts[3]), "state=")
		switch parts[3] {
		case "StateProbe":
			state = tracker.StateProbe
		case "StateReplicate":
			state = tracker.StateReplicate
		case "StateSnapshot":
			state = tracker.StateSnapshot
		default:
			panic("unknown replica state")
		}
	}

	return testingReplica{
		desc: roachpb.ReplicaDescriptor{
			NodeID:    roachpb.NodeID(storeID),
			StoreID:   roachpb.StoreID(storeID),
			ReplicaID: roachpb.ReplicaID(replicaID),
			Type:      replicaType,
		},
		info: FollowerStateInfo{State: state},
	}
}

func scanPriority(t *testing.T, input string) admissionpb.WorkPriority {
	require.True(t, strings.HasPrefix(input, "pri="))
	input = strings.TrimPrefix(strings.TrimSpace(input), "pri=")
	return parsePriority(t, input)
}

func parsePriority(t *testing.T, input string) admissionpb.WorkPriority {
	switch input {
	case "LowPri":
		return admissionpb.LowPri
	case "NormalPri":
		return admissionpb.NormalPri
	case "HighPri":
		return admissionpb.HighPri
	default:
		require.Fail(t, "unknown work class")
		return admissionpb.WorkPriority(-1)
	}
}

// TODO(kvoli):
// - Add stringer method for range state
// - Extend dd test file
// - Make adjusting tokens multi-variable
func TestRangeControllerWaitForEval(t *testing.T) {
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	ranges := make(map[roachpb.RangeID]*testingRCRange)
	ssTokenCounter := NewStreamTokenCounterProvider(settings)

	tokenCountsString := func() string {
		var b strings.Builder
		for stream, tc := range ssTokenCounter.mu.evalCounters {
			fmt.Fprintf(&b, "%v: %v\n", stream, tc)
		}
		return b.String()
	}

	evalStateString := func() string {
		var b strings.Builder
		for _, testRC := range ranges {
			fmt.Fprintf(&b, "range_id=%d tenant_id=%d local_replica_id=%d\n", testRC.r.rangeID, testRC.r.tenantID, testRC.r.localReplicaID)
			for name, eval := range testRC.evals {
				fmt.Fprintf(&b, "  eval_id=%s done=%t err=%v\n", name, eval.done, eval.err)
			}
		}
		return b.String()
	}

	datadriven.RunTest(t, "testdata/range_controller_wait_for_eval", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			for _, r := range scanRanges(t, d.Input) {
				testRC := &testingRCRange{
					r:     r,
					evals: map[string]*testingRCEval{},
				}
				options := RangeControllerOptions{
					RangeID:        r.rangeID,
					TenantID:       r.tenantID,
					LocalReplicaID: r.localReplicaID,
					SSTokenCounter: ssTokenCounter,
					RaftInterface:  testRC,
				}

				init := RangeControllerInitState{
					ReplicaSet:  r.replicas(),
					Leaseholder: r.localReplicaID,
				}
				testRC.rc = NewRangeController(ctx, options, init)
				ranges[r.rangeID] = testRC
			}
			return ""

		case "wait_for_eval":
			var rangeID int
			var evalID, priString string
			d.ScanArgs(t, "range_id", &rangeID)
			d.ScanArgs(t, "eval_id", &evalID)
			d.ScanArgs(t, "pri", &priString)
			testRC := ranges[roachpb.RangeID(rangeID)]
			testRC.startWaitForEval(evalID, parsePriority(t, priString))
			return evalStateString()

		case "check_state":
			return evalStateString()

		case "adjust_tokens":
			var store int
			var priString string
			var tokens int
			d.ScanArgs(t, "store", &store)
			d.ScanArgs(t, "pri", &priString)
			d.ScanArgs(t, "tokens", &tokens)

			stream := kvflowcontrol.Stream{
				StoreID:  roachpb.StoreID(store),
				TenantID: roachpb.SystemTenantID,
			}

			tc := ssTokenCounter.Eval(stream)
			tc.(*tokenCounter).adjust(ctx,
				admissionpb.WorkClassFromPri(parsePriority(t, priString)),
				kvflowcontrol.Tokens(tokens))
			return tokenCountsString()

		case "cancel_context":
			var rangeID int
			var evalID string

			d.ScanArgs(t, "range_id", &rangeID)
			d.ScanArgs(t, "eval_id", &evalID)
			testRC := ranges[roachpb.RangeID(rangeID)]
			testRC.evals[evalID].cancel()
			return evalStateString()

		case "set_replicas":
			for _, r := range scanRanges(t, d.Input) {
				testRC := ranges[r.rangeID]
				testRC.r = r
				testRC.rc.SetReplicasRaftMuLocked(ctx, r.replicas())
			}

			return ""

		case "set_leaseholder":
			var rangeID, replicaID int
			d.ScanArgs(t, "range_id", &rangeID)
			d.ScanArgs(t, "replica_id", &replicaID)
			testRC := ranges[roachpb.RangeID(rangeID)]
			testRC.rc.SetLeaseholderRaftMuLocked(ctx, roachpb.ReplicaID(replicaID))

			return ""

		default:
			panic(fmt.Sprintf("unknown command: %s", d.Cmd))
		}
	})
}
