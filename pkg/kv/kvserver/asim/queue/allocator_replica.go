// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SimulatorReplica is a replica that is being tracked as a potential candidate
// for rebalancing activities. It maintains a set of methods that enable
// querying its state and processing a rebalancing action if taken.
type SimulatorReplica struct {
	rng   state.Range
	repl  state.Replica
	usage allocator.RangeUsageInfo
	state state.State
}

var _ plan.AllocatorReplica = &SimulatorReplica{}

// NewSimulatorReplica returns a new SimulatorReplica which implements the
// plan.AllocatorReplica interface used for replication planning.
func NewSimulatorReplica(repl state.Replica, s state.State) *SimulatorReplica {
	rng, ok := s.Range(repl.Range())
	if !ok {
		return nil
	}
	sr := &SimulatorReplica{
		rng:   rng,
		repl:  repl,
		usage: s.RangeUsageInfo(repl.Range(), repl.StoreID()),
		state: s,
	}
	return sr
}

func (sr *SimulatorReplica) HasCorrectLeaseType(lease roachpb.Lease) bool {
	return true
}

// LeaseStatusAt returns the status of the current lease for the
// timestamp given.
//
// Common operations to perform on the resulting status are to check if
// it is valid using the IsValid method and to check whether the lease
// is held locally using the OwnedBy method.
//
// Note that this method does not check to see if a transfer is pending,
// but returns the status of the current lease and ownership at the
// specified point in time.
func (sr *SimulatorReplica) LeaseStatusAt(
	ctx context.Context, now hlc.ClockTimestamp,
) kvserverpb.LeaseStatus {
	return kvserverpb.LeaseStatus{
		Lease: roachpb.Lease{
			Replica: sr.repl.Descriptor(),
		},
		State: kvserverpb.LeaseState_VALID,
	}
}

// LeaseViolatesPreferences checks if current replica owns the lease and if it
// violates the lease preferences defined in the span config. If there is an
// error or no preferences defined then it will return false and consider that
// to be in-conformance.
func (sr *SimulatorReplica) LeaseViolatesPreferences(
	_ context.Context, conf *roachpb.SpanConfig,
) bool {
	descs := sr.state.StoreDescriptors(true /* useCached */, sr.repl.StoreID())
	if len(descs) != 1 {
		panic(fmt.Sprintf("programming error: cannot get  store descriptor for store %d", sr.repl.StoreID()))
	}
	storeDesc := descs[0]

	if len(conf.LeasePreferences) == 0 {
		return false
	}
	for _, preference := range conf.LeasePreferences {
		if constraint.CheckStoreConjunction(storeDesc, preference.Constraints) {
			return false
		}
	}
	// We have at lease one preference set up, but we don't satisfy any.
	return true
}

func (sr *SimulatorReplica) LastReplicaAdded() (roachpb.ReplicaID, time.Time) {
	// We return a hack here, using the next replica ID from the descriptor and
	// the current time. This is used when removing a replica to provide a grace
	// period for new replicas. The corresponding code in plan.findRemoveVoter
	// uses the system wall clock and we avoid that code path for now by always
	// finding a remove voter without retries.
	// TODO(kvoli): Record the actual time the last replica was added and rip out
	// the timeutil.Now() usage in plan.findRemoveVoter, instead passing in now()
	// so it maps to the simulated time.
	return sr.Desc().NextReplicaID - 1, timeutil.Now()
}

// OwnsValidLease returns whether this replica is the current valid
// leaseholder.
func (sr *SimulatorReplica) OwnsValidLease(context.Context, hlc.ClockTimestamp) bool {
	return sr.repl.HoldsLease()
}

// StoreID returns the Replica's StoreID.
func (sr *SimulatorReplica) StoreID() roachpb.StoreID {
	return roachpb.StoreID(sr.repl.StoreID())
}

// GetRangeID returns the Range ID.
func (sr *SimulatorReplica) GetRangeID() roachpb.RangeID {
	return roachpb.RangeID(sr.repl.Range())
}

// RaftStatus returns the current raft status of the replica. It returns
// nil if the Raft group has not been initialized yet.
func (sr *SimulatorReplica) RaftStatus() *raft.Status {
	return sr.state.RaftStatus(sr.rng.RangeID(), sr.repl.StoreID())
}

// GetFirstIndex returns the index of the first entry in the replica's Raft
// log.
func (sr *SimulatorReplica) GetFirstIndex() kvpb.RaftIndex {
	// TODO(kvoli): We always return 2 here as RaftStatus is unimplemented. When
	// it is implemented, this may become variable.
	return 2
}

func (sr *SimulatorReplica) SpanConfig() (*roachpb.SpanConfig, error) {
	return sr.rng.SpanConfig(), nil
}

// Desc returns the authoritative range descriptor, acquiring a replica lock in
// the process.
func (sr *SimulatorReplica) Desc() *roachpb.RangeDescriptor {
	return sr.rng.Descriptor()
}

// RangeUsageInfo returns usage information (sizes and traffic) needed by
// the allocator to make rebalancing decisions for a given range.
func (sr *SimulatorReplica) RangeUsageInfo() allocator.RangeUsageInfo {
	return sr.usage
}

func (sr *SimulatorReplica) SendStreamStats(stats *rac2.RangeSendStreamStats) {
}
