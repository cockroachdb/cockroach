// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package workload provides an abstraction for generators of sql query loads
// (and requisite initial data) as well as tools for working with these
// generators.

package storagebase

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// StoreTestingKnobs is a part of the context used to control parts of
// the system. The Testing*Filter functions are called at various
// points in the request pipeline if they are non-nil. These can be
// used either for synchronization (e.g. to write to a channel when a
// particular point is reached) or to change the behavior by returning
// an error (which aborts all further processing for the command).
type StoreTestingKnobs struct {
	EvalKnobs BatchEvalTestingKnobs

	// TestingRequestFilter is called before evaluating each command on a
	// replica. The filter is run before the request is added to the
	// CommandQueue, so blocking in the filter will not block interfering
	// requests. If it returns an error, the command will not be evaluated.
	TestingRequestFilter ReplicaRequestFilter

	// TestingProposalFilter is called before proposing each command.
	TestingProposalFilter ReplicaProposalFilter

	// TestingApplyFilter is called before applying the results of a
	// command on each replica. If it returns an error, the command will
	// not be applied. If it returns an error on some replicas but not
	// others, the behavior is poorly defined unless that error is a
	// ReplicaCorruptionError.
	TestingApplyFilter ReplicaApplyFilter

	// TestingPostApplyFilter is called after a command is applied to
	// rocksdb but before in-memory side effects have been processed.
	TestingPostApplyFilter ReplicaApplyFilter

	// TestingResponseFilter is called after the replica processes a
	// command in order for unittests to modify the batch response,
	// error returned to the client, or to simulate network failures.
	TestingResponseFilter ReplicaResponseFilter

	// Disables the use of optional one phase commits. Even when enabled, requests
	// that set the Require1PC flag are permitted to use one phase commits. This
	// prevents wedging node liveness, which requires one phase commits during
	// liveness updates.
	DisableOptional1PC bool
	// A hack to manipulate the clock before sending a batch request to a replica.
	// TODO(kaneda): This hook is not encouraged to use. Get rid of it once
	// we make TestServer take a ManualClock.
	ClockBeforeSend func(*hlc.Clock, roachpb.BatchRequest)
	// OnCommandQueueAction is called when the BatchRequest performs an action
	// on the CommandQueue.
	OnCommandQueueAction func(*roachpb.BatchRequest, CommandQueueAction)
	// MaxOffset, if set, overrides the server clock's MaxOffset at server
	// creation time.
	// See also DisableMaxOffsetCheck.
	MaxOffset time.Duration
	// DisableMaxOffsetCheck disables the rejection (in Store.Send) of requests
	// with the timestamp too much in the future. Normally, this rejection is a
	// good sanity check, but certain tests unfortunately insert a "message from
	// the future" into the system to advance the clock of a TestServer. We
	// should get rid of such practices once we make TestServer take a
	// ManualClock.
	DisableMaxOffsetCheck bool
	// DontPreventUseOfOldLeaseOnStart disables the initialization of
	// replica.mu.minLeaseProposedTS on replica.Init(). This has the effect of
	// allowing the replica to use the lease that it had in a previous life (in
	// case the tests persisted the engine used in said previous life).
	DontPreventUseOfOldLeaseOnStart bool
	// DisableAutomaticLeaseRenewal enables turning off the background worker
	// that attempts to automatically renew expiration-based leases.
	DisableAutomaticLeaseRenewal bool
	// LeaseRequestEvent, if set, is called when replica.requestLeaseLocked() is
	// called to acquire a new lease. This can be used to assert that a request
	// triggers a lease acquisition.
	LeaseRequestEvent func(ts hlc.Timestamp)
	// LeaseTransferBlockedOnExtensionEvent, if set, is called when
	// replica.TransferLease() encounters an in-progress lease extension.
	// nextLeader is the replica that we're trying to transfer the lease to.
	LeaseTransferBlockedOnExtensionEvent func(nextLeader roachpb.ReplicaDescriptor)
	// DisableGCQueue disables the GC queue.
	DisableGCQueue bool
	// DisableMergeQueue disables the merge queue.
	DisableMergeQueue bool
	// DisableReplicaGCQueue disables the replica GC queue.
	DisableReplicaGCQueue bool
	// DisableReplicateQueue disables the replication queue.
	DisableReplicateQueue bool
	// DisableReplicaRebalancing disables rebalancing of replicas but otherwise
	// leaves the replicate queue operational.
	DisableReplicaRebalancing bool
	// DisableSplitQueue disables the split queue.
	DisableSplitQueue bool
	// DisableTimeSeriesMaintenanceQueue disables the time series maintenance
	// queue.
	DisableTimeSeriesMaintenanceQueue bool
	// DisableRaftSnapshotQueue disables the raft snapshot queue.
	DisableRaftSnapshotQueue bool
	// DisableConsistencyQueue disables the consistency checker.
	DisableConsistencyQueue bool
	// DisableScanner disables the replica scanner.
	DisableScanner bool
	// DisablePeriodicGossips disables periodic gossiping.
	DisablePeriodicGossips bool
	// DisableLeaderFollowsLeaseholder disables attempts to transfer raft
	// leadership when it diverges from the range's leaseholder.
	DisableLeaderFollowsLeaseholder bool
	// DisableRefreshReasonNewLeader disables refreshing pending commands when a new
	// leader is discovered.
	DisableRefreshReasonNewLeader bool
	// DisableRefreshReasonNewLeaderOrConfigChange disables refreshing pending
	// commands when a new leader is discovered or when a config change is
	// dropped.
	DisableRefreshReasonNewLeaderOrConfigChange bool
	// DisableRefreshReasonTicks disables refreshing pending commands when a
	// snapshot is applied.
	DisableRefreshReasonSnapshotApplied bool
	// DisableRefreshReasonTicks disables refreshing pending commands
	// periodically.
	DisableRefreshReasonTicks bool
	// RefreshReasonTicksPeriod overrides the default period over which
	// pending commands are refreshed. The period is specified as a multiple
	// of Raft group ticks.
	RefreshReasonTicksPeriod int
	// DisableProcessRaft disables the process raft loop.
	DisableProcessRaft bool
	// DisableLastProcessedCheck disables checking on replica queue last processed times.
	DisableLastProcessedCheck bool
	// ReplicateQueueAcceptsUnsplit allows the replication queue to
	// process ranges that need to be split, for use in tests that use
	// the replication queue but disable the split queue.
	ReplicateQueueAcceptsUnsplit bool
	// SplitQueuePurgatoryChan allows a test to control the channel used to
	// trigger split queue purgatory processing.
	SplitQueuePurgatoryChan <-chan time.Time
	// SkipMinSizeCheck, if set, makes the store creation process skip the check
	// for a minimum size.
	SkipMinSizeCheck bool
	// DisableAsyncIntentResolution disables the async intent resolution
	// path (but leaves synchronous resolution). This can avoid some
	// edge cases in tests that start and stop servers.
	DisableAsyncIntentResolution bool
	// ForceSyncIntentResolution forces all asynchronous intent resolution to be
	// performed synchronously. It is equivalent to setting IntentResolverTaskLimit
	// to -1.
	ForceSyncIntentResolution bool
	// DisableLeaseCapacityGossip disables the ability of a changing number of
	// leases to trigger the store to gossip its capacity. With this enabled,
	// only changes in the number of replicas can cause the store to gossip its
	// capacity.
	DisableLeaseCapacityGossip bool
	// BootstrapVersion overrides the version the stores will be bootstrapped with.
	BootstrapVersion *cluster.ClusterVersion
	// SystemLogsGCPeriod is used to override the period of GC of system logs.
	SystemLogsGCPeriod time.Duration
	// SystemLogsGCGCDone is used to notify when system logs GC is done.
	SystemLogsGCGCDone chan<- struct{}
}

var _ base.ModuleTestingKnobs = &StoreTestingKnobs{}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*StoreTestingKnobs) ModuleTestingKnobs() {}

// BatchEvalTestingKnobs contains testing helpers that are used during batch evaluation.
type BatchEvalTestingKnobs struct {
	// TestingEvalFilter is called before evaluating each command. The
	// number of times this callback is run depends on the propEvalKV
	// setting, and it is therefore deprecated in favor of either
	// TestingProposalFilter (which runs only on the lease holder) or
	// TestingApplyFilter (which runs on each replica). If your filter is
	// not idempotent, consider wrapping it in a
	// ReplayProtectionFilterWrapper.
	// TODO(bdarnell,tschottdorf): Migrate existing tests which use this
	// to one of the other filters. See #10493
	// TODO(andrei): Provide guidance on what to use instead for trapping reads.
	TestingEvalFilter ReplicaCommandFilter
	// NumKeysEvaluatedForRangeIntentResolution is set by the stores to the
	// number of keys evaluated for range intent resolution.
	NumKeysEvaluatedForRangeIntentResolution *int64
	// DisallowUnsequencedTransactionalWrites enables an assertion that all
	// transactional writes include Request-scoped sequence numbers. This
	// assertion is not safe to enable by default, because it would trigger
	// in mixed-version clusters. However, it is useful in testing to ensure
	// that tests properly assign these sequence numbers.
	//
	// TODO(nvanbenschoten): Remove this testing knob in 2.2. The corresponding
	// assertion can be performed unconditionally.
	DisallowUnsequencedTransactionalWrites bool
}
