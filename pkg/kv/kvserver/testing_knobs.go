// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tenantrate"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StoreTestingKnobs is a part of the context used to control parts of
// the system. The Testing*Filter functions are called at various
// points in the request pipeline if they are non-nil. These can be
// used either for synchronization (e.g. to write to a channel when a
// particular point is reached) or to change the behavior by returning
// an error (which aborts all further processing for the command).
type StoreTestingKnobs struct {
	EvalKnobs               kvserverbase.BatchEvalTestingKnobs
	IntentResolverKnobs     kvserverbase.IntentResolverTestingKnobs
	TxnWaitKnobs            txnwait.TestingKnobs
	ConsistencyTestingKnobs ConsistencyTestingKnobs
	TenantRateKnobs         tenantrate.TestingKnobs
	EngineKnobs             []storage.ConfigOption
	AllocatorKnobs          *allocator.TestingKnobs
	GossipTestingKnobs      StoreGossipTestingKnobs
	ReplicaPlannerKnobs     plan.ReplicaPlannerTestingKnobs

	// TestingRequestFilter is called before evaluating each request on a
	// replica. The filter is run before the request acquires latches, so
	// blocking in the filter will not block interfering requests. If it
	// returns an error, the command will not be evaluated.
	TestingRequestFilter kvserverbase.ReplicaRequestFilter

	// TestingConcurrencyRetryFilter is called before a concurrency retry error is
	// handled and the batch is retried.
	TestingConcurrencyRetryFilter kvserverbase.ReplicaConcurrencyRetryFilter

	// TestingProposalFilter is called before proposing each command.
	TestingProposalFilter kvserverbase.ReplicaProposalFilter
	// TestingProposalSubmitFilter can be used by tests to observe and optionally
	// drop Raft proposals before they are handed to etcd/raft to begin the
	// process of replication. Dropped proposals are still eligible to be
	// reproposed due to ticks.
	TestingProposalSubmitFilter func(*ProposalData) (drop bool, err error)

	// TestingApplyCalledTwiceFilter is called before applying the results of a command on
	// each replica assuming the command was cleared for application (i.e. no
	// forced error occurred; the supplied AppliedFilterArgs will have a nil
	// ForcedError field). If this function returns an error, it is treated as
	// a forced error and the command will not be applied. If it returns an error
	// on some replicas but not others, the behavior is poorly defined. The
	// returned int is interpreted as a proposalReevaluationReason.
	//
	// Users have to expect the filter to be invoked twice for each command, once
	// from ephemerealReplicaAppBatch.Stage, and once from replicaAppBatch.Stage;
	// this has to do with wanting to early-ack successful proposals. The second
	// call is conditional on the first call succeeding.
	//
	// Consider using a TestPostApplyFilter instead, and use a
	// TestingApplyCalledTwiceFilter only to inject forced errors.
	TestingApplyCalledTwiceFilter kvserverbase.ReplicaApplyFilter
	// TestingApplyForcedErrFilter is like TestingApplyCalledTwiceFilter, but it
	// is only invoked when there is a pre-existing forced error (and in
	// particular, it will be invoked only once per command). The returned int and
	// *Error replace the existing proposalReevaluationReason (if initially zero
	// only) and forced error.
	TestingApplyForcedErrFilter kvserverbase.ReplicaApplyFilter

	// TestingPostApplyFilter is called after a command is applied to
	// rocksdb but before in-memory side effects have been processed.
	// It is only called on the replica the proposed the command.
	//
	// Filters need to handle the case in which the command applies
	// with a forced error. That is, the "command" will apply as a
	// no-op write, and the ForcedError field will be set.
	TestingPostApplyFilter kvserverbase.ReplicaApplyFilter

	// TestingResponseErrorEvent is called when an error is returned applying
	// a command.
	TestingResponseErrorEvent func(context.Context, *kvpb.BatchRequest, error)

	// TestingResponseFilter is called after the replica processes a
	// command in order for unittests to modify the batch response,
	// error returned to the client, or to simulate network failures.
	TestingResponseFilter kvserverbase.ReplicaResponseFilter

	// SlowReplicationThresholdOverride is an interceptor that allows setting a
	// per-Batch SlowReplicationThreshold.
	SlowReplicationThresholdOverride func(ba *kvpb.BatchRequest) time.Duration

	// TestingRangefeedFilter is called before a replica processes a rangefeed
	// in order for unit tests to modify the request, error returned to the client
	// or data.
	TestingRangefeedFilter kvserverbase.ReplicaRangefeedFilter

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
	// DisableAutomaticLeaseRenewal disables eager renewal of expiration-based
	// leases.
	DisableAutomaticLeaseRenewal bool
	// LeaseRequestEvent, if set, is called when replica.requestLeaseLocked() is
	// called to acquire a new lease. This can be used to assert that a request
	// triggers a lease acquisition.
	LeaseRequestEvent func(ts hlc.Timestamp, storeID roachpb.StoreID, rangeID roachpb.RangeID) *kvpb.Error
	// PinnedLeases can be used to prevent all but one store from acquiring leases on a given range.
	PinnedLeases *PinnedLeasesKnob
	// LeaseTransferBlockedOnExtensionEvent, if set, is called when
	// replica.TransferLease() encounters an in-progress lease extension.
	// nextLeader is the replica that we're trying to transfer the lease to.
	LeaseTransferBlockedOnExtensionEvent func(nextLeader roachpb.ReplicaDescriptor)
	// DisableGCQueue disables the GC queue.
	DisableGCQueue bool
	// DisableMergeQueue disables the merge queue.
	DisableMergeQueue bool
	// DisableReplicateQueue disables the raft log queue.
	DisableRaftLogQueue bool
	// DisableReplicaGCQueue disables the replica GC queue.
	DisableReplicaGCQueue bool
	// DisableReplicateQueue disables the replication queue.
	DisableReplicateQueue bool
	// DisableLoadBasedSplitting turns off LBS so no splits happen because of load.
	DisableLoadBasedSplitting bool
	// LoadBasedSplittingOverrideKey returns a key which should be used for load
	// based splitting, overriding any value returned from the real load based
	// splitter.
	LoadBasedSplittingOverrideKey func(rangeID roachpb.RangeID) (splitKey roachpb.Key, useSplitKey bool)
	// DisableSplitQueue disables the split queue.
	DisableSplitQueue bool
	// DisableTimeSeriesMaintenanceQueue disables the time series maintenance
	// queue.
	DisableTimeSeriesMaintenanceQueue bool
	// DisableRaftSnapshotQueue disables the raft snapshot queue. Use this
	// sparingly, as it tends to produce flaky tests during up-replication where
	// the explicit learner snapshot gets lost. Since uninitialized replicas
	// reject the raft leader's MsgApp with a rejection that hints at index zero,
	// Raft will always want to send another snapshot if it gets to talk to the
	// follower before it applied the snapshot. In the common case, the mechanism
	// we have to suppress raft snapshots while an explicit snapshot is in flight
	// avoids the duplication of work. However, in the uncommon case, the raft
	// leader is on a different store and so no suppression takes place. This
	// duplication is avoided by turning the raft snapshot queue off, but
	// unfortunately there are certain conditions under which the explicit
	// snapshot may fail to produce the desired result of an initialized follower
	// which the leader will append to.
	//
	// For example, if there is a term change between when the snapshot is
	// constructed and received, the follower will silently drop the snapshot
	// without signaling an error to the sender, i.e. the leader. (To work around
	// that, once could adjust the term for the message in that case in
	// `(*Replica).stepRaftGroup` to prevent the message from getting dropped).
	//
	// Another problem is that the snapshot may apply but fail to inform the
	// leader of this fact. Once a Raft leader has determined that a follower
	// needs a snapshot, it will keep asking for a snapshot to be sent until a) a
	// call to .ReportSnapshotStatus acks that a snapshot got applied, or b) an
	// MsgAppResp is received from from the follower acknowledging an index
	// *greater than or equal to the PendingSnapshotIndex* it tracks for that
	// follower (such an MsgAppResp is emitted in response to applying the
	// snapshot), whichever occurs first.
	//
	//
	// However, neither may happen despite the snapshot applying successfully. The
	// call to `ReportSnapshotStatus` may not occur on the correct Replica, since
	// Raft leader can change during replication operations, and MsgAppResp can
	// get dropped. Even if the MsgAppResp does get to the leader, the contained
	// index (which is the index of the snapshot that got applied) may be less
	// than the index at which the leader asked for a snapshot (since the applied
	// snapshot is an external snapshot, and may have been initiated before the
	// raft leader even wanted a snapshot) - again leaving the leader to continue
	// asking for a snapshot. Lastly, even if we improved the raft code to accept
	// all snapshots that allow it to continue replicating the log, there may have
	// been a log truncation separating the snapshot from the log.
	//
	// Either way, the result in all of the rare cases above this is that an
	// additional snapshot must be sent from the Raft snapshot queue until the
	// leader will include the follower in log replication. It is thus ill-advised
	// to turn the snap queue off except when this is known to be a good idea.
	//
	// An example of a test that becomes flaky with this set to false is
	// TestReportUnreachableRemoveRace, though it needs a 20-node roachprod-stress
	// cluster to reliably reproduce this within a few minutes.
	DisableRaftSnapshotQueue bool
	// DisableConsistencyQueue disables the consistency checker.
	DisableConsistencyQueue bool
	// DisableScanner disables the replica scanner.
	DisableScanner bool
	// DisableQuiescence disables replica quiescence. This can also be
	// set via COCKROACH_DISABLE_QUIESCENCE.
	DisableQuiescence bool
	// DisableLeaderFollowsLeaseholder disables attempts to transfer raft
	// leadership when it diverges from the range's leaseholder. This can
	// also be set via COCKROACH_DISABLE_LEADER_FOLLOWS_LEASEHOLDER.
	DisableLeaderFollowsLeaseholder bool
	// If set, the above-raft lease transfer safety checks (that verify that
	// we don't transfer leases to followers that need a snapshot, etc) are
	// disabled. The proposal-time checks are not affected by this knob.
	DisableAboveRaftLeaseTransferSafetyChecks bool
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
	// DisableEagerReplicaRemoval prevents the Replica from destroying itself
	// when it encounters a ChangeReplicasTrigger which would remove it or when
	// a ReplicaTooOldError in a RaftMessageResponse would lead to removal.
	// This option can lead to nasty cases during shutdown where a replica will
	// spin attempting to acquire a split or merge lock on a RHS which will
	// always fail and is generally not safe but is useful for testing.
	DisableEagerReplicaRemoval bool
	// RefreshReasonTicksPeriod overrides the default period over which
	// pending commands are refreshed. The period is specified as a multiple
	// of Raft group ticks.
	RefreshReasonTicksPeriod int
	// DisableProcessRaft disables the process raft loop.
	DisableProcessRaft func(roachpb.StoreID) bool
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
	// SystemLogsGCPeriod is used to override the period of GC of system logs.
	SystemLogsGCPeriod time.Duration
	// SystemLogsGCGCDone is used to notify when system logs GC is done.
	SystemLogsGCGCDone chan<- struct{}
	// DontPushOnWriteIntentError will propagate a write intent error immediately
	// instead of utilizing the intent resolver to try to push the corresponding
	// transaction.
	// TODO(nvanbenschoten): can we replace this knob with usage of the Error
	// WaitPolicy on BatchRequests?
	DontPushOnWriteIntentError bool
	// DontRetryPushTxnFailures will propagate a push txn failure immediately
	// instead of utilizing the txn wait queue to wait for the transaction to
	// finish or be pushed by a higher priority contender.
	DontRetryPushTxnFailures bool
	// DontRecoverIndeterminateCommits will propagate indeterminate commit
	// errors from failed txn pushes immediately instead of utilizing the txn
	// recovery manager to recovery from the indeterminate state.
	DontRecoverIndeterminateCommits bool
	// TraceAllRaftEvents enables raft event tracing even when the current
	// vmodule would not have enabled it.
	TraceAllRaftEvents bool
	// EnableUnconditionalRefreshesInRaftReady will always set the refresh reason
	// in handleRaftReady to refreshReasonNewLeaderOrConfigChange.
	EnableUnconditionalRefreshesInRaftReady bool

	// SendSnapshot is run after receiving a DelegateRaftSnapshot request but
	// before any throttling or sending logic.
	SendSnapshot func(*kvserverpb.DelegateSendSnapshotRequest)
	// ReceiveSnapshot is run after receiving a snapshot header but before
	// acquiring snapshot quota or doing shouldAcceptSnapshotData checks. If an
	// error is returned from the hook, it's sent as an ERROR SnapshotResponse.
	ReceiveSnapshot func(context.Context, *kvserverpb.SnapshotRequest_Header) error
	// HandleSnapshotDone is run after the entirety of receiving a snapshot,
	// regardless of whether it succeeds, gets cancelled, times out, or errors.
	HandleSnapshotDone func()
	// ReplicaAddSkipLearnerRollback causes replica addition to skip the learner
	// rollback that happens when either the initial snapshot or the promotion of
	// a learner to a voter fails.
	ReplicaAddSkipLearnerRollback func() bool
	// VoterAddStopAfterLearnerSnapshot causes voter addition to return early
	// if the func returns true. Specifically, after the learner txn is successful
	// and after the LEARNER type snapshot, but before promoting it to a voter.
	// This ensures the `*Replica` will be materialized on the Store when it
	// returns.
	VoterAddStopAfterLearnerSnapshot func([]roachpb.ReplicationTarget) bool
	// NonVoterAfterInitialization is called after a newly added non-voting
	// replica receives its initial snapshot. Note that this knob _can_ be used in
	// conjunction with ReplicaSkipInitialSnapshot.
	NonVoterAfterInitialization func()
	// ReplicaSkipInitialSnapshot causes snapshots to never be sent to learners or
	// non-voters if the func returns true. Adding replicas proceeds as usual,
	// though if an added voter has no prior state which can be caught up from the
	// raft log, the result will be an voter that is unable to participate in
	// quorum.
	ReplicaSkipInitialSnapshot func() bool
	// RaftSnapshotQueueSkipReplica causes the raft snapshot queue to skip sending
	// a snapshot to a follower replica.
	RaftSnapshotQueueSkipReplica func() bool
	// VoterAddStopAfterJointConfig causes voter addition to return early if
	// the func returns true. This happens before transitioning out of a joint
	// configuration, after the joint configuration has been entered by means
	// of a first ChangeReplicas transaction. If the replication change does
	// not use joint consensus, this early return is identical to the regular
	// return path.
	VoterAddStopAfterJointConfig func() bool
	// ReplicationAlwaysUseJointConfig causes replica addition to always go
	// through a joint configuration, even when this isn't necessary (because
	// the replication change affects only one replica).
	ReplicationAlwaysUseJointConfig func() bool
	// BeforeRemovingDemotedLearner is run before a demoted learner (i.e. a
	// learner that results from a range exiting its joint config) is removed.
	BeforeRemovingDemotedLearner func()
	// BeforeSnapshotSSTIngestion is run just before the SSTs are ingested when
	// applying a snapshot.
	BeforeSnapshotSSTIngestion func(IncomingSnapshot, kvserverpb.SnapshotRequest_Type, []string) error
	// OnRelocatedOne intercepts the return values of s.relocateOne after they
	// have successfully been put into effect.
	OnRelocatedOne func(_ []kvpb.ReplicationChange, leaseTarget *roachpb.ReplicationTarget)
	// DontIgnoreFailureToTransferLease makes `AdminRelocateRange` return an error
	// to its client if it failed to transfer the lease to the first voting
	// replica in the set of relocation targets.
	DontIgnoreFailureToTransferLease bool
	// MaxApplicationBatchSize enforces a maximum size on application batches.
	// This can be useful for testing conditions which require commands to be
	// applied in separate batches.
	MaxApplicationBatchSize int
	// RangeFeedPushTxnsInterval overrides the default value for
	// rangefeed.Config.PushTxnsInterval.
	RangeFeedPushTxnsInterval time.Duration
	// RangeFeedPushTxnsAge overrides the default value for
	// rangefeed.Config.PushTxnsAge.
	RangeFeedPushTxnsAge time.Duration
	// AllowLeaseProposalWhenNotLeader, if set, makes the proposal buffer allow
	// lease request proposals even when the replica inserting that proposal is
	// not the Raft leader. This can be used in tests to allow a replica to
	// acquire a lease without first moving the Raft leadership to it (e.g. it
	// allows tests to expire leases by stopping the old leaseholder's liveness
	// heartbeats and then expect other replicas to take the lease without
	// worrying about Raft).
	AllowLeaseRequestProposalsWhenNotLeader bool
	// LeaseTransferRejectedRetryLoopCount, if set, configures the maximum number
	// of retries for the retry loop used during lease transfers. This retry loop
	// retries after transfer attempts are rejected because the transfer is deemed
	// to be unsafe.
	LeaseTransferRejectedRetryLoopCount int
	// DontCloseTimestamps inhibits the propBuf's closing of timestamps. All Raft
	// commands will carry an empty closed timestamp.
	DontCloseTimestamps bool
	// AllowDangerousReplicationChanges disables safeguards
	// in execChangeReplicasTxn that prevent moving
	// to a configuration that cannot make progress.
	AllowDangerousReplicationChanges bool
	// AllowUnsynchronizedReplicationChanges allows calls to ChangeReplicas
	// even when the replicate queue is enabled. This often results in flaky
	// tests, so by default, it is prevented.
	AllowUnsynchronizedReplicationChanges bool
	// PurgeOutdatedReplicasInterceptor intercepts attempts to purge outdated
	// replicas in the store.
	PurgeOutdatedReplicasInterceptor func()
	// SpanConfigUpdateInterceptor is called after the store hears about a span
	// config update.
	SpanConfigUpdateInterceptor func(spanconfig.Update)
	// SetSpanConfigInterceptor is called before updating a replica's embedded
	// SpanConfig. The returned SpanConfig is used instead.
	// FIXME
	SetSpanConfigInterceptor func(*roachpb.RangeDescriptor, roachpb.SpanConfig) roachpb.SpanConfig
	// If set, use the given version as the initial replica version when
	// bootstrapping ranges. This is used for testing the migration
	// infrastructure.
	InitialReplicaVersionOverride *roachpb.Version
	// TimeSeriesDataStore is an interface used by the store's time series
	// maintenance queue to dispatch individual maintenance tasks.
	TimeSeriesDataStore TimeSeriesDataStore

	// Called whenever a range campaigns as a result of a tick. This
	// is called under the replica lock and raftMu, so basically don't
	// acquire any locks in this method.
	OnRaftTimeoutCampaign func(roachpb.RangeID)

	// RangefeedValueHeaderFilter, if set, is invoked before each value emitted on
	// the rangefeed, be it in steady state or during the catch-up scan.
	//
	// TODO(before merge): plumb the seqno through the rangefeed.
	RangefeedValueHeaderFilter func(key, endKey roachpb.Key, ts hlc.Timestamp, vh enginepb.MVCCValueHeader)

	// MakeSystemConfigSpanUnavailableToQueues makes the system config span
	// unavailable to queues that ask for it.
	MakeSystemConfigSpanUnavailableToQueues bool
	// ConfReaderInterceptor intercepts calls to get a span config reader.
	ConfReaderInterceptor func() spanconfig.StoreReader
	// IgnoreStrictGCEnforcement is used by tests to op out of strict GC
	// enforcement.
	IgnoreStrictGCEnforcement bool
	// ThrottleEmptySnapshots includes empty snapshots for throttling.
	ThrottleEmptySnapshots bool
	// BeforeSendSnapshotThrottle intercepts replicas before entering send
	// snapshot throttling.
	BeforeSendSnapshotThrottle func()
	// AfterSnapshotThrottle intercepts replicas after receiving a spot in the
	// send/recv snapshot semaphore.
	AfterSnapshotThrottle func()
	// BeforeRecvAcceptedSnapshot intercepts replicas before receiving the batches
	// of a reserved and accepted snapshot.
	BeforeRecvAcceptedSnapshot func()
	// SelectDelegateSnapshotSender returns an ordered list of replica which will
	// be used as delegates for sending a snapshot.
	SelectDelegateSnapshotSender func(*roachpb.RangeDescriptor) []roachpb.ReplicaDescriptor

	// EnqueueReplicaInterceptor intercepts calls to `store.Enqueue()`.
	EnqueueReplicaInterceptor func(queueName string, replica *Replica)

	// GlobalMVCCRangeTombstone will write a global MVCC range tombstone across
	// the entire user keyspace during cluster bootstrapping. This will be written
	// below all other data, and thus won't affect query results, but it does
	// activate MVCC range tombstone code paths in the storage layer for testing.
	//
	// This must typically be combined with the following knobs to prevent
	// various components choking on the range tombstone:
	//
	// - rangefeed.TestingKnobs.IgnoreOnDeleteRangeError
	// - kvserverbase.BatchEvalTestingKnobs.DisableInitPutFailOnTombstones
	GlobalMVCCRangeTombstone bool

	// LeaseUpgradeInterceptor intercepts leases that get upgraded to
	// epoch-based ones.
	LeaseUpgradeInterceptor func(*roachpb.Lease)

	// MVCCGCQueueLeaseCheckInterceptor intercepts calls to Replica.LeaseStatusAt when
	// making high priority replica scans.
	MVCCGCQueueLeaseCheckInterceptor func(ctx context.Context, replica *Replica, now hlc.ClockTimestamp) bool

	// DisableCanAckBeforeApplication disables acknowledging committed entries
	// before they apply. This can simplify some tests by making sure that a write
	// is on the engine when the request returns.
	DisableCanAckBeforeApplication bool

	// SmallEngineBlocks will configure the engine with a very small block size of
	// 1 byte, resulting in each key having its own block. This can provoke bugs
	// in time-bound iterators.
	SmallEngineBlocks bool

	// PreStorageSnapshotButChecksCompleteInterceptor intercepts calls to
	// Replica.executeReadOnlyBatch after checks have successfully determined
	// execution can proceed but a storage snapshot has not been acquired.
	PreStorageSnapshotButChecksCompleteInterceptor func(replica *Replica)

	// DisableMergeWaitForReplicasInit skips the waitForReplicasInit calls
	// during merges. Useful for testContext tests that want to use merges.
	DisableMergeWaitForReplicasInit bool

	// RangeLeaseAcquireTimeoutOverride overrides RaftConfig.RangeLeaseAcquireTimeout().
	RangeLeaseAcquireTimeoutOverride time.Duration

	// BaseQueueInterceptor is designed to intercept calls to baseQueue functions
	// to validate base queue properties. Currently, it only intercepts
	// baseQueue.MaybeAdd and ensure correct setting of the pprof label. However,
	// it can be easily extended to validate other properties of baseQueue if
	// required.
	BaseQueueInterceptor func(ctx context.Context, bq *baseQueue)

	// InjectReproposalError injects an error in tryReproposeWithNewLeaseIndex.
	// If nil is returned, reproposal will be attempted.
	InjectReproposalError func(p *ProposalData) error

	// LeaseIndexFilter can return nonzero to override the automatically assigned
	// LeaseAppliedIndex.
	LeaseIndexFilter func(*ProposalData) kvpb.LeaseAppliedIndex

	// FlowControlTestingKnobs provide fine-grained control over the various
	// kvflowcontrol components for testing.
	FlowControlTestingKnobs *kvflowcontrol.TestingKnobs
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*StoreTestingKnobs) ModuleTestingKnobs() {}

// NodeLivenessTestingKnobs allows tests to override some node liveness
// controls. When set, fields ultimately affect the NodeLivenessOptions used by
// the cluster.
type NodeLivenessTestingKnobs struct {
	// LivenessDuration overrides a liveness record's life time.
	LivenessDuration time.Duration
	// RenewalDuration specifies how long before the expiration a record is
	// heartbeated. If LivenessDuration is set, this should probably be set too.
	RenewalDuration time.Duration
	// StorePoolNodeLivenessFn is the function used by the StorePool to determine
	// whether a node is live or not.
	StorePoolNodeLivenessFn storepool.NodeLivenessFunc
	// IsLiveCallback, will be called when a node becomes live.
	IsLiveCallback liveness.IsLiveCallback
}

var _ base.ModuleTestingKnobs = NodeLivenessTestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (NodeLivenessTestingKnobs) ModuleTestingKnobs() {}

// PinnedLeasesKnob is a testing know for controlling what store can acquire a
// lease for specific ranges.
type PinnedLeasesKnob struct {
	mu struct {
		syncutil.Mutex
		pinned map[roachpb.RangeID]roachpb.StoreID
	}
}

// NewPinnedLeases creates a PinnedLeasesKnob.
func NewPinnedLeases() *PinnedLeasesKnob {
	p := &PinnedLeasesKnob{}
	p.mu.pinned = make(map[roachpb.RangeID]roachpb.StoreID)
	return p
}

// PinLease makes it so that only the specified store can take a lease for the
// specified range. Replicas on other stores attempting to acquire a lease will
// get NotLeaseHolderErrors. Lease transfers away from the pinned store are
// permitted.
func (p *PinnedLeasesKnob) PinLease(rangeID roachpb.RangeID, storeID roachpb.StoreID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.pinned[rangeID] = storeID
}

// rejectLeaseIfPinnedElsewhere is called when r is trying to acquire a lease.
// It returns a NotLeaseholderError if the lease is pinned on another store.
// r.mu needs to be rlocked.
func (p *PinnedLeasesKnob) rejectLeaseIfPinnedElsewhere(r *Replica) *kvpb.Error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	pinnedStore, ok := p.mu.pinned[r.RangeID]
	if !ok || pinnedStore == r.StoreID() {
		return nil
	}

	repDesc, err := r.getReplicaDescriptorRLocked()
	if err != nil {
		return kvpb.NewError(err)
	}
	pinned, _ := r.descRLocked().GetReplicaDescriptor(pinnedStore)
	return kvpb.NewError(&kvpb.NotLeaseHolderError{
		Replica: repDesc,
		Lease: &roachpb.Lease{
			Replica: pinned,
		},
		RangeID:   r.RangeID,
		CustomMsg: "injected: lease pinned to another store",
	})
}
