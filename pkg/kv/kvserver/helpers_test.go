// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file includes test-only helper methods added to types in
// package storage. These methods are only linked in to tests in this
// directory (but may be used from tests in both package storage and
// package storage_test).

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/storepool"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/split"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
)

func (s *Store) Transport() *RaftTransport {
	return s.cfg.Transport
}

func (s *Store) FindTargetAndTransferLease(
	ctx context.Context, repl *Replica, desc *roachpb.RangeDescriptor, conf roachpb.SpanConfig,
) (bool, error) {
	transferStatus, err := s.replicateQueue.shedLease(
		ctx, repl, desc, conf, allocator.TransferLeaseOptions{ExcludeLeaseRepl: true},
	)
	return transferStatus == allocator.TransferOK, err
}

// AddReplica adds the replica to the store's replica map and to the sorted
// replicasByKey slice. To be used only by unittests.
func (s *Store) AddReplica(repl *Replica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.addToReplicasByRangeIDLocked(repl); err != nil {
		return err
	} else if err := s.addToReplicasByKeyLocked(repl, repl.Desc()); err != nil {
		return err
	}
	s.metrics.ReplicaCount.Inc(1)
	return nil
}

// ComputeMVCCStats immediately computes correct total MVCC usage statistics
// for the store, returning the computed values (but without modifying the
// store).
func (s *Store) ComputeMVCCStats() (enginepb.MVCCStats, error) {
	var totalStats enginepb.MVCCStats
	var err error

	now := s.Clock().PhysicalNow()
	newStoreReplicaVisitor(s).Visit(func(r *Replica) bool {
		var stats enginepb.MVCCStats
		stats, err = rditer.ComputeStatsForRange(r.Desc(), s.TODOEngine(), now)
		if err != nil {
			return false
		}
		totalStats.Add(stats)
		return true
	})
	return totalStats, err
}

func (s *Store) UpdateLivenessMap() {
	s.updateLivenessMap()
}

// ConsistencyQueueShouldQueue invokes the shouldQueue method on the
// store's consistency queue.
func ConsistencyQueueShouldQueue(
	ctx context.Context,
	now hlc.ClockTimestamp,
	desc *roachpb.RangeDescriptor,
	getQueueLastProcessed func(ctx context.Context) (hlc.Timestamp, error),
	isNodeAvailable func(nodeID roachpb.NodeID) bool,
	disableLastProcessedCheck bool,
	interval time.Duration,
) (bool, float64) {
	return consistencyQueueShouldQueueImpl(ctx, now, consistencyShouldQueueData{
		desc, getQueueLastProcessed, isNodeAvailable,
		disableLastProcessedCheck, interval})
}

// LogReplicaChangeTest adds a fake replica change event to the log for the
// range which contains the given key.
func (s *Store) LogReplicaChangeTest(
	ctx context.Context,
	txn *kv.Txn,
	changeType roachpb.ReplicaChangeType,
	replica roachpb.ReplicaDescriptor,
	desc roachpb.RangeDescriptor,
	reason kvserverpb.RangeLogEventReason,
	details string,
	logAsync bool,
) error {
	return s.logChange(ctx, txn, changeType, replica, desc, reason, details, logAsync)
}

// LogSplitTest adds a fake split event to the rangelog.
func (s *Store) LogSplitTest(
	ctx context.Context,
	txn *kv.Txn,
	updatedDesc, newDesc roachpb.RangeDescriptor,
	reason string,
	logAsync bool,
) error {
	return s.logSplit(ctx, txn, updatedDesc, newDesc, reason, logAsync)
}

// ReplicateQueuePurgatoryLength returns the number of replicas in replicate
// queue purgatory.
func (s *Store) ReplicateQueuePurgatoryLength() int {
	return s.replicateQueue.PurgatoryLength()
}

// SplitQueuePurgatoryLength returns the number of replicas in split
// queue purgatory.
func (s *Store) SplitQueuePurgatoryLength() int {
	return s.splitQueue.PurgatoryLength()
}

// SetRaftLogQueueActive enables or disables the raft log queue.
func (s *Store) SetRaftLogQueueActive(active bool) {
	s.setRaftLogQueueActive(active)
}

// SetReplicaGCQueueActive enables or disables the replica GC queue.
func (s *Store) SetReplicaGCQueueActive(active bool) {
	s.setReplicaGCQueueActive(active)
}

// SetSplitQueueActive enables or disables the split queue.
func (s *Store) SetSplitQueueActive(active bool) {
	s.setSplitQueueActive(active)
}

// SetMergeQueueActive enables or disables the merge queue.
func (s *Store) SetMergeQueueActive(active bool) {
	s.setMergeQueueActive(active)
}

// SetRaftSnapshotQueueActive enables or disables the raft snapshot queue.
func (s *Store) SetRaftSnapshotQueueActive(active bool) {
	s.setRaftSnapshotQueueActive(active)
}

// SetReplicaScannerActive enables or disables the scanner. Note that while
// inactive, removals are still processed.
func (s *Store) SetReplicaScannerActive(active bool) {
	s.setScannerActive(active)
}

// EnqueueRaftUpdateCheck enqueues the replica for a Raft update check, forcing
// the replica's Raft group into existence.
func (s *Store) EnqueueRaftUpdateCheck(rangeID roachpb.RangeID) {
	s.enqueueRaftUpdateCheck(rangeID)
}

func manualQueue(s *Store, q queueImpl, repl *Replica) error {
	ctx := repl.AnnotateCtx(context.Background())
	_, err := q.process(ctx, repl)
	return err
}

// ManualMVCCGC processes the specified replica using the store's MVCC GC queue.
func (s *Store) ManualMVCCGC(repl *Replica) error {
	return manualQueue(s, s.mvccGCQueue, repl)
}

// ManualReplicaGC processes the specified replica using the store's replica
// GC queue.
func (s *Store) ManualReplicaGC(repl *Replica) error {
	return manualQueue(s, s.replicaGCQueue, repl)
}

// ManualRaftSnapshot will manually send a raft snapshot to the target replica.
func (s *Store) ManualRaftSnapshot(repl *Replica, target roachpb.ReplicaID) error {
	_, err := s.raftSnapshotQueue.processRaftSnapshot(context.Background(), repl, target)
	return err
}

// ReservationCount counts the number of outstanding reservations that are not
// running.
func (s *Store) ReservationCount() int {
	return int(s.cfg.SnapshotApplyLimit) - s.snapshotApplyQueue.AvailableLen()
}

// RaftSchedulerPriorityID returns the Raft scheduler's prioritized ranges.
func (s *Store) RaftSchedulerPriorityIDs() []roachpb.RangeID {
	return s.scheduler.PriorityIDs()
}

// GetStoreMetric retrieves the count of the store metric whose metadata name
// matches with the given name parameter. If the specified metric cannot be
// found, the function will return an error.
func (sm *StoreMetrics) GetStoreMetric(name string) (int64, error) {
	var c int64
	var found bool
	sm.registry.Each(func(n string, v interface{}) {
		if name == n {
			switch t := v.(type) {
			case *metric.Counter:
				c = t.Count()
				found = true
			case *metric.Gauge:
				c = t.Value()
				found = true
			}
		}
	})
	if !found {
		return -1, errors.Errorf("cannot find metric for %s", name)
	}
	return c, nil
}

// GetStoreMetrics fetches the count of each specified Store metric from the
// `metricNames` parameter and returns the result as a map. The keys in the map
// represent the metric metadata names, while the corresponding values indicate
// the count of each metric. If any of the specified metric cannot be found or
// is not a counter, the function will return an error.
//
// Assumption: 1. The metricNames parameter should consist of string literals
// that match the metadata names used for metric counters. 2. Each metric name
// provided in `metricNames` must exist, unique and be a counter type.
func (sm *StoreMetrics) GetStoreMetrics(metricsNames []string) (map[string]int64, error) {
	metrics := make(map[string]int64)
	for _, metricName := range metricsNames {
		count, err := sm.GetStoreMetric(metricName)
		if err != nil {
			return map[string]int64{}, errors.Errorf("cannot find metric for %s", metricName)
		}
		metrics[metricName] = count
	}
	return metrics, nil
}

func NewTestStorePool(cfg StoreConfig) *storepool.StorePool {
	liveness.TimeUntilNodeDead.Override(context.Background(), &cfg.Settings.SV, liveness.TestTimeUntilNodeDeadOff)
	return storepool.NewStorePool(
		cfg.AmbientCtx,
		cfg.Settings,
		cfg.Gossip,
		cfg.Clock,
		// NodeCountFunc
		func() int {
			return 1
		},
		func(roachpb.NodeID) livenesspb.NodeLivenessStatus {
			return livenesspb.NodeLivenessStatus_LIVE
		},
		/* deterministic */ false,
	)
}

func (r *Replica) Store() *Store {
	return r.store
}

func (r *Replica) Breaker() *circuit.Breaker {
	return r.breaker.wrapped
}

func (r *Replica) AssertState(ctx context.Context, reader storage.Reader) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.RLock()
	defer r.mu.RUnlock()
	r.assertStateRaftMuLockedReplicaMuRLocked(ctx, reader)
}

func (r *Replica) RaftLock() {
	r.raftMu.Lock()
}

func (r *Replica) RaftUnlock() {
	r.raftMu.Unlock()
}

func (r *Replica) RaftReportUnreachable(id roachpb.ReplicaID) error {
	return r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		raftGroup.ReportUnreachable(uint64(id))
		return false /* unquiesceAndWakeLeader */, nil
	})
}

// LastAssignedLeaseIndexRLocked returns the last assigned lease index.
func (r *Replica) LastAssignedLeaseIndex() kvpb.LeaseAppliedIndex {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.proposalBuf.LastAssignedLeaseIndexRLocked()
}

// Campaign campaigns the replica.
func (r *Replica) Campaign(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.campaignLocked(ctx)
}

// ForceCampaign force-campaigns the replica.
func (r *Replica) ForceCampaign(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.forceCampaignLocked(ctx)
}

// LastAssignedLeaseIndexRLocked is like LastAssignedLeaseIndex, but requires
// b.mu to be held in read mode.
func (b *propBuf) LastAssignedLeaseIndexRLocked() kvpb.LeaseAppliedIndex {
	return b.assignedLAI
}

// SetQuotaPool allows the caller to set a replica's quota pool initialized to
// a given quota. Additionally it initializes the replica's quota release queue
// and its command sizes map. Only safe to call on the replica that is both
// lease holder and raft leader while holding the raftMu.
func (r *Replica) InitQuotaPool(quota uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var appliedIndex kvpb.RaftIndex
	err := r.withRaftGroupLocked(func(r *raft.RawNode) (unquiesceAndWakeLeader bool, err error) {
		appliedIndex = kvpb.RaftIndex(r.BasicStatus().Applied)
		return false, nil
	})
	if err != nil {
		return err
	}

	r.mu.proposalQuotaBaseIndex = appliedIndex
	if r.mu.proposalQuota != nil {
		r.mu.proposalQuota.Close("re-creating")
	}
	r.mu.proposalQuota = quotapool.NewIntPool(r.rangeStr.String(), quota)
	r.mu.quotaReleaseQueue = nil
	return nil
}

// QuotaAvailable returns the quota available in the replica's quota pool. Only
// safe to call on the replica that is both lease holder and raft leader.
func (r *Replica) QuotaAvailable() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.proposalQuota.ApproximateQuota()
}

// GetProposalQuota returns the Replica's internal proposal quota.
// It is not safe to be used concurrently so do ensure that the Replica is
// no longer active.
func (r *Replica) GetProposalQuota() *quotapool.IntPool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.proposalQuota
}

func (r *Replica) QuotaReleaseQueueLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.mu.quotaReleaseQueue)
}

func (r *Replica) IsFollowerActiveSince(
	ctx context.Context, followerID roachpb.ReplicaID, threshold time.Duration,
) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.lastUpdateTimes.isFollowerActiveSince(followerID, timeutil.Now(), threshold)
}

// GetTSCacheHighWater returns the high water mark of the replica's timestamp
// cache.
func (r *Replica) GetTSCacheHighWater() hlc.Timestamp {
	start := roachpb.Key(r.Desc().StartKey)
	end := roachpb.Key(r.Desc().EndKey)
	t, _ := r.store.tsCache.GetMax(start, end)
	return t
}

// ShouldBackpressureWrites returns whether writes to the range should be
// subject to backpressure.
func (r *Replica) ShouldBackpressureWrites() bool {
	return r.shouldBackpressureWrites()
}

// GetRaftLogSize returns the approximate raft log size and whether it is
// trustworthy.. See r.mu.raftLogSize for details.
func (r *Replica) GetRaftLogSize() (int64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.raftLogSize, r.mu.raftLogSizeTrusted
}

// GetCachedLastTerm returns the cached last term value. May return
// invalidLastTerm if the cache is not set.
func (r *Replica) GetCachedLastTerm() kvpb.RaftTerm {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.lastTermNotDurable
}

// SideloadedRaftMuLocked returns r.raftMu.sideloaded. Requires a previous call
// to RaftLock() or some other guarantee that r.raftMu is held.
func (r *Replica) SideloadedRaftMuLocked() logstore.SideloadStorage {
	return r.raftMu.sideloaded
}

// LargestPreviousMaxRangeSizeBytes returns the in-memory value used to mitigate
// backpressure when the zone.RangeMaxBytes is decreased.
func (r *Replica) LargestPreviousMaxRangeSizeBytes() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.largestPreviousMaxRangeSizeBytes
}

// LoadBasedSplitter returns the replica's split.Decider, which is used to
// assist load-based split (and merge) decisions.
func (r *Replica) LoadBasedSplitter() *split.Decider {
	return &r.loadBasedSplitter
}

func MakeSSTable(
	ctx context.Context, key, value string, ts hlc.Timestamp,
) ([]byte, storage.MVCCKeyValue) {
	sstFile := &storage.MemObject{}
	sst := storage.MakeIngestionSSTWriter(ctx, cluster.MakeTestingClusterSettings(), sstFile)
	defer sst.Close()

	v := roachpb.MakeValueFromBytes([]byte(value))
	v.InitChecksum([]byte(key))

	kv := storage.MVCCKeyValue{
		Key: storage.MVCCKey{
			Key:       []byte(key),
			Timestamp: ts,
		},
		Value: v.RawBytes,
	}

	if err := sst.Put(kv.Key, kv.Value); err != nil {
		panic(errors.Wrap(err, "while finishing SSTable"))
	}
	if err := sst.Finish(); err != nil {
		panic(errors.Wrap(err, "while finishing SSTable"))
	}
	return sstFile.Data(), kv
}

func ProposeAddSSTable(ctx context.Context, key, val string, ts hlc.Timestamp, store *Store) error {
	ba := &kvpb.BatchRequest{}
	ba.RangeID = store.LookupReplica(roachpb.RKey(key)).RangeID

	var addReq kvpb.AddSSTableRequest
	addReq.Data, _ = MakeSSTable(ctx, key, val, ts)
	addReq.Key = roachpb.Key(key)
	addReq.EndKey = addReq.Key.Next()
	ba.Add(&addReq)

	_, pErr := store.Send(ctx, ba)
	if pErr != nil {
		return pErr.GoError()
	}
	return nil
}

func SetMockAddSSTable() (undo func()) {
	prev, _ := batcheval.LookupCommand(kvpb.AddSSTable)

	// TODO(tschottdorf): this already does nontrivial work. Worth open-sourcing the relevant
	// subparts of the real evalAddSSTable to make this test less likely to rot.
	evalAddSSTable := func(
		ctx context.Context, _ storage.ReadWriter, cArgs batcheval.CommandArgs, _ kvpb.Response,
	) (result.Result, error) {
		log.Event(ctx, "evaluated testing-only AddSSTable mock")
		args := cArgs.Args.(*kvpb.AddSSTableRequest)

		return result.Result{
			Replicated: kvserverpb.ReplicatedEvalResult{
				AddSSTable: &kvserverpb.ReplicatedEvalResult_AddSSTable{
					Data:  args.Data,
					CRC32: util.CRC32(args.Data),
				},
			},
		}, nil
	}

	batcheval.UnregisterCommand(kvpb.AddSSTable)
	batcheval.RegisterReadWriteCommand(kvpb.AddSSTable, batcheval.DefaultDeclareKeys, evalAddSSTable)
	return func() {
		batcheval.UnregisterCommand(kvpb.AddSSTable)
		batcheval.RegisterReadWriteCommand(kvpb.AddSSTable, prev.DeclareKeys, prev.EvalRW)
	}
}

// GetQueueLastProcessed returns the last processed timestamp for the
// specified queue, or the zero timestamp if not available.
func (r *Replica) GetQueueLastProcessed(ctx context.Context, queue string) (hlc.Timestamp, error) {
	return r.getQueueLastProcessed(ctx, queue)
}

func (r *Replica) MaybeUnquiesce() bool {
	return r.maybeUnquiesce(true /* wakeLeader */, true /* mayCampaign */)
}

// MaybeUnquiesceAndPropose will unquiesce the range and submit a noop proposal.
// This is useful when unquiescing the leader and wanting to also unquiesce
// followers, since the leader may otherwise simply quiesce again immediately.
func (r *Replica) MaybeUnquiesceAndPropose() (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.canUnquiesceRLocked() {
		return false, nil
	}
	return true, r.withRaftGroupLocked(func(r *raft.RawNode) (bool, error) {
		if err := r.Propose(nil); err != nil {
			return false, err
		}
		return true /* unquiesceAndWakeLeader */, nil
	})
}

func (r *Replica) ReadCachedProtectedTS() (readAt, earliestProtectionTimestamp hlc.Timestamp) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mu.cachedProtectedTS.readAt, r.mu.cachedProtectedTS.earliestProtectionTimestamp
}

// ClosedTimestampPolicy returns the closed timestamp policy of the range, which
// is updated asynchronously through gossip of zone configurations.
func (r *Replica) ClosedTimestampPolicy() roachpb.RangeClosedTimestampPolicy {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.closedTimestampPolicyRLocked()
}

// TripBreaker synchronously trips the breaker.
func (r *Replica) TripBreaker() {
	r.breaker.tripSync(errors.New("injected error"))
}

// GetCircuitBreaker returns the circuit breaker controlling
// connection attempts to the specified node.
func (t *RaftTransport) GetCircuitBreaker(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) (*circuit.Breaker, bool) {
	return t.dialer.GetCircuitBreaker(nodeID, class)
}

func WriteRandomDataToRange(
	t testing.TB, store *Store, rangeID roachpb.RangeID, keyPrefix roachpb.Key,
) (splitKey []byte) {
	t.Helper()

	ctx := context.Background()
	src, _ := randutil.NewTestRand()
	for i := 0; i < 1000; i++ {
		var req kvpb.Request
		if src.Float64() < 0.05 {
			// Write some occasional range tombstones.
			startKey := append(keyPrefix.Clone(), randutil.RandBytes(src, int(src.Int31n(1<<4)))...)
			var endKey roachpb.Key
			for startKey.Compare(endKey) >= 0 {
				endKey = append(keyPrefix.Clone(), randutil.RandBytes(src, int(src.Int31n(1<<4)))...)
			}
			req = &kvpb.DeleteRangeRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    startKey,
					EndKey: endKey,
				},
				UseRangeTombstone: true,
			}
		} else {
			// Write regular point keys.
			key := append(keyPrefix.Clone(), randutil.RandBytes(src, int(src.Int31n(1<<4)))...)
			val := randutil.RandBytes(src, int(src.Int31n(1<<8)))
			pArgs := putArgs(key, val)
			req = &pArgs
		}
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{RangeID: rangeID}, req)
		require.NoError(t, pErr.GoError())
	}
	// Return a random non-empty split key.
	return append(keyPrefix.Clone(), randutil.RandBytes(src, int(src.Int31n(1<<4))+1)...)
}

func WatchForDisappearingReplicas(t testing.TB, store *Store) {
	m := make(map[roachpb.RangeID]struct{})
	for {
		select {
		case <-store.Stopper().ShouldQuiesce():
			return
		default:
		}

		store.mu.replicasByRangeID.Range(func(repl *Replica) {
			m[repl.RangeID] = struct{}{}
		})

		for k := range m {
			if _, ok := store.mu.replicasByRangeID.Load(k); !ok {
				t.Fatalf("r%d disappeared from Store.mu.replicas map", k)
			}
		}
	}
}

// getMapsDiff returns the difference between the values of corresponding
// metrics in two maps. Assumption: beforeMap and afterMap contain the same set
// of keys.
func getMapsDiff(beforeMap map[string]int64, afterMap map[string]int64) map[string]int64 {
	diffMap := make(map[string]int64)
	for metricName, beforeValue := range beforeMap {
		if v, ok := afterMap[metricName]; ok {
			diffMap[metricName] = v - beforeValue
		}
	}
	return diffMap
}
