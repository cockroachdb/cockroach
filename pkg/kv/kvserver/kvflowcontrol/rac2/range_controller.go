// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// RangeController provides flow control for replication traffic in KV, for a
// range at the leader. It must be created for a particular leader term, and
// closed if the term changes.
//
// Almost none of the methods are called with Replica.mu held. The caller and
// callee should order their mutexes before Replica.mu. The exceptions are
// HoldsSendTokensLocked, ForceFlushIndexChangedLocked, which hold both raftMu
// and Replica.mu. The callee must not acquire its own mutex.
//
// RangeController dynamically switches between push and pull mode based on
// RaftEvent handling. In general, the code here is oblivious to the fact that
// WaitForEval in push mode will only be called for elastic work. However,
// there are emergent behaviors that rely on this behavior (which are noted in
// comments). Unit tests can run RangeController in push mode and call
// WaitForEval for regular work.
type RangeController interface {
	// WaitForEval seeks admission to evaluate a request at the given priority.
	// If the priority is subject to replication admission control, it blocks
	// until there are positive tokens available for the request to be admitted
	// for evaluation, or the context is canceled (which returns an error). Note
	// the number of tokens required by the request is not considered, only the
	// priority of the request, as the number of tokens is not known until eval.
	//
	// In the non-error case, the waited return value is true if the priority
	// was subject to replication admission control, and the RangeController was
	// not closed during the execution of WaitForEval. If closed, or the
	// priority is not subject to replication admission control, a (false, nil)
	// will be returned -- this is important for the caller to fall back to
	// waiting on the local store.
	//
	// No mutexes should be held.
	WaitForEval(ctx context.Context, pri admissionpb.WorkPriority) (waited bool, err error)
	// HandleRaftEventRaftMuLocked handles the provided raft event for the range.
	//
	// Requires replica.raftMu to be held.
	HandleRaftEventRaftMuLocked(ctx context.Context, e RaftEvent) error
	// HandleSchedulerEventRaftMuLocked processes an event scheduled by the
	// controller. logSnapshot is only used if mode is MsgAppPull.
	//
	// Requires replica.raftMu to be held.
	HandleSchedulerEventRaftMuLocked(
		ctx context.Context, mode RaftMsgAppMode, logSnapshot raft.LogSnapshot)
	// AdmitRaftMuLocked handles the notification about the given replica's
	// admitted vector change. No-op if the replica is not known, or the admitted
	// vector is stale (either in Term, or the indices).
	//
	// Requires replica.raftMu to be held.
	AdmitRaftMuLocked(context.Context, roachpb.ReplicaID, AdmittedVector)
	// MaybeSendPingsRaftMuLocked sends a MsgApp ping to each raft peer in
	// StateReplicate whose admitted vector is lagging, and there wasn't a
	// recent MsgApp to this peer.
	//
	// Requires replica.raftMu to be held.
	MaybeSendPingsRaftMuLocked()
	// HoldsSendTokensLocked returns true if the replica holds any send tokens.
	// Used to prevent replica quiescence.
	//
	// Requires replica.raftMu and replica.mu to be held.
	HoldsSendTokensLocked() bool
	// SetReplicasRaftMuLocked sets the replicas of the range. The caller will
	// never mutate replicas, and neither should the callee.
	//
	// Requires replica.raftMu to be held.
	SetReplicasRaftMuLocked(ctx context.Context, replicas ReplicaSet) error
	// SetLeaseholderRaftMuLocked sets the leaseholder of the range.
	//
	// Requires replica.raftMu to be held.
	SetLeaseholderRaftMuLocked(ctx context.Context, replica roachpb.ReplicaID)
	// ForceFlushIndexChangedLocked sets the force flush index, i.e., the index
	// (inclusive) up to which all replicas with a send-queue must be
	// force-flushed in MsgAppPull mode. It may be rarely called with no change
	// to the index.
	//
	// Requires replica.raftMu and replica.mu to be held.
	ForceFlushIndexChangedLocked(ctx context.Context, index uint64)
	// CloseRaftMuLocked closes the range controller.
	//
	// Requires replica.raftMu to be held.
	CloseRaftMuLocked(ctx context.Context)
	// InspectRaftMuLocked returns a handle containing the state of the range
	// controller. It's used to power /inspectz-style debugging pages.
	//
	// Requires replica.raftMu to be held.
	InspectRaftMuLocked(ctx context.Context) kvflowinspectpb.Handle
	// StatusRaftMuLocked returns basic information about the range controller and
	// its send streams.
	//
	// Requires replica.raftMu to be held.
	StatusRaftMuLocked() serverpb.RACStatus
	// SendStreamStats sets the stats for the replica send streams that belong to
	// the range controller. It is only populated on the leader. The stats struct
	// is provided by the caller and should be empty, it is then populated before
	// returning.
	//
	// INVARIANT: len(stats.internal) = 0.
	//
	// NOTE: The send queue size and count are populated but have bounded
	// staleness, up to sendQueueStatRefreshInterval (5s). On each call,
	// IsStateReplicate and HasSendQueue is recomputed for each
	// ReplicaSendStreamStats.
	SendStreamStats(stats *RangeSendStreamStats)
}

// RaftInterface implements methods needed by RangeController. It abstracts
// raft.RawNode.
//
// Replica.mu is not held when calling any methods. Replica.raftMu is held,
// though is not needed, and is mentioned in the method names purely from an
// informational perspective.
type RaftInterface interface {
	// SendPingRaftMuLocked sends a MsgApp ping to the given raft peer if
	// there wasn't a recent MsgApp to this peer. The message is added to raft's
	// message queue, and will be extracted and sent during the next Ready
	// processing.
	//
	// If the peer is not in StateReplicate, this call does nothing.
	SendPingRaftMuLocked(roachpb.ReplicaID) bool
	// SendMsgAppRaftMuLocked is used to construct a MsgApp for entries in the
	// slice and must only be called in MsgAppPull mode for followers. Say
	// [start, end) represent the entries in the slice.
	//
	// REQUIRES (to the best knowledge of the caller):
	// - start < end, i.e., the slice is non-empty.
	// - replicaID i, is in StateReplicate.
	// - start == Next(i)
	// - end <= NextUnstableIndex
	//
	// Returns false if a message cannot be generated. This could be because the
	// knowledge of the caller is incorrect (which can happen because it is
	// stale). See RawNode.SendMsgApp for the error conditions.
	//
	// If it returns true, all the entries in the slice are in the message, and
	// Next is advanced to be equal to end.
	SendMsgAppRaftMuLocked(replicaID roachpb.ReplicaID, slice raft.LogSlice) (raftpb.Message, bool)
}

// RaftMsgAppMode specifies how Raft (at the leader) generates MsgApps. In
// both modes, Raft knows that (Match(i), Next(i)) are in-flight for a
// follower i.
type RaftMsgAppMode uint8

const (
	// MsgAppPush is the classic way in which Raft operates, where the Ready
	// includes MsgApps for followers. We want to preserve this mode for now due
	// to confidence in its performance characteristics, and to lower the risk
	// of a bug in replication flow control affecting MsgApps. In this mode Raft
	// is responsible for flow control, i.e., deciding when to send the entries
	// in [Next(i),NextUnstableIndex), to follower i.
	MsgAppPush RaftMsgAppMode = iota
	// MsgAppPull is the way in which Raft operates when the RangeController is
	// using send tokens to pace sending of work to a follower. The MsgApps are
	// generated by calling a method on RaftInterface, and Raft's flow control
	// is disabled. That is, the caller asks Raft to generate MsgApps for a
	// prefix of [Next(i),NextUnstableIndex), for follower i.
	MsgAppPull
)

type ReplicaStateInfo struct {
	State tracker.StateType

	// Remaining only populated in StateReplicate.
	// (Match, Next) is in-flight.
	Match uint64
	Next  uint64
	// InflightBytes are the bytes that have been sent but not yet persisted. It
	// corresponds to tracker.Inflights.bytes.
	InflightBytes uint64
}

// sendQueueStatRefreshInterval is the interval at which the send queue stats
// are refreshed by the range controller, as part of
// HandleRaftEventRaftMuLocked. One should expect the stats to be at most this
// stale.
const sendQueueStatRefreshInterval = 5 * time.Second

// RangeSendStreamStats contains the stats for the replica send streams that
// belong to a range.
type RangeSendStreamStats struct {
	internal []ReplicaSendStreamStats
}

func (s *RangeSendStreamStats) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s *RangeSendStreamStats) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("[")
	for i := range s.internal {
		if i > 0 {
			w.Printf(", ")
		}
		w.Printf("r%v=(%v)", s.internal[i].ReplicaID, s.internal[i])
	}
	w.Printf("]")
}

// Clear clears the stats for all replica send streams so that the underlying
// memory can be reused.
func (s *RangeSendStreamStats) Clear() {
	s.internal = s.internal[:0]
}

// SetReplicaSendStreamStats sets the stats for the replica send stream that
// belong to the given replicaID.
func (s *RangeSendStreamStats) SetReplicaSendStreamStats(stats ReplicaSendStreamStats) {
	for i := range s.internal {
		if s.internal[i].ReplicaID == stats.ReplicaID {
			s.internal[i] = stats
			return
		}
	}
	s.internal = append(s.internal, stats)
}

// ReplicaSendStreamStats returns the stats for the replica send stream that
// belong to the given replicaID, if it exists, otherwise an empty stats
// struct is returned.
func (s *RangeSendStreamStats) ReplicaSendStreamStats(
	replicaID roachpb.ReplicaID,
) (ReplicaSendStreamStats, bool) {
	for i := range s.internal {
		if s.internal[i].ReplicaID == replicaID {
			return s.internal[i], true
		}
	}
	return ReplicaSendStreamStats{}, false
}

// SumSendQueues returns the sum of the send queues across all replicas,
// returning both the aggregated number of entries and the number of bytes.
func (s *RangeSendStreamStats) SumSendQueues() (count int64, bytes int64) {
	for _, stats := range s.internal {
		count += stats.SendQueueCount
		bytes += stats.SendQueueBytes
	}
	return count, bytes
}

// RangeSendQueueStats contains the stats for the replica send queues that
// belong to a range. Currently, this is only used to periodically refresh
// queue stats, which are used to create the RangeSendStreamStats above.
type RangeSendQueueStats []ReplicaSendQueueStats

// ReplicaSendQueueStats returns the stats for the replica send queue that
// belongs to the replica with the given replicaID, and true if it exists,
// otherwise an empty stats struct and false is returned.
func (q *RangeSendQueueStats) ReplicaSendQueueStats(
	replicaID roachpb.ReplicaID,
) (ReplicaSendQueueStats, bool) {
	for i := range *q {
		if (*q)[i].ReplicaID == replicaID {
			return (*q)[i], true
		}
	}
	return ReplicaSendQueueStats{}, false
}

// Set sets the queue stats for the replica with the given replicaID.
func (q *RangeSendQueueStats) Set(stats ReplicaSendQueueStats) {
	for i := range *q {
		if (*q)[i].ReplicaID == stats.ReplicaID {
			(*q)[i] = stats
			return
		}
	}
	*q = append(*q, stats)
}

// Remove removes the queue stats for the replica with the given replicaID.
func (q *RangeSendQueueStats) Remove(replicaID roachpb.ReplicaID) {
	for i := range *q {
		if (*q)[i].ReplicaID == replicaID {
			*q = append((*q)[:i], (*q)[i+1:]...)
			return
		}
	}
}

// Clear empties the queue stats.
func (q *RangeSendQueueStats) Clear() {
	*q = (*q)[:0]
}

// ReplicaSendStreamStats contains the stats for a replica send stream that may
// be used to inform placement decisions pertaining to the replica.
type ReplicaSendStreamStats struct {
	// IsStateReplicate is true iff the replica is being sent entries.
	IsStateReplicate bool
	// HasSendQueue is true when a replica has a non-zero amount of queued
	// entries waiting on flow tokens to be sent.
	//
	// !IsStateReplicate => HasSendQueue, even if the replica doesn't have a send
	// queue tracked by the send stream explicitly.
	HasSendQueue bool
	// ReplicaSendStreamStats is updated infrequently (unlike the above) and may
	// be up to sendStreamStatRefreshInterval stale. It contains the size and
	// count of the send queue, if it exists, otherwise 0.
	ReplicaSendQueueStats
}

func (rsss ReplicaSendStreamStats) String() string {
	return redact.StringWithoutMarkers(rsss)
}

func (rsss ReplicaSendStreamStats) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("is_state_replicate=%v has_send_queue=%v %v",
		rsss.IsStateReplicate, rsss.HasSendQueue, rsss.ReplicaSendQueueStats)
}

// ReplicaSendQueueStats contains the size and count of the send stream queue
// for a replica.
type ReplicaSendQueueStats struct {
	ReplicaID roachpb.ReplicaID
	// SendQueueBytes is the total size of the entries in the send queue.
	SendQueueBytes int64
	// SendQueueCount is the number of entries in the send queue.
	SendQueueCount int64
}

func (rsqs ReplicaSendQueueStats) String() string {
	return redact.StringWithoutMarkers(rsqs)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (rsqs ReplicaSendQueueStats) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("send_queue_size=%v / %v entries",
		humanizeutil.IBytes(rsqs.SendQueueBytes), rsqs.SendQueueCount)
}

// RaftEvent carries a RACv2-relevant subset of raft state sent to storage.
type RaftEvent struct {
	// MsgAppMode is the current mode. This is only relevant on the leader.
	MsgAppMode RaftMsgAppMode
	// Term is the leader term on whose behalf the entries or snapshot are
	// written. Note that it may be behind the raft node's current term. Not
	// populated if Entries is empty and Snap is nil.
	Term uint64
	// Snap contains the snapshot to be written to storage.
	Snap *raftpb.Snapshot
	// Entries contains the log entries to be written to storage.
	Entries []raftpb.Entry
	// MsgApps to followers. Only populated on the leader, when operating in
	// MsgAppPush mode. This is informational, for bookkeeping in the callee,
	// which only looks at MsgApps with non-empty Entries.
	//
	// These MsgApps can be for entries in Entries, or for earlier ones.
	// Typically, the MsgApps are ordered by entry index, and are a sequence of
	// dense indices, starting at the previously observed (during the previous
	// RaftEvent) value of FollowerStateInfo.Next.
	//
	// But there can be exceptions. Indices can regress (on leader term change
	// or MsgAppResp reject) or jump (on receiving a MsgAppResp that indicates
	// that the follower is ahead of what the leader thinks). It is also
	// possible for there to be state transitions from StateReplicate =>
	// StateProbe/StateSnapshot => StateReplicate, between two consecutive
	// RaftEvents, i.e., RangeController would not observe the intermediate
	// state. Such missed transitions can also cause regressions or forward
	// jumps. Also, MsgApps can correspond to an older leader term, or prior to
	// the state transition to StateProbe/StateReplicate.
	//
	// Due to these exceptions, we only expect a single invariant: if the leader
	// term has not changed from the previous RaftEvent, all the MsgApps will be
	// from this leader term.
	//
	// A key can map to an empty slice, in order to reuse already allocated
	// slice memory.
	MsgApps map[roachpb.ReplicaID][]raftpb.Message
	// LogSnapshot must be populated on the leader, when operating in MsgAppPull
	// mode. It is used (along with RaftInterface) to construct MsgApps.
	LogSnapshot raft.LogSnapshot
	// ReplicasStateInfo contains the state of all replicas. This is used to
	// determine if the state of a replica has changed, and if so, to update the
	// flow control state. It also informs the RangeController of a replica's
	// Match and Next.
	ReplicasStateInfo map[roachpb.ReplicaID]ReplicaStateInfo
}

// Scheduler abstracts the raftScheduler to allow the RangeController to
// schedule its own internal processing. This internal processing is to pop
// some entries from the send queue and send them in a MsgApp.
type Scheduler interface {
	ScheduleControllerEvent(rangeID roachpb.RangeID)
}

// MsgAppSender is used to send a MsgApp in pull mode.
type MsgAppSender interface {
	// SendMsgApp sends a MsgApp with the given lowPriorityOverride.
	SendMsgApp(ctx context.Context, msg raftpb.Message, lowPriorityOverride bool)
}

// RaftEventFromMsgStorageAppendAndMsgApps constructs a RaftEvent from the
// given raft MsgStorageAppend message, and outboundMsgs. The replicaID is the
// local replica. The outboundMsgs will only contain MsgApps on the leader.
// msgAppScratch is used as the map in RaftEvent.MsgApps. Returns the zero
// value if the MsgStorageAppend is empty and there are no MsgApps.
func RaftEventFromMsgStorageAppendAndMsgApps(
	mode RaftMsgAppMode,
	replicaID roachpb.ReplicaID,
	appendMsg raftpb.Message,
	outboundMsgs []raftpb.Message,
	logSnapshot raft.LogSnapshot,
	msgAppScratch map[roachpb.ReplicaID][]raftpb.Message,
	replicaStateInfoMap map[roachpb.ReplicaID]ReplicaStateInfo,
) RaftEvent {
	event := RaftEvent{
		MsgAppMode: mode, LogSnapshot: logSnapshot, ReplicasStateInfo: replicaStateInfoMap}
	if appendMsg.Type == raftpb.MsgStorageAppend {
		event.Term = appendMsg.LogTerm
		event.Snap = appendMsg.Snapshot
		event.Entries = appendMsg.Entries
	}
	if len(outboundMsgs) == 0 || mode == MsgAppPull {
		// MsgAppPull mode can have MsgApps with entries under some cases: (a)
		// when the replica is in StateProbe, (b) stale MsgApps queued up inside
		// Raft from when the replica was in StateProbe, even though it is now in
		// StateReplicate. We ignore those in the RaftEvent created for the
		// RangeController. They will get sent, but that is not the concern of the
		// RACv2 code.
		return event
	}
	// Clear the slices, to reuse slice allocations.
	for k := range msgAppScratch {
		msgAppScratch[k] = msgAppScratch[k][:0]
	}
	if len(msgAppScratch) > 10 {
		// Clear all memory, in case we have a long-lived leader while other
		// replicas keep changing.
		clear(msgAppScratch)
	}
	added := false
	for _, msg := range outboundMsgs {
		if msg.Type != raftpb.MsgApp || roachpb.ReplicaID(msg.To) == replicaID {
			continue
		}
		added = true
		msgs := msgAppScratch[roachpb.ReplicaID(msg.To)]
		msgs = append(msgs, msg)
		msgAppScratch[roachpb.ReplicaID(msg.To)] = msgs
	}
	if added {
		event.MsgApps = msgAppScratch
	}
	return event
}

// NoReplicaID is a special value of roachpb.ReplicaID, which can never be a
// valid ID.
const NoReplicaID roachpb.ReplicaID = 0

// ReplicaSet is a map, unlike roachpb.ReplicaSet, for convenient lookup by
// ReplicaID.
type ReplicaSet map[roachpb.ReplicaID]roachpb.ReplicaDescriptor

// SafeFormat implements the redact.SafeFormatter interface.
func (rs ReplicaSet) SafeFormat(w redact.SafePrinter, _ rune) {
	// If <= 7 replicas, no need to allocate.
	var buf [7]roachpb.ReplicaDescriptor
	replicas := buf[0:0:len(buf)]
	for _, desc := range rs {
		replicas = append(replicas, desc)
	}
	slices.SortFunc(replicas, func(a, b roachpb.ReplicaDescriptor) int {
		return cmp.Compare(a.ReplicaID, b.ReplicaID)
	})
	w.Printf("[")
	i := 0
	for _, desc := range replicas {
		if i > 0 {
			w.Printf(",")
		}
		w.Printf("%v", desc)
		i++
	}
	w.Printf("]")
}

func (rs ReplicaSet) String() string {
	return redact.StringWithoutMarkers(rs)
}

// ProbeToCloseTimerScheduler is an interface for scheduling the closing of a
// replica send stream.
type ProbeToCloseTimerScheduler interface {
	// ScheduleSendStreamCloseRaftMuLocked schedules a callback with a raft event
	// after the given delay. This function may be used to handle send stream
	// state transition, usually to close a send stream after the given delay.
	// e.g.,
	//
	//   HandleRaftEventRaftMuLocked(ctx, RaftEvent{})
	//
	// Which will trigger handleReadyStateRaftMuLocked to close the send stream if it hasn't
	// transitioned to StateReplicate.
	//
	// Requires replica.raftMu to be held.
	ScheduleSendStreamCloseRaftMuLocked(
		ctx context.Context, rangeID roachpb.RangeID, delay time.Duration)
}

// ReplicaMutexAsserter must only be used to assert that mutexes are held.
// This is a concrete struct so that the assertions can be compiled away in
// production code.
type ReplicaMutexAsserter struct {
	RaftMu    *syncutil.Mutex
	ReplicaMu *syncutil.RWMutex
}

func MakeReplicaMutexAsserter(
	raftMu *syncutil.Mutex, replicaMu *syncutil.RWMutex,
) ReplicaMutexAsserter {
	return ReplicaMutexAsserter{
		RaftMu:    raftMu,
		ReplicaMu: replicaMu,
	}
}

// RaftMuAssertHeld asserts that Replica.raftMu is held.
//
// gcassert:inline
func (rmu ReplicaMutexAsserter) RaftMuAssertHeld() {
	rmu.RaftMu.AssertHeld()
}

// ReplicaMuAssertHeld asserts that Replica.mu is held for writing.
//
// gcassert:inline
func (rmu ReplicaMutexAsserter) ReplicaMuAssertHeld() {
	rmu.ReplicaMu.AssertHeld()
}

type RangeControllerOptions struct {
	RangeID  roachpb.RangeID
	TenantID roachpb.TenantID
	// LocalReplicaID is the ReplicaID of the local replica, which is the
	// leader.
	LocalReplicaID roachpb.ReplicaID
	// SSTokenCounter provides access to all the TokenCounters that will be
	// needed (keyed by (tenantID, storeID)).
	SSTokenCounter         *StreamTokenCounterProvider
	RaftInterface          RaftInterface
	MsgAppSender           MsgAppSender
	Clock                  *hlc.Clock
	CloseTimerScheduler    ProbeToCloseTimerScheduler
	Scheduler              Scheduler
	SendTokenWatcher       *SendTokenWatcher
	EvalWaitMetrics        *EvalWaitMetrics
	RangeControllerMetrics *RangeControllerMetrics
	WaitForEvalConfig      *WaitForEvalConfig
	// RaftMaxInflightBytes is a soft limit on the maximum inflight bytes when
	// using MsgAppPull mode. Currently, the RangeController only attempts to
	// respect this when force-flushing a replicaSendStream, since the typical
	// production configuration of this value (32MiB) is larger than the typical
	// production configuration of the shared regular token pool (16MiB), so
	// attempting to respect this when doing non-force-flush sends is
	// unnecessary.
	RaftMaxInflightBytes uint64
	ReplicaMutexAsserter ReplicaMutexAsserter
	Knobs                *kvflowcontrol.TestingKnobs
}

// RangeControllerInitState is the initial state at the time of creation.
type RangeControllerInitState struct {
	// Term is the raft term of the leader who runs this RangeController. It does
	// not change during the lifetime of the RangeController.
	Term uint64
	// Must include RangeControllerOptions.ReplicaID.
	ReplicaSet ReplicaSet
	// Leaseholder may be set to NoReplicaID, in which case the leaseholder is
	// unknown.
	Leaseholder roachpb.ReplicaID
	// NextRaftIndex is the first index that will appear in the next non-empty
	// RaftEvent.Entries handled by this RangeController.
	NextRaftIndex uint64
	// FirstFlushIndex is an index up to (and including) which the
	// rangeController running in pull mode must force-flush all send streams.
	ForceFlushIndex uint64
}

// rangeController is tied to a single leader term.
type rangeController struct {
	opts RangeControllerOptions
	// term is the raft term of the leader who runs this range controller. The
	// term does not change during the lifetime of the range controller.
	term uint64
	// replicaSet contains exactly the same entries as replicaMap.
	//
	// TODO(sumeer): remove the replicaSet member.
	replicaSet ReplicaSet
	// leaseholder can be NoReplicaID or not be in ReplicaSet, i.e., it is
	// eventually consistent with the set of replicas.
	leaseholder     roachpb.ReplicaID
	nextRaftIndex   uint64
	forceFlushIndex uint64

	mu struct {
		// All the fields in this struct are modified while holding raftMu and
		// this mutex. So readers can hold either mutex. This mutex must be
		// released quickly, since it is needed by rangeController.WaitForEval
		// which can have high concurrency.
		syncutil.RWMutex

		// State for waiters. When anything in voterSets or nonVoterSets changes,
		// waiterSetRefreshCh is closed, and replaced with a new channel. The
		// voterSets and nonVoterSets is copy-on-write, so waiters make a shallow
		// copy.
		//
		// Replicas in replicaSet that are neither voter or non-voter, e.g.
		// LEARNER, does not appear here.
		voterSets          []voterSet
		nonVoterSet        []stateForWaiters
		waiterSetRefreshCh chan struct{}

		// lastSendQueueStats is the last send queue stats that were populated
		// via HandleRaftEventRaftMuLocked, at a frequency of
		// sendStreamStatRefreshInterval. These don't contain the full
		// SendStreamStats, which is returned by SendStreamStats(). The full stats
		// are populated by using these boundedly stale stats in conjunction with
		// recalculating the IsStateReplicate and HasSendQueue fields.
		lastSendQueueStats RangeSendQueueStats
		// lastSendQueueStatRefresh is the time at which the current
		// lastSendQueueStats were populated.
		lastSendQueueStatRefresh time.Time
	}

	replicaMap map[roachpb.ReplicaID]*replicaState

	scheduledMu struct {
		syncutil.Mutex
		// When HandleControllerSchedulerEventRaftMuLocked is called, this is used
		// to call into the replicaSendStreams that have asked to be scheduled.
		replicas map[roachpb.ReplicaID]struct{}
	}
	entryFCStateScratch       []entryFCState
	lastSendQueueStatsScratch RangeSendQueueStats

	consistencyCheckerScratchMap map[roachpb.ReplicaID]stateForWaiters
	consistencyCheckerCount      int
}

// voterStateForWaiters informs whether WaitForEval is required to wait for
// eval-tokens for a voter.
type voterStateForWaiters struct {
	stateForWaiters
	isLeader      bool
	isLeaseHolder bool
}

// stateForWaiters informs whether WaitForEval is required to wait for
// eval-tokens for a replica.
type stateForWaiters struct {
	replicaID roachpb.ReplicaID
	// !isStateReplicate replicas are not required to be waited on for
	// evaluating elastic work.
	//
	// For voters, we ensure the following invariant: !isStateReplicate =>
	// hasSendQ. Since, hasSendQ voters are not included in the quorum, this
	// ensures that !isStateReplicate are not in the quorum. This is done since
	// voters that are down will tend to have all the eval tokens returned by
	// their streams, so will have positive eval tokens. We don't want to
	// erroneously think that these are actually part of the quorum.
	isStateReplicate bool
	// When hasSendQ is true, the voter is not included as part of the quorum.
	hasSendQ         bool
	evalTokenCounter *tokenCounter
}

type voterSet []voterStateForWaiters

var _ RangeController = &rangeController{}

func NewRangeController(
	ctx context.Context, o RangeControllerOptions, init RangeControllerInitState,
) *rangeController {
	if log.V(1) {
		log.VInfof(ctx, 1, "r%v creating range controller", o.RangeID)
	}
	if o.RaftMaxInflightBytes == 0 {
		o.RaftMaxInflightBytes = math.MaxUint64
	}
	rc := &rangeController{
		opts:            o,
		term:            init.Term,
		leaseholder:     init.Leaseholder,
		nextRaftIndex:   init.NextRaftIndex,
		forceFlushIndex: init.ForceFlushIndex,
		replicaMap:      make(map[roachpb.ReplicaID]*replicaState),
	}
	rc.scheduledMu.replicas = make(map[roachpb.ReplicaID]struct{})
	rc.mu.waiterSetRefreshCh = make(chan struct{})
	rc.mu.lastSendQueueStats = make(RangeSendQueueStats, 0, len(init.ReplicaSet))
	rc.updateReplicaSetRaftMuLocked(ctx, init.ReplicaSet)
	rc.updateWaiterSetsRaftMuLocked()
	rc.opts.RangeControllerMetrics.Count.Inc(1)
	return rc
}

// WaitForEval implements RangeController.WaitForEval.
func (rc *rangeController) WaitForEval(
	ctx context.Context, pri admissionpb.WorkPriority,
) (waited bool, err error) {
	expensiveLoggingEnabled := log.ExpensiveLogEnabled(ctx, 2)
	wc := admissionpb.WorkClassFromPri(pri)
	rc.opts.EvalWaitMetrics.OnWaiting(wc)
	waitCategory, waitCategoryChangeCh := rc.opts.WaitForEvalConfig.Current()
	bypass := waitCategory.Bypass(wc)
	if knobs := rc.opts.Knobs; knobs != nil &&
		knobs.OverrideBypassAdmitWaitForEval != nil {
		// This is used by some tests to bypass the wait for eval, for two different
		// purposes:
		// - to get the entry into HandleRaftEventRaftMuLocked, where normally the
		//   request would block here.
		// - to prevent the entry from being subject to replication admission
		//   control.
		bypass, waited = rc.opts.Knobs.OverrideBypassAdmitWaitForEval(ctx)
	}
	if bypass {
		if expensiveLoggingEnabled {
			log.VEventf(ctx, 2, "r%v/%v bypassed request (pri=%v)",
				rc.opts.RangeID, rc.opts.LocalReplicaID, pri)
		}
		rc.opts.EvalWaitMetrics.OnBypassed(wc, 0 /* duration */)
		return waited, nil
	}
	waitForAllReplicateHandles := false
	if wc == admissionpb.ElasticWorkClass {
		waitForAllReplicateHandles = true
	}
	var handlesScratch [5]tokenWaitingHandleInfo
	handles := handlesScratch[:]
	var scratch [7]reflect.SelectCase
	selectCasesScratch := scratch[:0:cap(scratch)]

	start := rc.opts.Clock.PhysicalTime()
retry:
	// Snapshot the waiter sets and the refresh channel.
	rc.mu.RLock()
	vss := rc.mu.voterSets
	nvs := rc.mu.nonVoterSet
	refreshCh := rc.mu.waiterSetRefreshCh
	rc.mu.RUnlock()

	if refreshCh == nil {
		// RangeControllerImpl is closed.
		waitDuration := rc.opts.Clock.PhysicalTime().Sub(start)
		if expensiveLoggingEnabled {
			log.VEventf(ctx, 2,
				"r%v/%v bypassed request as RC closed (pri=%v wait-duration=%v)",
				rc.opts.RangeID, rc.opts.LocalReplicaID, pri, waitDuration)
		}
		rc.opts.EvalWaitMetrics.OnBypassed(wc, waitDuration)
		return false, nil
	}
	for _, vs := range vss {
		quorumCount := (len(vs) + 2) / 2
		votersHaveEvalTokensCount := 0
		handles = handles[:0]
		requiredWait := false
		// First check the voter set, which participate in quorum.
		for _, v := range vs {
			available, handle := v.evalTokenCounter.TokensAvailable(wc)
			if available && !v.hasSendQ {
				votersHaveEvalTokensCount++
				continue
			}

			// Don't have eval tokens, and have a handle OR have a send-queue and no handle.
			handleInfo := tokenWaitingHandleInfo{
				handle: handle,
				requiredWait: v.isLeader || v.isLeaseHolder ||
					(waitForAllReplicateHandles && v.isStateReplicate),
				partOfQuorum: !v.hasSendQ,
			}
			handles = append(handles, handleInfo)
			if !requiredWait && handleInfo.requiredWait {
				requiredWait = true
			}
		}
		// If we don't need to wait for all replicate handles, then we will never
		// wait the non-voter streams having positive tokens, so we can skip
		// checking them.
		if waitForAllReplicateHandles {
			for _, nv := range nvs {
				available, handle := nv.evalTokenCounter.TokensAvailable(wc)
				if available || !nv.isStateReplicate {
					// Ignore non-voters without tokens which are not in StateReplicate,
					// as we won't be waiting for them.
					continue
				}
				// Don't have eval tokens, and have a handle for the non-voter.
				handleInfo := tokenWaitingHandleInfo{
					handle:       handle,
					requiredWait: true,
					partOfQuorum: false,
				}
				handles = append(handles, handleInfo)
				if !requiredWait {
					// NB: requiredWait won't always be true before here, because it is
					// possible that the leaseholder and leader have tokens, but all
					// other (non-)voters are not in StateReplicate or have tokens
					// available.
					requiredWait = true
				}
			}
		}

		remainingForQuorum := quorumCount - votersHaveEvalTokensCount
		if remainingForQuorum < 0 {
			remainingForQuorum = 0
		}
		if remainingForQuorum > 0 || requiredWait {
			var state WaitEndState
			// We may call WaitForEval with a remainingForQuorum count higher than
			// the number of handles that have partOfQuorum set to true. This can
			// happen when not enough replicas are in StateReplicate, or not enough
			// have no send-queue. This is acceptable in that the callee will end up
			// waiting on the refreshCh.
			//
			// NB: we are using expensiveLoggingEnabled for the traceIndividualWaits
			// parameter, to output trace statements, since expensiveLoggingEnabled
			// is a superset of when tracing is enabled (and in a production setting
			// is likely to be identical, so there isn't much waste).
			state, selectCasesScratch = WaitForEval(ctx, waitCategoryChangeCh, refreshCh, handles,
				remainingForQuorum, expensiveLoggingEnabled, selectCasesScratch)
			switch state {
			case WaitSuccess:
				continue
			case ContextCanceled:
				waitDuration := rc.opts.Clock.PhysicalTime().Sub(start)
				if expensiveLoggingEnabled {
					log.VEventf(ctx, 2, "r%v/%v canceled request (pri=%v wait-duration=%v)",
						rc.opts.RangeID, rc.opts.LocalReplicaID, pri, waitDuration)
				}
				rc.opts.EvalWaitMetrics.OnErrored(wc, waitDuration)
				return false, ctx.Err()
			case ConfigRefreshWaitSignaled:
				waitCategory, waitCategoryChangeCh = rc.opts.WaitForEvalConfig.Current()
				bypass = waitCategory.Bypass(wc)
				if bypass {
					waitDuration := rc.opts.Clock.PhysicalTime().Sub(start)
					if expensiveLoggingEnabled {
						log.VEventf(ctx, 2,
							"r%v/%v bypassed request as settings changed (pri=%v wait-duration=%v)",
							rc.opts.RangeID, rc.opts.LocalReplicaID, pri, waitDuration)
					}
					rc.opts.EvalWaitMetrics.OnBypassed(wc, waitDuration)
					return false, nil
				}
				goto retry
			case ReplicaRefreshWaitSignaled:
				goto retry
			}
		}
	}
	waitDuration := rc.opts.Clock.PhysicalTime().Sub(start)
	if expensiveLoggingEnabled {
		log.VEventf(ctx, 2, "r%v/%v admitted request (pri=%v wait-duration=%v wait-for-all=%v)",
			rc.opts.RangeID, rc.opts.LocalReplicaID, pri, waitDuration, waitForAllReplicateHandles)
	}
	rc.opts.EvalWaitMetrics.OnAdmitted(wc, waitDuration)
	return true, nil
}

// raftEventForReplica is constructed for a replica iff it is in
// StateReplicate.
type raftEventForReplica struct {
	mode RaftMsgAppMode
	// Reminder: (ReplicaStateInfo.Match, ReplicaStateInfo.Next) are in-flight.
	// nextRaftIndex is where the next entry will be added.
	//
	// ReplicaStateInfo.{State, Match, InflightBytes} are the latest state.
	// ReplicaStateInfo.Next represents the state preceding this raft event,
	// i.e., it will be altered by sendingEntries. Note that InflightBytes
	// already incorporates sendingEntries -- we could choose to iterate over
	// the sending entries in constructRaftEventForReplica and compensate for
	// them, but we don't bother.
	//
	// nextRaftIndex also represents the state preceding this event, and will be
	// altered by newEntries.
	//
	// createSendStream is set to true if the replicaSendStream should be
	// (re)created.
	replicaStateInfo   ReplicaStateInfo
	nextRaftIndex      uint64
	newEntries         []entryFCState
	sendingEntries     []entryFCState
	recreateSendStream bool
	logSnapshot        raft.LogSnapshot
}

// raftEventAppendState is the general state computed from RaftEvent that is
// the same for all replicas. Used as input for computing raftEventForReplica.
type raftEventAppendState struct {
	// Computed from RaftEvent.Entries.
	newEntries []entryFCState
	// rewoundNextRaftIndex is the next raft index prior to newEntries. That is,
	// if newEntries is non-empty, this is equal to newEntries[0].Index.
	rewoundNextRaftIndex uint64
}

// existingSendStreamState is used as input in computing raftEventForReplica.
type existingSendStreamState struct {
	existsAndInStateReplicate bool
	// indexToSend is populated iff existsAndInStateReplicate.
	indexToSend uint64
}

// infinityEntryIndex is an exclusive upper-bound on the index of an actual
// entry.
const infinityEntryIndex uint64 = math.MaxUint64

// constructRaftEventForReplica is called iff latestFollowerStateInfo.State is
// StateReplicate.
//
// latestReplicaStateInfo includes the effect of RaftEvent.Entries and
// RaftEvent.MsgApps on ReplicaStateInfo.Next. msgApps is the map entry in
// RaftEvent.MsgApps for this replica. For other parameters, see the struct
// declarations.
func constructRaftEventForReplica(
	ctx context.Context,
	mode RaftMsgAppMode,
	raftEventAppendState raftEventAppendState,
	latestReplicaStateInfo ReplicaStateInfo,
	existingSendStreamState existingSendStreamState,
	msgApps []raftpb.Message,
	logSnapshot raft.LogSnapshot,
	scratchSendingEntries []entryFCState,
) (_ raftEventForReplica, scratch []entryFCState) {
	firstNewEntryIndex, lastNewEntryIndex := infinityEntryIndex, infinityEntryIndex
	if n := len(raftEventAppendState.newEntries); n > 0 {
		firstNewEntryIndex = raftEventAppendState.newEntries[0].id.index
		lastNewEntryIndex = raftEventAppendState.newEntries[n-1].id.index + 1
		// The rewoundNextRaftIndex + newEntries should never lag behind Next.
		if latestReplicaStateInfo.Next > lastNewEntryIndex {
			panic(errors.AssertionFailedf("unexpected next=%v > last_new_entry_index=%v",
				latestReplicaStateInfo.Next, lastNewEntryIndex))
		}
	} else {
		if latestReplicaStateInfo.Next > raftEventAppendState.rewoundNextRaftIndex {
			panic(errors.AssertionFailedf("unexpected next=%v > rewound_next_raft_index=%v",
				latestReplicaStateInfo.Next, raftEventAppendState.rewoundNextRaftIndex))
		}
	}

	// Note that latestFollowerStateInfo.Next reflects the effects of msgApps,
	// if any, but that msgApps may not be consistent. We use msgApps only if
	// the replicaSendStream already exists and the msgApps are consistent with
	// its indexToSend, and are internally consistent etc. Else, we (re)create
	// the replicaSendStream and base the computation of sendingEntries only on
	// the latest Next and the newEntries.
	var createSendStream bool
	// [msgAppFirstIndex, msgAppUBIndex) are being sent.
	var msgAppFirstIndex, msgAppUBIndex uint64
	if existingSendStreamState.existsAndInStateReplicate {
		initialized := false
		for i := range msgApps {
			for _, entry := range msgApps[i].Entries {
				if !initialized {
					msgAppFirstIndex = entry.Index
					msgAppUBIndex = msgAppFirstIndex + 1
					initialized = true
				} else {
					if entry.Index != msgAppUBIndex {
						createSendStream = true
					} else {
						msgAppUBIndex++
					}
				}
			}
		}
		if !createSendStream {
			if msgAppFirstIndex < msgAppUBIndex {
				// First disjunct is true if there is a regression or forward jump in
				// the send-queue.
				if existingSendStreamState.indexToSend != msgAppFirstIndex ||
					// Unclear if the following can ever occur, but we are being
					// defensive.
					latestReplicaStateInfo.Next != msgAppUBIndex ||
					msgAppFirstIndex > raftEventAppendState.rewoundNextRaftIndex ||
					msgAppUBIndex > lastNewEntryIndex {
					createSendStream = true
				}
			} else {
				// No MsgApp, but Next may not be as expected.
				if existingSendStreamState.indexToSend != latestReplicaStateInfo.Next {
					createSendStream = true
				}
			}
		}
	} else {
		createSendStream = true
	}
	next := latestReplicaStateInfo.Next
	if createSendStream {
		if next > raftEventAppendState.rewoundNextRaftIndex {
			// NB: will never happen in pull mode, except for leader (which is
			// always in push mode).
			if buildutil.CrdbTestBuild && mode == MsgAppPull {
				panic(errors.AssertionFailedf("next %d > rewoundNextRaftIndex %d in pull mode",
					next, raftEventAppendState.rewoundNextRaftIndex))
			}
			//
			// We initialize the send-queue to be empty.
			next = raftEventAppendState.rewoundNextRaftIndex
			// At least one entry is "sent".
			msgAppFirstIndex = next
			msgAppUBIndex = latestReplicaStateInfo.Next
		} else {
			// NB: always the case in pull mode.
			//
			// next is in the past. No need to change it. Nothing is "sent".
			msgAppFirstIndex = 0
			msgAppUBIndex = 0
		}
	} else {
		next = existingSendStreamState.indexToSend
	}
	scratch = scratchSendingEntries
	var sendingEntries []entryFCState
	if msgAppFirstIndex < msgAppUBIndex {
		// NB: never in pull mode.
		if buildutil.CrdbTestBuild && mode == MsgAppPull {
			panic(errors.AssertionFailedf("msgAppFirstIndex %d < msgAppUBIndex %d in pull mode",
				msgAppFirstIndex, msgAppUBIndex))
		}
		if msgAppFirstIndex == firstNewEntryIndex {
			// Common case. Sub-slice and don't use scratch.
			// We've already ensured that msgAppUBIndex is <= lastNewEntryIndex.
			sendingEntries = raftEventAppendState.newEntries[0 : msgAppUBIndex-msgAppFirstIndex]
		} else {
			sendingEntries = scratchSendingEntries[:0]
			// We've already ensured that msgAppFirstIndex <= firstNewEntryIndex.
			for i := range msgApps {
				for _, entry := range msgApps[i].Entries {
					if entry.Index >= firstNewEntryIndex {
						sendingEntries = append(
							sendingEntries, raftEventAppendState.newEntries[entry.Index-firstNewEntryIndex])
					} else {
						// Need to decode.
						sendingEntries = append(sendingEntries, getEntryFCStateOrFatal(ctx, entry))
					}
				}
			}
			scratch = sendingEntries
		}
	}
	refr := raftEventForReplica{
		mode: mode,
		replicaStateInfo: ReplicaStateInfo{
			State:         latestReplicaStateInfo.State,
			Match:         latestReplicaStateInfo.Match,
			Next:          next,
			InflightBytes: latestReplicaStateInfo.InflightBytes,
		},
		nextRaftIndex:      raftEventAppendState.rewoundNextRaftIndex,
		newEntries:         raftEventAppendState.newEntries,
		sendingEntries:     sendingEntries,
		recreateSendStream: createSendStream,
		logSnapshot:        logSnapshot,
	}
	return refr, scratch
}

// HandleRaftEventRaftMuLocked handles the provided raft event for the range.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) HandleRaftEventRaftMuLocked(ctx context.Context, e RaftEvent) error {
	// NB: e.Term can be empty when the RaftEvent was not constructed using a
	// MsgStorageAppend. Hence, the assertion is gated on the conditions that
	// ensure e.Term was initialized.
	//
	// The e.Term is the leader term with which the log is consistent, and since
	// this is the leader (at rangeController.term), the terms must match. Note
	// that it is the responsibility of the caller to create a new
	// rangeController on the leader when the term changes.
	if (len(e.Entries) != 0 || e.Snap != nil) && e.Term != rc.term {
		panic(errors.AssertionFailedf("term mismatch: RaftEvent.Term %d != rangeController.Term %d",
			e.Term, rc.term))
	}
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	// Compute the flow control state for each new entry. We do this once
	// here, instead of decoding each entry multiple times for all replicas.
	numEntries := len(e.Entries)
	if cap(rc.entryFCStateScratch) < numEntries {
		rc.entryFCStateScratch = make([]entryFCState, 0, 2*numEntries)
	}
	newEntries := rc.entryFCStateScratch[:numEntries:numEntries]
	// needsTokens tracks which classes need tokens for the new entries. This
	// informs first-pass decision-making on replicas that don't have
	// send-queues, in MsgAppPull mode, and therefore can potentially send the
	// new entries.
	var needsTokens [admissionpb.NumWorkClasses]bool
	for i, entry := range e.Entries {
		newEntries[i] = getEntryFCStateOrFatal(ctx, entry)
		if newEntries[i].usesFlowControl {
			needsTokens[WorkClassFromRaftPriority(newEntries[i].pri)] = true
		}
	}
	rewoundNextRaftIndex := rc.nextRaftIndex
	if n := len(e.Entries); n > 0 {
		rc.nextRaftIndex = e.Entries[n-1].Index + 1
	}
	appendState := raftEventAppendState{
		newEntries:           newEntries,
		rewoundNextRaftIndex: rewoundNextRaftIndex,
	}

	shouldWaitChange := false
	voterSets := rc.mu.voterSets
	numSets := len(voterSets)
	var votersContributingToQuorum [2]int
	var numOptionalForceFlushes [2]int
	for r, rs := range rc.replicaMap {
		info := e.ReplicasStateInfo[r]
		// Defensive, since we already clear the scratchEvent later in this method.
		// Intended invariant: scratchEvent is always empty except in this method.
		rs.scratchEvent = raftEventForReplica{}
		mode := e.MsgAppMode
		if info.State == tracker.StateReplicate {
			// The leader won't have a MsgApp for itself, so we need to construct a
			// MsgApp for the leader, containing all the entries. The leader always
			// operates in push mode.
			var msgApps []raftpb.Message
			if r != rc.opts.LocalReplicaID {
				msgApps = e.MsgApps[r]
			} else {
				msgAppVec := [1]raftpb.Message{
					{
						Entries: e.Entries,
					},
				}
				msgApps = msgAppVec[:]
				// Leader is always in push mode.
				mode = MsgAppPush
			}
			existingSSState := rs.getExistingSendStreamStateRaftMuLocked()
			rs.scratchEvent, rs.scratchSendingEntries = constructRaftEventForReplica(
				ctx, mode, appendState, info, existingSSState, msgApps, e.LogSnapshot, rs.scratchSendingEntries)
			info = rs.scratchEvent.replicaStateInfo
		}
		shouldWaitChange = rs.handleReadyStateRaftMuLocked(
			ctx, mode, info, rs.scratchEvent.nextRaftIndex, rs.scratchEvent.recreateSendStream) || shouldWaitChange

		if e.MsgAppMode == MsgAppPull && rs.desc.IsAnyVoter() {
			// Compute state and first-pass decision on force-flushing and sending
			// the new entries.
			rs.scratchVoterStreamState = rs.computeReplicaStreamStateRaftMuLocked(ctx, needsTokens)
			if (rs.scratchVoterStreamState.noSendQ && rs.scratchVoterStreamState.hasSendTokens) ||
				rs.scratchVoterStreamState.forceFlushStopIndex.active() {
				if rs.desc.IsVoterOldConfig() {
					votersContributingToQuorum[0]++
					if rs.scratchVoterStreamState.forceFlushStopIndex.untilInfinity() &&
						!rs.scratchVoterStreamState.forceFlushBecauseLeaseholder {
						numOptionalForceFlushes[0]++
					}
				}
				if numSets > 1 && rs.desc.IsVoterNewConfig() {
					votersContributingToQuorum[1]++
					if rs.scratchVoterStreamState.forceFlushStopIndex.untilInfinity() &&
						!rs.scratchVoterStreamState.forceFlushBecauseLeaseholder {
						// We never actually use numOptionalForceFlushes[1]. Just doing this
						// for symmetry.
						numOptionalForceFlushes[1]++
					}
				}
			}
		}
	}
	if e.MsgAppMode == MsgAppPull {
		// Need to consider making force-flushing changes, or deny voters wanting
		// to form a send-queue.
		var quorumCounts [2]int
		noChangesNeeded := true
		for i := 0; i < numSets; i++ {
			quorumCounts[i] = (len(voterSets[i]) + 2) / 2
			if quorumCounts[i] > votersContributingToQuorum[i] {
				noChangesNeeded = false
			} else if quorumCounts[i] < votersContributingToQuorum[i] && numOptionalForceFlushes[i] > 0 &&
				numSets == 1 {
				// In a joint config (numSets == 2), config 0 may not need a replica
				// to force-flush, but the replica may also be in config 1 and be
				// force-flushing due to that (or vice versa). This complicates
				// computeVoterDirectivesRaftMuLocked, so under the assumption that
				// joint configs are temporary, we don't bother stopping force flushes
				// in that joint configs.
				noChangesNeeded = false
			}
			// Else, common case.
		}
		if noChangesNeeded {
			// Common case.
		} else {
			rc.computeVoterDirectivesRaftMuLocked(votersContributingToQuorum, quorumCounts)
		}
	}

	for _, rs := range rc.replicaMap {
		var rd replicaDirective
		if e.MsgAppMode == MsgAppPull {
			var ss replicaStreamState
			if rs.desc.IsAnyVoter() {
				// Have already computed this above.
				ss = rs.scratchVoterStreamState
			} else {
				// Only need to do first-pass computation for non-voters, since
				// there is no adjustment needed to ensure quorum.
				ss = rs.computeReplicaStreamStateRaftMuLocked(ctx, needsTokens)
			}
			// Make a final adjustment to start force-flushing due to
			// rc.forceFlushIndex. We deliberately leave this until the end, since
			// quorum or leaseholder requirements may already have ensured that this
			// replica must force-flush.
			//
			// NB: Next is exclusive and the first entry that has not yet been sent.
			// And forceFlushIndex is inclusive. Therefore, [Next, forceFlushIndex]
			// needs to have been sent for force-flush to not be needed, and we
			// check for non-emptiness of the interval below.
			if rs.scratchEvent.replicaStateInfo.Next <= rc.forceFlushIndex &&
				ss.isReplicate && !ss.noSendQ &&
				(!ss.forceFlushStopIndex.active() || uint64(ss.forceFlushStopIndex) < rc.forceFlushIndex) {
				ss.forceFlushStopIndex = forceFlushStopIndex(rc.forceFlushIndex)
			}
			rd = replicaDirective{
				forceFlushStopIndex:      ss.forceFlushStopIndex,
				hasSendTokens:            ss.hasSendTokens,
				preventSendQNoForceFlush: ss.preventSendQNoForceFlush,
			}
		}
		shouldWaitChange = rs.handleReadyEntriesRaftMuLocked(ctx, rs.scratchEvent, rd) || shouldWaitChange
		// Clear scratchEvent, since it contains a reference to a raft.LogSnapshot.
		rs.scratchEvent = raftEventForReplica{}
	}

	// If there was a quorum change, update the voter sets, triggering the
	// refresh channel for any requests waiting for eval tokens.
	if shouldWaitChange {
		rc.updateWaiterSetsRaftMuLocked()
	}

	if buildutil.CrdbTestBuild || rc.consistencyCheckerCount == 0 {
		rc.checkConsistencyRaftMuLocked(ctx)
	}
	rc.consistencyCheckerCount = (rc.consistencyCheckerCount + 1) % 100

	// It may have been longer than the sendQueueStatRefreshInterval since we
	// last updated the send queue stats. Maybe update them now.
	rc.maybeUpdateSendQueueStatsRaftMuLocked()

	return nil
}

// Score is tuple (bucketed tokens_send(elastic), tokens_eval(elastic)).
type replicaScore struct {
	replicaID          roachpb.ReplicaID
	bucketedTokensSend kvflowcontrol.Tokens
	tokensEval         kvflowcontrol.Tokens
}

// Second-pass decision-making.
func (rc *rangeController) computeVoterDirectivesRaftMuLocked(
	votersContributingToQuorum [2]int, quorumCounts [2]int,
) {
	var scratchFFScores, scratchCandidateFFScores, scratchDenySendQScores [5]replicaScore
	// Used to stop force-flushes if no longer needed. Never includes the
	// leaseholder, even though it may be force-flushing.
	forceFlushingScores := scratchFFScores[:0:len(scratchFFScores)]
	// Used to start force-flushes if we cannot handle the situation with
	// denying formation of send-queue.
	candidateForceFlushingScores := scratchCandidateFFScores[:0:len(scratchCandidateFFScores)]
	// Candidates who want to form a send-queue, but we will consider denying.
	// This will never include the leader or leaseholder.
	candidateDenySendQScores := scratchDenySendQScores[:0:len(scratchDenySendQScores)]
	// Compute the scores.
	for _, rs := range rc.replicaMap {
		if !rs.scratchVoterStreamState.isReplicate || !rs.desc.IsAnyVoter() {
			continue
		}
		if rs.scratchVoterStreamState.noSendQ && rs.scratchVoterStreamState.hasSendTokens {
			// NB: this also includes probeRecentlyNoSendQ.
			continue
		}
		if rs.scratchVoterStreamState.forceFlushBecauseLeaseholder {
			// No choice in whether to force-flush, so not added to any slices.
			continue
		}
		if rs.scratchVoterStreamState.forceFlushStopIndex.active() &&
			!rs.scratchVoterStreamState.forceFlushStopIndex.untilInfinity() {
			// No choice in whether to force-flush, so not added to any slices.
			continue
		}
		// INVARIANTS:
		// Voter and not leaseholder and not leader.
		// isReplicate
		// !noSendQ || !hasSendTokens
		// NB: forceFlushing => !noSendQ
		sendPoolLimit := rs.sendTokenCounter.limit(admissionpb.ElasticWorkClass)
		sendPoolBucket := sendPoolLimit / 10
		if sendPoolBucket == 0 {
			sendPoolBucket = 1
		}
		sendTokens := rs.sendTokenCounter.tokens(admissionpb.ElasticWorkClass)
		bucketedSendTokens := (sendTokens / sendPoolBucket) * sendPoolBucket
		score := replicaScore{
			replicaID:          rs.replicaID,
			bucketedTokensSend: bucketedSendTokens,
			tokensEval:         rs.evalTokenCounter.tokens(admissionpb.ElasticWorkClass),
		}
		if rs.scratchVoterStreamState.forceFlushStopIndex.active() {
			forceFlushingScores = append(forceFlushingScores, score)
		} else if rs.scratchVoterStreamState.noSendQ {
			candidateDenySendQScores = append(candidateDenySendQScores, score)
		} else {
			candidateForceFlushingScores = append(candidateForceFlushingScores, score)
		}
	}
	// Sort the scores. We include the replicaID for determinism in tests.
	if len(forceFlushingScores) > 1 {
		slices.SortFunc(forceFlushingScores, func(a, b replicaScore) int {
			return cmp.Or(cmp.Compare(a.bucketedTokensSend, b.bucketedTokensSend),
				cmp.Compare(a.tokensEval, b.tokensEval), cmp.Compare(a.replicaID, b.replicaID))
		})
	}
	if len(candidateForceFlushingScores) > 1 {
		slices.SortFunc(candidateForceFlushingScores, func(a, b replicaScore) int {
			return cmp.Or(cmp.Compare(a.bucketedTokensSend, b.bucketedTokensSend),
				cmp.Compare(a.tokensEval, b.tokensEval), cmp.Compare(a.replicaID, b.replicaID))
		})
	}
	if len(candidateDenySendQScores) > 1 {
		slices.SortFunc(candidateDenySendQScores, func(a, b replicaScore) int {
			return cmp.Or(cmp.Compare(a.bucketedTokensSend, b.bucketedTokensSend),
				cmp.Compare(a.tokensEval, b.tokensEval), cmp.Compare(a.replicaID, b.replicaID))
		})
	}
	voterSets := rc.mu.voterSets
	for i := range voterSets {
		gap := quorumCounts[i] - votersContributingToQuorum[i]
		if gap < 0 {
			// Have more than we need for quorum.
			if len(voterSets) > 1 {
				// Complicated to decide who to stop force-flushing in joint config,
				// so we don't bother.
				continue
			}
			// Stop force-flushes. Most overloaded are earlier in the slice.
			for i := range forceFlushingScores {
				if gap == 0 {
					break
				}
				// Since there is a single set, this must be a member.
				rs := rc.replicaMap[forceFlushingScores[i].replicaID]
				rs.scratchVoterStreamState.forceFlushStopIndex = 0
				gap++
			}
		} else if gap > 0 {
			// Tell someone to not form send-queue or start force-flushing.
			//
			// First try to prevent someone from forming a send-queue.
			n := len(candidateDenySendQScores)
			// Search from the back since sorted in decreasing order of overload.
			for j := n - 1; j >= 0 && gap > 0; j-- {
				rs := rc.replicaMap[candidateDenySendQScores[j].replicaID]
				var isSetMember bool
				if i == 0 {
					isSetMember = rs.desc.IsVoterOldConfig()
				} else {
					isSetMember = rs.desc.IsVoterNewConfig()
				}
				if !isSetMember {
					continue
				}
				rs.scratchVoterStreamState.hasSendTokens = true
				rs.scratchVoterStreamState.preventSendQNoForceFlush = true
				gap--
				if i == 0 && len(voterSets) > 1 && rs.desc.IsVoterNewConfig() {
					// By denying formation of a send-queue, we have also increased the
					// voters contributing to quorum for the other set.
					votersContributingToQuorum[1]++
				}
			}
			if gap > 0 {
				// Have not successfully closed the gap by stopping formation of
				// send-queue, so need to force-flush.
				n := len(candidateForceFlushingScores)
				for j := n - 1; j >= 0 && gap > 0; j-- {
					rs := rc.replicaMap[candidateForceFlushingScores[j].replicaID]
					var isSetMember bool
					if i == 0 {
						isSetMember = rs.desc.IsVoterOldConfig()
					} else {
						isSetMember = rs.desc.IsVoterNewConfig()
					}
					if !isSetMember {
						continue
					}

					rs.scratchVoterStreamState.forceFlushStopIndex = forceFlushStopIndex(infinityEntryIndex)
					rs.scratchVoterStreamState.preventSendQNoForceFlush = false
					gap--
					if i == 0 && len(voterSets) > 1 && rs.desc.IsVoterNewConfig() {
						// By force-flushing, we have also increased the voters
						// contributing to quorum for the other set.
						votersContributingToQuorum[1]++
					}
				}
			}
		}
	}
}

// HandleSchedulerEventRaftMuLocked implements RangeController.
func (rc *rangeController) HandleSchedulerEventRaftMuLocked(
	ctx context.Context, mode RaftMsgAppMode, logSnapshot raft.LogSnapshot,
) {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	var scheduledScratch [5]*replicaState
	// scheduled will contain all the replicas in scheduledMu.replicas, filtered
	// by whether they have a replicaSendStream.
	scheduled := scheduledScratch[:0:cap(scheduledScratch)]
	func() {
		rc.scheduledMu.Lock()
		defer rc.scheduledMu.Unlock()
		for r := range rc.scheduledMu.replicas {
			if rs, ok := rc.replicaMap[r]; ok && rs.sendStream != nil {
				scheduled = append(scheduled, rs)
			}
		}
		clear(rc.scheduledMu.replicas)
	}()

	// nextScheduled contains all the replicas that need to be scheduled again.
	// We reuse the scheduled slice since we only overwrite scheduled[i] after
	// it has already been read.
	nextScheduled := scheduled[:0]
	updateWaiterSets := false
	for _, rs := range scheduled {
		scheduleAgain, updateWaiters := rs.scheduledRaftMuLocked(ctx, mode, logSnapshot)
		if scheduleAgain {
			nextScheduled = append(nextScheduled, rs)
		}
		if updateWaiters {
			updateWaiterSets = true
		}
	}
	if len(nextScheduled) > 0 {
		// Need to update the scheduledMu.replicas map.
		func() {
			rc.scheduledMu.Lock()
			defer rc.scheduledMu.Unlock()
			// Call ScheduleControllerEvent on transition from empty => non-empty.
			if len(rc.scheduledMu.replicas) == 0 {
				rc.opts.Scheduler.ScheduleControllerEvent(rc.opts.RangeID)
			}
			for _, rs := range nextScheduled {
				rc.scheduledMu.replicas[rs.replicaID] = struct{}{}
			}
		}()
	}
	if updateWaiterSets {
		rc.updateWaiterSetsRaftMuLocked()
	}
	if buildutil.CrdbTestBuild {
		rc.checkConsistencyRaftMuLocked(ctx)
	}
}

// AdmitRaftMuLocked handles the notification about the given replica's
// admitted vector change. No-op if the replica is not known, or the admitted
// vector is stale (either in Term, or the indices).
//
// Requires replica.raftMu to be held.
func (rc *rangeController) AdmitRaftMuLocked(
	ctx context.Context, replicaID roachpb.ReplicaID, av AdmittedVector,
) {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	if rs, ok := rc.replicaMap[replicaID]; ok {
		rs.admitRaftMuLocked(ctx, av)
	}
}

// MaybeSendPingsRaftMuLocked implements RangeController.
func (rc *rangeController) MaybeSendPingsRaftMuLocked() {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	for id, state := range rc.replicaMap {
		if id == rc.opts.LocalReplicaID {
			continue
		}
		if s := state.sendStream; s != nil && s.holdsTokensRaftMuLocked() {
			rc.opts.RaftInterface.SendPingRaftMuLocked(id)
		}
	}
}

// HoldsSendTokensLocked implements RangeController.
func (rc *rangeController) HoldsSendTokensLocked() bool {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rc.opts.ReplicaMutexAsserter.ReplicaMuAssertHeld()
	// TODO(pav-kv): we are doing the same checks in MaybeSendPingsRaftMuLocked
	// here, and both are called from Replica.tick. We can optimize this, and do
	// both in one method.
	for _, state := range rc.replicaMap {
		if s := state.sendStream; s != nil && s.holdsTokensRaftMuLocked() {
			return true
		}
	}
	return false
}

// SetReplicasRaftMuLocked sets the replicas of the range. The caller will
// never mutate replicas, and neither should the callee.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) SetReplicasRaftMuLocked(ctx context.Context, replicas ReplicaSet) error {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rc.updateReplicaSetRaftMuLocked(ctx, replicas)
	rc.updateWaiterSetsRaftMuLocked()
	return nil
}

// SetLeaseholderRaftMuLocked sets the leaseholder of the range.
//
// Requires raftMu to be held.
func (rc *rangeController) SetLeaseholderRaftMuLocked(
	ctx context.Context, replica roachpb.ReplicaID,
) {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	if replica == rc.leaseholder {
		return
	}
	if log.V(1) {
		log.VInfof(ctx, 1, "r%v setting range leaseholder replica_id=%v", rc.opts.RangeID, replica)
	}
	rc.leaseholder = replica
	rc.updateWaiterSetsRaftMuLocked()
}

// ForceFlushIndexChangedLocked implements RangeController.
func (rc *rangeController) ForceFlushIndexChangedLocked(ctx context.Context, index uint64) {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rc.opts.ReplicaMutexAsserter.ReplicaMuAssertHeld()
	rc.forceFlushIndex = index
}

// CloseRaftMuLocked closes the range controller.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) CloseRaftMuLocked(ctx context.Context) {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	if log.V(1) {
		log.VInfof(ctx, 1, "r%v closing range controller", rc.opts.RangeID)
	}
	func() {
		rc.mu.Lock()
		defer rc.mu.Unlock()

		rc.mu.voterSets = nil
		rc.mu.nonVoterSet = nil
		close(rc.mu.waiterSetRefreshCh)
		rc.mu.waiterSetRefreshCh = nil
	}()
	// Return any tracked token deductions, as we don't expect to receive more
	// AdmittedVector updates.
	for _, rs := range rc.replicaMap {
		if rs.sendStream != nil {
			rs.closeSendStreamRaftMuLocked(ctx)
		}
	}
	rc.opts.RangeControllerMetrics.Count.Dec(1)
}

// InspectRaftMuLocked returns a handle containing the state of the range
// controller. It's used to power /inspectz-style debugging pages.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) InspectRaftMuLocked(ctx context.Context) kvflowinspectpb.Handle {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	var streams []kvflowinspectpb.ConnectedStream
	for _, rs := range rc.replicaMap {
		if rs.sendStream == nil {
			continue
		}

		func() {
			evalTokens, sendTokens := kvflowcontrol.Tokens(0), kvflowcontrol.Tokens(0)
			rs.sendStream.mu.Lock()
			defer rs.sendStream.mu.Unlock()
			// The number of send tokens deducted is equal to the number of tracked
			// tokens and the number of tokens deducted for the scheduler, if any.
			//
			// The number of eval tokens deducted is equal to the number of tokens
			// deducted for eval, conveniently stored in the replicaSendStream.
			trackedDeductions, trackedTokens := rs.sendStream.raftMu.tracker.Inspect()
			sendTokens += trackedTokens
			sendTokens += rs.sendStream.mu.sendQueue.deductedForSchedulerTokens
			for _, tokens := range rs.sendStream.mu.eval.tokensDeducted {
				evalTokens += tokens
			}
			streams = append(streams, kvflowinspectpb.ConnectedStream{
				Stream:                  rc.opts.SSTokenCounter.InspectStream(rs.stream),
				Disconnected:            rs.sendStream.mu.connectedState != replicate,
				TrackedDeductions:       trackedDeductions,
				TotalEvalDeductedTokens: int64(evalTokens),
				TotalSendDeductedTokens: int64(sendTokens),
			})
		}()
	}

	// Sort the connected streams for determinism, which some tests rely on.
	slices.SortFunc(streams, func(a, b kvflowinspectpb.ConnectedStream) int {
		return cmp.Or(
			cmp.Compare(a.Stream.TenantID.ToUint64(), b.Stream.TenantID.ToUint64()),
			cmp.Compare(a.Stream.StoreID, b.Stream.StoreID),
		)
	})

	return kvflowinspectpb.Handle{
		RangeID:          rc.opts.RangeID,
		ConnectedStreams: streams,
	}
}

// StatusRaftMuLocked returns basic information about the range controller and
// its send streams.
//
// Requires replica.raftMu to be held.
func (rc *rangeController) StatusRaftMuLocked() serverpb.RACStatus {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	status := serverpb.RACStatus{
		NextRaftIndex:   rc.nextRaftIndex,
		ForceFlushIndex: rc.forceFlushIndex,
		Streams:         map[uint64]serverpb.RACStatus_Stream{},
	}
	for id, rs := range rc.replicaMap {
		if rs.sendStream == nil {
			continue
		}
		var s serverpb.RACStatus_Stream
		func() {
			// TODO(pav-kv): locking is unnecessary, since indexToSend, nextRaftIndex,
			// and eval.tokensDeducted are always updated under raftMu. Update the
			// locking semantics in rs.sendStream.
			rs.sendStream.mu.Lock()
			defer rs.sendStream.mu.Unlock()
			s.IndexToSend = rs.sendStream.mu.sendQueue.indexToSend
			s.NextRaftIndexInitial = rs.sendStream.mu.nextRaftIndexInitial
			s.ForceFlushStopIndex = uint64(rs.sendStream.mu.sendQueue.forceFlushStopIndex)
			// Don't waste space if there are no tokens held.
			if tokens := rs.sendStream.mu.eval.tokensDeducted[:]; holdsTokens(tokens) {
				s.EvalTokensHeld = convertTokensSlice(tokens)
			}
		}()
		if rs.sendStream.holdsTokensRaftMuLocked() {
			s.SendTokensHeld = convertTokensSlice(rs.sendStream.raftMu.tracker.deducted[:])
		}
		status.Streams[uint64(id)] = s
	}
	return status
}

func holdsTokens(tokens []kvflowcontrol.Tokens) bool {
	return slices.ContainsFunc(tokens, func(tokens kvflowcontrol.Tokens) bool {
		return tokens != 0
	})
}

func convertTokensSlice(tokens []kvflowcontrol.Tokens) []int64 {
	result := make([]int64, len(tokens))
	for i, tok := range tokens {
		result[i] = int64(tok)
	}
	return result
}

func (rc *rangeController) SendStreamStats(statsToSet *RangeSendStreamStats) {
	if len(statsToSet.internal) != 0 {
		panic(errors.AssertionFailedf("statsToSet is non-empty %v", statsToSet.internal))
	}
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	statsToSet.internal = slices.Grow(statsToSet.internal, len(rc.mu.lastSendQueueStats))
	// We will update the cheaper stats to ensure they are up-to-date. For the
	// more expensive ones, we use the cached copy.
	for _, vss := range rc.mu.voterSets {
		// We loop over both voter sets, if a voter exists in both, we will just
		// end up overwriting the same state at most twice, not a big issue.
		for _, vs := range vss {
			stats := ReplicaSendStreamStats{
				IsStateReplicate: vs.isStateReplicate,
				HasSendQueue:     vs.hasSendQ,
			}
			stats.ReplicaSendQueueStats, _ = rc.mu.lastSendQueueStats.ReplicaSendQueueStats(vs.replicaID)
			statsToSet.SetReplicaSendStreamStats(stats)
		}
	}
	// Now handle the non-voters.
	for _, nv := range rc.mu.nonVoterSet {
		stats := ReplicaSendStreamStats{
			IsStateReplicate: nv.isStateReplicate,
			HasSendQueue:     nv.hasSendQ,
		}
		stats.ReplicaSendQueueStats, _ = rc.mu.lastSendQueueStats.ReplicaSendQueueStats(nv.replicaID)
		statsToSet.SetReplicaSendStreamStats(stats)
	}
}

func (rc *rangeController) maybeUpdateSendQueueStatsRaftMuLocked() {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	now := rc.opts.Clock.PhysicalTime()
	updateStats := false
	func() {
		rc.mu.Lock()
		defer rc.mu.Unlock()
		if nextUpdateTime := rc.mu.lastSendQueueStatRefresh.Add(
			sendQueueStatRefreshInterval); now.After(nextUpdateTime) ||
			(rc.opts.Knobs != nil && rc.opts.Knobs.OverrideAlwaysRefreshSendStreamStats) {
			// We should update the stats, it has been longer than
			// sendQueueStatRefreshInterval.
			updateStats = true
		}
	}()
	if !updateStats {
		// Common case.
		return
	}
	rc.updateSendQueueStatsRaftMuLocked(now)
}

func (rc *rangeController) updateSendQueueStatsRaftMuLocked(now time.Time) {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rc.lastSendQueueStatsScratch.Clear()
	for _, rs := range rc.replicaMap {
		stats := ReplicaSendQueueStats{
			ReplicaID: rs.replicaID,
		}
		if rs.sendStream != nil {
			func() {
				rs.sendStream.mu.Lock()
				defer rs.sendStream.mu.Unlock()
				stats.SendQueueBytes = int64(rs.sendStream.approxQueueSizeStreamLocked())
				stats.SendQueueCount = rs.sendStream.queueLengthRaftMuAndStreamLocked()
			}()
		}
		rc.lastSendQueueStatsScratch.Set(stats)
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.mu.lastSendQueueStats, rc.lastSendQueueStatsScratch =
		rc.lastSendQueueStatsScratch, rc.mu.lastSendQueueStats
	rc.mu.lastSendQueueStatRefresh = now
}

func (rc *rangeController) updateReplicaSetRaftMuLocked(ctx context.Context, newSet ReplicaSet) {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	var toAdd, toRemove [5]roachpb.ReplicaID
	add := toAdd[:0:len(toAdd)]
	remove := toRemove[:0:len(toRemove)]
	prevSet := rc.replicaSet
	for r := range prevSet {
		desc, ok := newSet[r]
		if !ok {
			if rs := rc.replicaMap[r]; rs.sendStream != nil {
				// The replica is no longer part of the range, so we don't expect any
				// tracked token deductions to be returned. Return them now.
				rs.closeSendStreamRaftMuLocked(ctx)
			}
			delete(rc.replicaMap, r)
			remove = append(remove, r)
		} else {
			rs := rc.replicaMap[r]
			rs.desc = desc
		}
	}
	for r, desc := range newSet {
		_, ok := prevSet[r]
		if ok {
			// Already handled above.
			continue
		}
		newRepl := NewReplicaState(ctx, rc, desc)
		rc.replicaMap[r] = newRepl
		add = append(add, r)
	}
	rc.replicaSet = newSet

	// Acquire rc.mu since we need to update rc.mu.lastSendQueueStats.
	rc.mu.Lock()
	defer rc.mu.Unlock()
	for _, r := range remove {
		rc.mu.lastSendQueueStats.Remove(r)
	}
	for _, r := range add {
		rc.mu.lastSendQueueStats.Set(ReplicaSendQueueStats{
			ReplicaID: r,
			// NOTE: We leave the SendQueue(Bytes|Count) unpopulated, they will be updated
			// on the next call to updateSendQueueStats, which is at most
			// sendQueueStatRefreshInterval duration from now.
		})
	}
}

func (rc *rangeController) updateWaiterSetsRaftMuLocked() {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rc.mu.Lock()
	defer rc.mu.Unlock()

	setCount := 1
	for _, r := range rc.replicaSet {
		isOld := r.IsVoterOldConfig()
		isNew := r.IsVoterNewConfig()
		if !isOld && !isNew {
			continue
		}
		if !isOld && isNew {
			setCount++
			break
		}
	}
	var voterSets []voterSet
	var nonVoterSet []stateForWaiters
	for len(voterSets) < setCount {
		voterSets = append(voterSets, voterSet{})
	}
	for _, r := range rc.replicaSet {
		isOld := r.IsVoterOldConfig()
		isNew := r.IsVoterNewConfig()

		rs := rc.replicaMap[r.ReplicaID]
		isStateReplicate, hasSendQ := rs.isStateReplicateAndSendQRaftMuLocked()
		waiterState := stateForWaiters{
			replicaID:        r.ReplicaID,
			isStateReplicate: isStateReplicate,
			evalTokenCounter: rs.evalTokenCounter,
			hasSendQ:         hasSendQ,
		}

		if r.IsNonVoter() {
			nonVoterSet = append(nonVoterSet, waiterState)
		}

		if !isOld && !isNew {
			continue
		}

		// Is a voter.
		vsfw := voterStateForWaiters{
			stateForWaiters: waiterState,
			isLeader:        r.ReplicaID == rc.opts.LocalReplicaID,
			isLeaseHolder:   r.ReplicaID == rc.leaseholder,
		}
		if isOld {
			voterSets[0] = append(voterSets[0], vsfw)
		}
		if isNew && setCount == 2 {
			voterSets[1] = append(voterSets[1], vsfw)
		}
	}
	rc.mu.voterSets = voterSets
	rc.mu.nonVoterSet = nonVoterSet
	close(rc.mu.waiterSetRefreshCh)
	rc.mu.waiterSetRefreshCh = make(chan struct{})
}

// scheduleReplica may be called with or without raftMu held.
func (rc *rangeController) scheduleReplica(r roachpb.ReplicaID) {
	rc.scheduledMu.Lock()
	defer rc.scheduledMu.Unlock()

	wasEmpty := len(rc.scheduledMu.replicas) == 0
	rc.scheduledMu.replicas[r] = struct{}{}
	if wasEmpty && len(rc.scheduledMu.replicas) == 1 {
		// Call ScheduleControllerEvent on transition from empty => non-empty.
		rc.opts.Scheduler.ScheduleControllerEvent(rc.opts.RangeID)
	}
}

// checkConsistencyRaftMuLocked is an expensive function to check consistency.
func (rc *rangeController) checkConsistencyRaftMuLocked(ctx context.Context) {
	rc.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	if rc.consistencyCheckerScratchMap == nil {
		rc.consistencyCheckerScratchMap = map[roachpb.ReplicaID]stateForWaiters{}
	}
	// replicas contains everything in rc.mu.voterSets and rc.mu.nonVoterSet.
	replicas := rc.consistencyCheckerScratchMap
	clear(replicas)
	func() {
		var leaderID, leaseholderID roachpb.ReplicaID
		rc.mu.RLock()
		defer rc.mu.RUnlock()
		for _, vs := range rc.mu.voterSets {
			for _, v := range vs {
				if v.isLeader {
					if leaderID == 0 {
						leaderID = v.replicaID
					} else if v.replicaID != leaderID {
						panic(errors.AssertionFailedf("two leaders: %s and %s", leaderID, v.replicaID))
					}
				}
				if v.isLeaseHolder {
					if leaseholderID == 0 {
						leaseholderID = v.replicaID
					} else if v.replicaID != leaseholderID {
						panic(errors.AssertionFailedf(
							"two leaseholders: %s and %s", leaseholderID, v.replicaID))
					}
				}
				replicas[v.replicaID] = v.stateForWaiters
			}
		}
		for _, nv := range rc.mu.nonVoterSet {
			replicas[nv.replicaID] = nv
		}
	}()
	// Check that every member of replicas is also in rc.replicaMap and
	// rc.replicaSet, and that the state is consistent.
	for _, state := range replicas {
		rs, ok := rc.replicaMap[state.replicaID]
		if !ok {
			panic(errors.AssertionFailedf("replica %s not in replicaMap", state.replicaID))
		}
		isStateReplicate, hasSendQ := rs.isStateReplicateAndSendQRaftMuLocked()
		if state.isStateReplicate != isStateReplicate {
			panic(errors.AssertionFailedf("inconsistent isStateReplicate: %t, %t",
				state.isStateReplicate, isStateReplicate))
		}
		if state.hasSendQ != hasSendQ {
			panic(errors.AssertionFailedf("inconsistent hasSendQ: %t, %t",
				state.hasSendQ, hasSendQ))
		}
		_, ok = rc.replicaSet[state.replicaID]
		if !ok {
			panic(errors.AssertionFailedf("replica %s not in replicaSet", state.replicaID))
		}
	}
	// Check that every member of rc.replicaMap is in rc.replicaSet, and if it
	// is a voter or non-voter it is in replicas. Additionally check each
	// replicaSendStream for internal consistency.
	for replicaID, rs := range rc.replicaMap {
		if rs.desc.IsAnyVoter() || rs.desc.IsNonVoter() {
			_, ok := replicas[replicaID]
			if !ok {
				panic(errors.AssertionFailedf("replica %s not in voter or non-voter sets", replicaID))
			}
		}
		_, ok := rc.replicaSet[replicaID]
		if !ok {
			panic(errors.AssertionFailedf("replica %s not in replicaSet", replicaID))
		}
		rss := rs.sendStream
		if rss == nil {
			continue
		}
		// Check internal consistency of replicaSendStream.
		rss.checkConsistencyRaftMuLocked()
	}
}

// replicaState holds state for each replica. All methods are called with
// raftMu held, hence it does not have its own mutex.
type replicaState struct {
	// ==== Immutable fields ====
	parent *rangeController
	// stream aggregates across the streams for the same (tenant, store). This
	// is the identity that is used to deduct tokens or wait for tokens to be
	// positive.
	stream                             kvflowcontrol.Stream
	evalTokenCounter, sendTokenCounter *tokenCounter
	replicaID                          roachpb.ReplicaID

	// ==== Mutable fields ====
	desc roachpb.ReplicaDescriptor

	sendStream *replicaSendStream

	// Scratch space used in constructRaftEventForReplica.
	scratchSendingEntries []entryFCState

	// Scratch space for temporarily stashing state in
	// RangeController.HandleRaftEventRaftMuLocked.
	scratchEvent            raftEventForReplica
	scratchVoterStreamState replicaStreamState
}

// replicaStreamState captures the state of the stream, and the plan for what
// the stream should do.
type replicaStreamState struct {
	isReplicate bool
	noSendQ     bool
	// The remaining fields serve as output from replicaState and subsequent
	// input into replicaState.

	// forceFlushStopIndex.active() is true iff in StateReplicate and there is a
	// send-queue (!noSendQ) and is being force-flushed. When provided as
	// subsequent input, it should be interpreted as a directive that *may*
	// change the current behavior, i.e., it may be asking the stream to start a
	// force-flush or stop a force-flush.
	//
	// INVARIANT: forceFlushStopIndex.active() => !noSendQ && !hasSendTokens.
	//
	// When forceFlushStopIndex.active() is true and forceFlushStopIndex <
	// infinityEntryIndex, the force-flush is being done due to the
	// externally provided force-flush index.
	//
	// INVARIANT: forceFlushBecauseLeaseholder =>
	//   forceFlushStopIndex==infinityEntryIndex.
	forceFlushStopIndex forceFlushStopIndex
	// A true value is always a directive, that is computed in the first-pass,
	// in computeReplicaStreamStateRaftMuLocked.
	forceFlushBecauseLeaseholder bool
	// indexToSend is the state of the replicaSendStream. It is only populated
	// in StateReplicate.
	indexToSend uint64
	// True only if noSendQ. When interpreted as a directive in subsequent
	// input, it may have been changed from false to true to prevent formation
	// of a send-queue.
	hasSendTokens bool
	// preventSendQNoForceFlush is true only if noSendQ and hasSendTokens is
	// true. When interpreted as a directive in subsequent input, it may have
	// been changed from false to true to prevent formation of a send-queue.
	//
	// NB: preventSendQNoForceFlush is only relevant to observability and
	// debugging, no entry sending logic is based on it.
	preventSendQNoForceFlush bool
}

// replicaDirective is passed to a replica when we have already decided
// whether it has send tokens or should be force flushing. Only relevant for
// pull mode.
type replicaDirective struct {
	forceFlushStopIndex forceFlushStopIndex
	hasSendTokens       bool
	// preventSendQNoForceFlush is only used for observability and debugging.
	preventSendQNoForceFlush bool
}

func NewReplicaState(
	ctx context.Context, parent *rangeController, desc roachpb.ReplicaDescriptor,
) *replicaState {
	stream := kvflowcontrol.Stream{TenantID: parent.opts.TenantID, StoreID: desc.StoreID}
	rs := &replicaState{
		parent:           parent,
		stream:           stream,
		evalTokenCounter: parent.opts.SSTokenCounter.Eval(stream),
		sendTokenCounter: parent.opts.SSTokenCounter.Send(stream),
		replicaID:        desc.ReplicaID,
		desc:             desc,
	}
	// Don't bother creating the replicaSendStream here. We will do this in
	// the next Ready which will be called immediately after. This centralizes
	// the logic of replicaSendStream creation.
	return rs
}

// replicaSendStream maintains state for a replica to which we (typically) are
// actively replicating.
type replicaSendStream struct {
	parent *replicaState

	// Mutex is ordered before Replica.mu. IO is done while holding the mutex.
	//
	// TODO(sumeer): there are a number of fields inside mu, that don't need to
	// be protected by mu. Consider moving them to the raftMu struct after we
	// have raftMu assertions in this file. Note that moving them will likely
	// not reduce the size of a critical section or avoid needing
	// replicaSendStream.mu where we currently need it. So we could potentially
	// keep them under replicaSendStream.mu as a defensive mechanism.
	mu struct {
		syncutil.Mutex
		// connectedStateStart is the time when the connectedState was last
		// transitioned from replicate to probeRecentlyNoSendQ.
		connectedState      connectedState
		connectedStateStart time.Time
		mode                RaftMsgAppMode
		// nextRaftIndexInitial is the value of nextRaftIndex when this
		// replicaSendStream was created, or transitioned into replicate.
		nextRaftIndexInitial uint64
		// Eval state.
		//
		// Contains no tokens in probeRecentlyNoSendQ.
		eval struct {
			// Only for indices >= nextRaftIndexInitial. These are either in
			// the send-queue, or in the tracker.
			tokensDeducted [admissionpb.NumWorkClasses]kvflowcontrol.Tokens
		}
		// When the presence of a sendQueue is due to Raft flow control (push
		// mode), which does not take store overload into account, we consider
		// that the delay in reaching quorum due to the send-queue is acceptable.
		// The state is maintained here so that we can transition between Raft
		// flow control caused send-queue and replication flow control caused
		// send-queue, and vice versa.
		//
		// Say we pretended that there was no send-queue when using Raft flow
		// control. Then send tokens would have been deducted and entries placed
		// in tracker at eval time. When transitioning from replication flow
		// control to raft flow control we would need to iterate over all entries
		// in the send-queue, read them from storage, and place them in the
		// tracker. Which is clearly not viable.
		//
		// We have another issue when maintaining the send-queue in push-mode. We
		// do not control generation of the MsgApp, or the RaftMessageRequest in
		// which it is wrapped. So we cannot set
		// RaftMessageRequest.LowPriorityOverride. Even if we somehow fixed the
		// code plumbing difficulties in setting LowPriorityOverride, the MsgApp
		// (generated wholly by Raft in push mode) may span some entries in the
		// send-queue and some not in the send-queue, so a single bool override
		// does not apply to the whole MsgApp. To work around this, when in push
		// mode (the only mode in the current code) we use the original priority
		// when deducting eval tokens (and eventually send tokens) for entries in
		// the send-queue.
		//
		// Not updated in state probeRecentlyNoSendQ.
		sendQueue struct {
			// State of send-queue. [indexToSend, nextRaftIndex) have not been sent.
			// indexToSend == FollowerStateInfo.Next. nextRaftIndex is the current
			// value of NextUnstableIndex at the leader. The send-queue is always
			// empty for the leader.
			indexToSend   uint64
			nextRaftIndex uint64

			// Tokens corresponding to items in the send-queue that have had eval
			// tokens deducted, i.e., have indices >= nextRaftIndexInitial and are
			// subject to replication flow control.
			//
			// In push mode, we deduct based on originalEvalTokens. In pull mode,
			// all originalEvalTokens[RegularWorkClass] are deducted as elastic.
			//
			// When switching from push to pull:
			//  evalTokenCounter.Deduct(ElasticWorkClass, originalEvalTokensDeducted[RegularWorkClass])
			//  evalTokenCounter.Deduct(RegularWorkClass, -originalEvalTokensDeducted[RegularWorkClass])
			//
			// When switching from pull to push:
			//  evalTokenCounter.Deduct(ElasticWorkClass, -originalEvalTokens[RegularWorkClass])
			//  evalTokenCounter.Deduct(RegularWorkClass, originalEvalTokens[RegularWorkClass])
			//
			// Nothing in the send-queue is in the tracker, so that is unaffected.
			// When de-queuing from the send-queue and sending in push mode, we use
			// the original priority when adding to the tracker. In pull mode we use
			// LowPri.
			originalEvalTokens [admissionpb.NumWorkClasses]kvflowcontrol.Tokens

			// entryTokensApproximator approximates the tokens needed per entry, for
			// indices < nextRaftIndexInitial, in the send-queue.
			//
			// It guides how many bytes to grab in deductedForScheduler.tokens.
			entryTokensApproximator entryTokensApproximator

			// preciseSizeSum is the total size of entries subject to AC, and have
			// an index >= nextRaftIndexInitial and >= indexToSend.
			preciseSizeSum kvflowcontrol.Tokens

			// tokenWatcherHandle, deductedForSchedulerTokens, forceFlushStopIndex
			// can only be non-zero when connectedState == replicate, and the
			// send-queue is non-empty.
			//
			// INVARIANTS:
			//
			// forceFlushStopIndex.active() => tokenWatcherHandle is zero and
			// deductedForSchedulerTokens == 0.
			//
			// tokenWatcherHandle is non-zero => deductedForSchedulerTokens == 0 and
			// !forceFlushStopIndex.active().
			//
			// It follows from the above that:
			//
			// deductedForSchedulerTokens != 0 => tokenWatcherHandle is zero and
			// !forceFlushStopIndex.active()
			forceFlushStopIndex forceFlushStopIndex

			tokenWatcherHandle         SendTokenWatcherHandle
			deductedForSchedulerTokens kvflowcontrol.Tokens
		}
		// inflightBytes is the sum of bytes that are inflight, i.e., in
		// (ReplicaStateInfo.Match,ReplicaStateInfo.Next).
		inflightBytes uint64

		// TODO(sumeer): remove closed. Whenever a replicaSendStream is closed it
		// is also no longer referenced by replicaState. The only motivation for
		// closed is that replicaSendStream.Notify calls directly into
		// replicaSendStream. But closing a send stream also sets
		// replicaSendStream.mu.sendQueue.tokenWatcherHandle to empty, and Notify
		// is already a noop in that case. So this field serves no useful purpose.
		closed bool
	}
	// Fields that are read/written while holding raftMu.
	raftMu struct {
		// tracker contains entries that have been sent, and have had send-tokens
		// deducted (and have had eval-tokens deducted iff index >=
		// nextRaftIndexInitial).
		//
		// Contains no entries in probeRecentlyNoSendQ.
		tracker Tracker
	}
}

func (rss *replicaSendStream) changeConnectedStateRaftMuAndStreamLocked(
	state connectedState, now time.Time,
) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	rss.mu.connectedState = state
	rss.mu.connectedStateStart = now
}

func (rss *replicaSendStream) holdsTokensRaftMuLocked() bool {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	return !rss.raftMu.tracker.Empty()
}

func (rss *replicaSendStream) admitRaftMuLocked(ctx context.Context, av AdmittedVector) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.Lock()
	defer rss.mu.Unlock()

	returnedSend, returnedEval := rss.raftMu.tracker.Untrack(av,
		rss.mu.nextRaftIndexInitial)
	if log.V(2) {
		returnedSomething := false
		for _, tokenArray := range [2][raftpb.NumPriorities]kvflowcontrol.Tokens{returnedSend, returnedEval} {
			for _, t := range tokenArray {
				if t > 0 {
					returnedSomething = true
				}
			}
		}
		if returnedSomething {
			var b strings.Builder
			printReturned := func(prefix string, returned [raftpb.NumPriorities]kvflowcontrol.Tokens) {
				first := true
				for pri, tokens := range returned {
					if tokens > 0 {
						if first {
							fmt.Fprintf(&b, "%s:", prefix)
							first = false
						}
						fmt.Fprintf(&b, "%v:%v", raftpb.Priority(pri), tokens)
					}
				}
			}
			printReturned("send", returnedSend)
			printReturned(" eval", returnedEval)
			log.VInfof(ctx, 2, "r%v:%v stream %v admit %v returned %s",
				rss.parent.parent.opts.RangeID, rss.parent.desc, rss.parent.stream, av,
				redact.SafeString(b.String()))
		}
	}
	rss.returnSendTokensRaftMuAndStreamLocked(ctx, returnedSend, false /* disconnect */)
	rss.returnEvalTokensRaftMuAndStreamLocked(ctx, returnedEval)
}

func (rs *replicaState) getExistingSendStreamStateRaftMuLocked() existingSendStreamState {
	rs.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	if rs.sendStream == nil {
		return existingSendStreamState{
			existsAndInStateReplicate: false,
		}
	}
	rs.sendStream.mu.Lock()
	defer rs.sendStream.mu.Unlock()
	if rs.sendStream.mu.connectedState != replicate {
		return existingSendStreamState{
			existsAndInStateReplicate: false,
		}
	}
	return existingSendStreamState{
		existsAndInStateReplicate: true,
		indexToSend:               rs.sendStream.mu.sendQueue.indexToSend,
	}
}

func (rs *replicaState) createReplicaSendStreamRaftMuLocked(
	ctx context.Context, mode RaftMsgAppMode, indexToSend uint64, nextRaftIndex uint64,
) {
	rs.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	// Must be in StateReplicate on creation.
	if log.ExpensiveLogEnabled(ctx, 1) {
		log.VEventf(ctx, 1, "r%v creating send stream %v for replica %v",
			rs.parent.opts.RangeID, rs.stream, rs.desc)
	}
	rs.sendStream = &replicaSendStream{
		parent: rs,
	}
	rss := rs.sendStream
	// NB: need to lock rss.mu due to (a) assertions in some of the methods
	// called below, and (b) since
	// startAttemptingToEmptySendQueueViaWatcherStreamLocked can hand a reference to
	// rss to a different goroutine, which can start running immediately.
	rss.mu.Lock()
	defer rss.mu.Unlock()
	rss.raftMu.tracker.Init(rs.parent.term, rs.stream)
	rss.mu.closed = false
	rss.changeConnectedStateRaftMuAndStreamLocked(replicate, rs.parent.opts.Clock.PhysicalTime())
	rss.mu.mode = mode
	rss.mu.nextRaftIndexInitial = nextRaftIndex
	rss.mu.sendQueue.indexToSend = indexToSend
	rss.mu.sendQueue.nextRaftIndex = nextRaftIndex
	if mode == MsgAppPull && !rs.sendStream.isEmptySendQueueStreamLocked() {
		rss.startAttemptingToEmptySendQueueViaWatcherStreamLocked(ctx)
	}
}

func (rs *replicaState) isStateReplicateAndSendQRaftMuLocked() (isStateReplicate, hasSendQ bool) {
	rs.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	if rs.sendStream == nil {
		return false, true
	}
	rs.sendStream.mu.Lock()
	defer rs.sendStream.mu.Unlock()
	isStateReplicate = rs.sendStream.mu.connectedState == replicate
	if isStateReplicate {
		hasSendQ = !rs.sendStream.isEmptySendQueueStreamLocked()
	} else {
		// For WaitForEval, we treat probeRecentlyNoSendQ as having a send-queue
		// and not part of the quorum. We don't want to keep evaluating and pile
		// up work. Note, that this is the exact opposite of how
		// probeRecentlyNoSendQ behaves wrt contributing to the quorum when
		// deciding to force-flush.
		hasSendQ = true
	}
	return isStateReplicate, hasSendQ
}

type entryFCState struct {
	id              entryID
	usesFlowControl bool
	tokens          kvflowcontrol.Tokens
	pri             raftpb.Priority
}

// getEntryFCStateOrFatal returns the given entry's flow control state. If the
// entry encoding cannot be determined, a fatal is logged.
func getEntryFCStateOrFatal(ctx context.Context, entry raftpb.Entry) entryFCState {
	enc, pri, err := raftlog.EncodingOf(entry)
	if err != nil {
		log.Fatalf(ctx, "error getting encoding of entry: %v", err)
	}

	if enc == raftlog.EntryEncodingStandardWithAC || enc == raftlog.EntryEncodingSideloadedWithAC {
		// When the entry is encoded with the v1 encoding, we don't have access to
		// the priority via the priority bit and would need to decode the admission
		// metadata. Instead, assume the priority is low priority, which is the
		// only sane flow control priority enforcement level in v1 (elastic only).
		pri = raftpb.LowPri
	}

	return entryFCState{
		id:              entryID{index: entry.Index, term: entry.Term},
		usesFlowControl: enc.UsesAdmissionControl(),
		tokens:          kvflowcontrol.Tokens(len(entry.Data)),
		pri:             pri,
	}
}

// computeReplicaStreamStateRaftMuLocked computes the current state of the
// stream and a first-pass decision on what the stream should do. Called for
// all replicas when in pull mode.
func (rs *replicaState) computeReplicaStreamStateRaftMuLocked(
	ctx context.Context, needsTokens [admissionpb.NumWorkClasses]bool,
) replicaStreamState {
	rs.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	if rs.sendStream == nil {
		// This is the zero value of replicaStreamState. Listed for readability.
		return replicaStreamState{
			isReplicate:                  false,
			noSendQ:                      false,
			forceFlushStopIndex:          0,
			forceFlushBecauseLeaseholder: false,
			indexToSend:                  0,
			hasSendTokens:                false,
			preventSendQNoForceFlush:     false,
		}
	}
	rss := rs.sendStream
	rss.mu.Lock()
	defer rss.mu.Unlock()
	if rss.mu.connectedState == probeRecentlyNoSendQ {
		return replicaStreamState{
			// Pretend.
			isReplicate: true,
			// Pretend has no send-queue and has tokens, to delay any other stream
			// from having to force-flush.
			//
			// NB: this pretense is helpful in delaying force-flush, but we don't
			// need this pretense in deciding whether to prevent another
			// replicaSendStream from forming a send-queue. But doing separate logic
			// for these two situations is more complicated, and we accept the
			// slight increase in latency when applying this behavior in the latter
			// situation.
			noSendQ:       true,
			hasSendTokens: true,
		}
	}
	vss := replicaStreamState{
		isReplicate:              true,
		noSendQ:                  rss.isEmptySendQueueStreamLocked(),
		forceFlushStopIndex:      rss.mu.sendQueue.forceFlushStopIndex,
		indexToSend:              rss.mu.sendQueue.indexToSend,
		preventSendQNoForceFlush: false,
	}
	if rs.replicaID == rs.parent.leaseholder {
		if vss.noSendQ {
			// The first-pass itself decides that we need to send.
			vss.hasSendTokens = true
		} else {
			// The leaseholder may not be force-flushing yet, but this will start
			// force-flushing.
			vss.forceFlushStopIndex = forceFlushStopIndex(infinityEntryIndex)
			vss.forceFlushBecauseLeaseholder = true
		}
		return vss
	}
	if rs.replicaID == rs.parent.opts.LocalReplicaID {
		// Leader.
		vss.hasSendTokens = true
		return vss
	}
	// Non-leaseholder and non-leader replica.
	if vss.noSendQ {
		vss.hasSendTokens = true
		// If tokens are available, that is > 0, we decide we can send all the new
		// entries. This allows for a burst, but it is too complicated to make a
		// tentative decision to send now, and reverse it later (the quorum
		// computation depends on not reversing this decision). To allow for
		// reversing the decision (since some other range could have taken these
		// tokens until we get to sending), we would need to iterate the decision
		// computation until we converge, which would be bad for performance.
		//
		// Alternatively, we could deduct the tokens now, but it introduces the
		// slight code complexity of sending only some of the new entries. We'd
		// rather send all or nothing.
		//
		// This admits a burst, in that we will get into a deficit, and then
		// because of that deficit, form a send-queue, and will need to both (a)
		// pay back the deficit, (b) have enough tokens to empty the send-queue,
		// before the send-queue disappears. The positive side of this is that the
		// frequency of flapping between send-queue and no send-queue is reduced,
		// which means the WaitForEval refreshCh needs to be used less frequently.
		if needsTokens[admissionpb.ElasticWorkClass] {
			if rs.sendTokenCounter.tokens(admissionpb.ElasticWorkClass) <= 0 {
				vss.hasSendTokens = false
			}
		}
		if needsTokens[admissionpb.RegularWorkClass] {
			if rs.sendTokenCounter.tokens(admissionpb.RegularWorkClass) <= 0 {
				vss.hasSendTokens = false
			}
		}
	}
	return vss
}

func (rs *replicaState) handleReadyEntriesRaftMuLocked(
	ctx context.Context, eventForReplica raftEventForReplica, directive replicaDirective,
) (transitionedSendQState bool) {
	rs.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	if rs.sendStream == nil {
		return false
	}

	rs.sendStream.mu.Lock()
	defer rs.sendStream.mu.Unlock()
	if rs.sendStream.mu.connectedState != replicate {
		return false
	}
	transitionedSendQState, err :=
		rs.sendStream.handleReadyEntriesRaftMuAndStreamLocked(ctx, eventForReplica, directive)
	if err != nil {
		// Transitioned to StateSnapshot, or some other error that Raft needs to
		// deal with.
		rs.sendStream.closeRaftMuAndStreamLocked(ctx)
		rs.sendStream = nil
		transitionedSendQState = true
	}
	return transitionedSendQState
}

// handleReadyStateRaftMuLocked handles state management for the replica based
// on the provided follower state information. If the state changes in a way
// that affects requests waiting for evaluation, returns true. mode,
// nextRaftIndex and recreateSendStream are only relevant when info.State is
// StateReplicate. mode, info.Next, nextRaftIndex are only used when
// recreateSendStream is true.
func (rs *replicaState) handleReadyStateRaftMuLocked(
	ctx context.Context,
	mode RaftMsgAppMode,
	info ReplicaStateInfo,
	nextRaftIndex uint64,
	recreateSendStream bool,
) (shouldWaitChange bool) {
	rs.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	switch info.State {
	case tracker.StateProbe:
		if rs.sendStream == nil {
			// We have already closed the stream, nothing to do.
			return false
		}
		if shouldClose := func() (shouldClose bool) {
			now := rs.parent.opts.Clock.PhysicalTime()
			rs.sendStream.mu.Lock()
			defer rs.sendStream.mu.Unlock()

			if state := rs.sendStream.mu.connectedState; state == probeRecentlyNoSendQ &&
				now.Sub(rs.sendStream.mu.connectedStateStart) >= probeRecentlyNoSendQDuration() {
				// The replica has been in StateProbe for at least
				// probeRecentlyNoSendQDuration, so close the stream.
				shouldClose = true
			} else if state != probeRecentlyNoSendQ {
				if rs.sendStream.isEmptySendQueueStreamLocked() {
					// Empty send-queue. We will transition to probeRecentlyNoSendQ,
					// which trades off not doing a force-flush with allowing for higher
					// latency to achieve quorum.
					rs.sendStream.changeToProbeRaftMuAndStreamLocked(ctx, now)
				} else {
					// Had a send-queue.
					shouldClose = true
				}
				// Since not in StateReplicate, cannot be considered part of the
				// quorum, so waiting may need to change.
				shouldWaitChange = true
			}
			return shouldClose
		}(); shouldClose {
			rs.closeSendStreamRaftMuLocked(ctx)
		}

	case tracker.StateReplicate:
		if rs.sendStream == nil {
			if !recreateSendStream {
				panic(errors.AssertionFailedf("in StateReplica, but recreateSendStream is false"))
			}
		}
		if rs.sendStream != nil && recreateSendStream {
			// This includes both (a) inconsistencies, and (b) transition from
			// probeRecentlyNoSendQ => replicate.
			rs.closeSendStreamRaftMuLocked(ctx)
		}
		if rs.sendStream == nil {
			rs.createReplicaSendStreamRaftMuLocked(ctx, mode, info.Next, nextRaftIndex)
			// Have stale send-queue state.
			shouldWaitChange = true
		}

	case tracker.StateSnapshot:
		if rs.sendStream != nil {
			rs.closeSendStreamRaftMuLocked(ctx)
			shouldWaitChange = true
		}
	}
	return shouldWaitChange
}

// scheduled is only called when rs.sendStream != nil, and on followers.
//
// closedReplica => !scheduleAgain.
func (rs *replicaState) scheduledRaftMuLocked(
	ctx context.Context, mode RaftMsgAppMode, logSnapshot raft.LogSnapshot,
) (scheduleAgain bool, updateWaiterSets bool) {
	rs.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	if rs.replicaID == rs.parent.opts.LocalReplicaID {
		panic("scheduled called on the leader replica")
	}
	rss := rs.sendStream
	rss.mu.Lock()
	defer rss.mu.Unlock()
	if !rss.mu.sendQueue.forceFlushStopIndex.active() && rss.mu.sendQueue.deductedForSchedulerTokens == 0 {
		// NB: it is possible mode != rss.mu.mode, and we will ignore the change
		// here. This is fine in that we will pick up the change in the next
		// RaftEvent.
		return false, false
	}
	if rss.isEmptySendQueueStreamLocked() {
		panic(errors.AssertionFailedf("scheduled with empty send-queue"))
	}
	if rss.mu.mode != MsgAppPull {
		panic(errors.AssertionFailedf("force-flushing or deducted tokens in push mode"))
	}
	if mode != rss.mu.mode {
		// Must be switching from MsgAppPull => MsgAppPush.
		rss.tryHandleModeChangeRaftMuAndStreamLocked(ctx, mode, false, false)
		return false, false
	}
	forceFlushActiveAndPaused := func() bool {
		return rss.mu.sendQueue.forceFlushStopIndex.active() &&
			rss.reachedInflightBytesThresholdRaftMuAndStreamLocked()
	}
	if forceFlushActiveAndPaused() {
		return false, false
	}
	// 4MB. Don't want to hog the scheduler thread for too long.
	const MaxBytesToSend kvflowcontrol.Tokens = 4 << 20
	bytesToSend := MaxBytesToSend
	if !rss.mu.sendQueue.forceFlushStopIndex.active() &&
		rss.mu.sendQueue.deductedForSchedulerTokens < bytesToSend {
		bytesToSend = rss.mu.sendQueue.deductedForSchedulerTokens
	}
	// TODO(sumeer): if (for some reason) many entries are not subject to
	// replication AC in pull mode, and a send-queue forms, and these entries
	// are > 4KiB, we will empty the send-queue one entry at a time.
	// Specifically, we will only deduct 4KiB of tokens, since the approx size
	// of the send-queue will be zero. Then we will call LogSlice with
	// maxSize=4KiB, which will return one entry. And then we will repeat. If
	// this is a real problem, we can fix this by keeping track of not just the
	// preciseSizeSum of tokens needed, but also the size sum of these entries.
	// Then scale up the value of maxSize=deducted*(sizeSum/preciseSizeSum). In
	// this example preciseSizeSum would be 0, so we would instead scale it up
	// to MaxBytesToSend.
	//
	// NB: the rss.mu.sendQueue.deductedForScheduler.tokens represent what is
	// subject to RAC. But Raft is unaware of this linkage between admission
	// control and flow tokens, and MakeMsgApp will use this bytesToSend to
	// compute across all entries. This is not harmful for the following
	// reasons. RACv2 will be configured in one of three modes (a) fully
	// disabled, so this is irrelevant, (b) flow tokens only for elastic work,
	// and MsgAppPush mode, so this is irrelevant, (c) flow tokens for regular
	// and elastic work, and MsgAppPull mode, in which case the total size of
	// entries not subject to flow control will be tiny. We of course return the
	// unused tokens for entries not subject to flow control.
	slice, err := logSnapshot.LogSlice(
		rss.mu.sendQueue.indexToSend-1, rss.mu.sendQueue.nextRaftIndex-1, uint64(bytesToSend))
	var msg raftpb.Message
	if err == nil {
		var sent bool
		msg, sent = rss.parent.parent.opts.RaftInterface.SendMsgAppRaftMuLocked(
			rss.parent.replicaID, slice)
		if !sent {
			err = errors.Errorf("SendMsgApp could not send for replica %d", rss.parent.replicaID)
		}
	}
	if err != nil {
		// Transitioned to StateSnapshot, or some other error that Raft needs to
		// deal with.
		rs.sendStream.closeRaftMuAndStreamLocked(ctx)
		rs.sendStream = nil
		return false, true
	}
	rss.updateInflightRaftMuAndStreamLocked(slice)
	rss.dequeueFromQueueAndSendRaftMuAndStreamLocked(ctx, msg)
	isEmpty := rss.isEmptySendQueueStreamLocked()
	if isEmpty {
		rss.stopAttemptingToEmptySendQueueRaftMuAndStreamLocked(ctx, false)
		return false, true
	}
	// Still have a send-queue.
	if rss.mu.sendQueue.forceFlushStopIndex.active() &&
		uint64(rss.mu.sendQueue.forceFlushStopIndex) < rss.mu.sendQueue.indexToSend {
		// It is possible that we don't have a quorum with no send-queue and we
		// needed to rely on this force-flush until the send-queue was empty. That
		// knowledge will become known in the next
		// rangeController.HandleRaftEventRaftMuLocked, which will happen at the
		// next tick. We accept a latency hiccup in this case for now.
		rss.mu.sendQueue.forceFlushStopIndex = 0
	}
	forceFlushNeedsToPause := forceFlushActiveAndPaused()
	watchForTokens :=
		!rss.mu.sendQueue.forceFlushStopIndex.active() && rss.mu.sendQueue.deductedForSchedulerTokens == 0
	if watchForTokens {
		rss.startAttemptingToEmptySendQueueViaWatcherStreamLocked(ctx)
	}
	scheduleAgain = !watchForTokens && !forceFlushNeedsToPause
	return scheduleAgain, false
}

func (rs *replicaState) closeSendStreamRaftMuLocked(ctx context.Context) {
	rs.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	if log.ExpensiveLogEnabled(ctx, 1) {
		log.VEventf(ctx, 1, "r%v closing send stream %v for replica %v",
			rs.parent.opts.RangeID, rs.stream, rs.desc)
	}
	rs.sendStream.mu.Lock()
	defer rs.sendStream.mu.Unlock()

	rs.sendStream.closeRaftMuAndStreamLocked(ctx)
	rs.sendStream = nil
}

func (rs *replicaState) admitRaftMuLocked(ctx context.Context, av AdmittedVector) {
	rs.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	if rss := rs.sendStream; rss != nil {
		rss.admitRaftMuLocked(ctx, av)
	}
}

func (rss *replicaSendStream) closeRaftMuAndStreamLocked(ctx context.Context) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	rss.returnSendTokensRaftMuAndStreamLocked(ctx, rss.raftMu.tracker.UntrackAll(), true /* disconnect */)
	rss.returnAllEvalTokensRaftMuAndStreamLocked(ctx)
	rss.stopAttemptingToEmptySendQueueRaftMuAndStreamLocked(ctx, true)
	rss.mu.closed = true
}

func (rss *replicaSendStream) applySendQueuePreciseSizeDeltaRaftMuAndStreamLocked(
	ctx context.Context, delta kvflowcontrol.Tokens,
) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()

	before := rss.mu.sendQueue.preciseSizeSum
	after := before + delta
	rss.mu.sendQueue.preciseSizeSum = after
	if rss.isEmptySendQueueStreamLocked() && after != 0 {
		panic(errors.AssertionFailedf(
			"empty send-queue with non-zero precise size: "+
				"before=%v after=%v delta=%v [queue_len=%v, queue_approx_size=%v]",
			before, after, delta,
			rss.queueLengthRaftMuAndStreamLocked(),
			rss.approxQueueSizeStreamLocked(),
		))
	}
}

func (rss *replicaSendStream) handleReadyEntriesRaftMuAndStreamLocked(
	ctx context.Context, event raftEventForReplica, directive replicaDirective,
) (transitionedSendQState bool, err error) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	wasEmptySendQ := rss.isEmptySendQueueStreamLocked()
	rss.tryHandleModeChangeRaftMuAndStreamLocked(
		ctx, event.mode, wasEmptySendQ, directive.forceFlushStopIndex.active())
	// Use the latest inflight bytes, since it reflects the advancing Match.
	wasExceedingInflightBytesThreshold := rss.reachedInflightBytesThresholdRaftMuAndStreamLocked()
	rss.mu.inflightBytes = event.replicaStateInfo.InflightBytes
	if event.mode == MsgAppPull {
		// MsgAppPull mode (i.e., followers). Populate sendingEntries.
		n := len(event.sendingEntries)
		if n != 0 {
			panic(errors.AssertionFailedf("pull mode must not have sending entries (leader=%t)",
				rss.parent.replicaID == rss.parent.parent.opts.LocalReplicaID))
		}
		if directive.forceFlushStopIndex.active() {
			// Must have a send-queue, so sendingEntries should stay empty (these
			// will be queued).
			if !rss.mu.sendQueue.forceFlushStopIndex.active() {
				rss.startForceFlushRaftMuAndStreamLocked(ctx, directive.forceFlushStopIndex)
			} else {
				if rss.mu.sendQueue.forceFlushStopIndex != directive.forceFlushStopIndex {
					rss.mu.sendQueue.forceFlushStopIndex = directive.forceFlushStopIndex
				}
				if wasExceedingInflightBytesThreshold &&
					!rss.reachedInflightBytesThresholdRaftMuAndStreamLocked() {
					rss.parent.parent.scheduleReplica(rss.parent.replicaID)
				}
			}
		} else {
			// INVARIANT: !directive.forceFlushStopIndex.active()
			if rss.mu.sendQueue.forceFlushStopIndex.active() {
				// Must have a send-queue, so sendingEntries should stay empty (these
				// will be queued).
				rss.mu.sendQueue.forceFlushStopIndex = 0
				rss.parent.parent.opts.RangeControllerMetrics.SendQueue.ForceFlushedScheduledCount.Dec(1)
				rss.startAttemptingToEmptySendQueueViaWatcherStreamLocked(ctx)
				if directive.hasSendTokens {
					panic(errors.AssertionFailedf("hasSendTokens true despite send-queue"))
				}
			} else if directive.hasSendTokens {
				// Send everything that is being added.
				event.sendingEntries = event.newEntries
			}
		}
	}
	// Common behavior for updating state using sendingEntries and newEntries
	// for MsgAppPush and MsgAppPull.
	if n := len(event.sendingEntries); n > 0 {
		if event.sendingEntries[0].id.index != rss.mu.sendQueue.indexToSend {
			panic(errors.AssertionFailedf("first send entry %d does not match indexToSend %d",
				event.sendingEntries[0].id.index, rss.mu.sendQueue.indexToSend))
		}
		rss.mu.sendQueue.indexToSend = event.sendingEntries[n-1].id.index + 1
		var sendTokensToDeduct [admissionpb.NumWorkClasses]kvflowcontrol.Tokens
		var sendQPreciseSizeDelta kvflowcontrol.Tokens
		for _, entry := range event.sendingEntries {
			if !entry.usesFlowControl {
				continue
			}
			var pri raftpb.Priority
			inSendQueue := false
			if entry.id.index >= rss.mu.sendQueue.nextRaftIndex {
				// Was never in the send-queue.
				pri = entry.pri
			} else {
				// Was in the send-queue.
				inSendQueue = true
				if event.mode == MsgAppPush {
					pri = entry.pri
				} else {
					pri = raftpb.LowPri
				}
			}
			tokens := entry.tokens
			if rss.parent.parent.opts.Knobs != nil {
				if fn := rss.parent.parent.opts.Knobs.OverrideTokenDeduction; fn != nil {
					tokens = fn(tokens)
				}
			}
			if inSendQueue && entry.id.index >= rss.mu.nextRaftIndexInitial {
				// Was in send-queue and had eval tokens deducted for it.
				rss.mu.sendQueue.originalEvalTokens[WorkClassFromRaftPriority(entry.pri)] -= tokens
				sendQPreciseSizeDelta -= tokens
			}
			rss.raftMu.tracker.Track(ctx, entry.id, pri, tokens)
			sendTokensToDeduct[WorkClassFromRaftPriority(pri)] += tokens
		}
		if sendQPreciseSizeDelta != 0 {
			rss.applySendQueuePreciseSizeDeltaRaftMuAndStreamLocked(ctx, sendQPreciseSizeDelta)
		}
		flag := AdjNormal
		if directive.preventSendQNoForceFlush {
			flag = AdjPreventSendQueue
		}
		for wc, tokens := range sendTokensToDeduct {
			if tokens != 0 {
				rss.parent.sendTokenCounter.Deduct(ctx, admissionpb.WorkClass(wc), tokens, flag)
			}
		}
		if directive.preventSendQNoForceFlush {
			rss.parent.parent.opts.RangeControllerMetrics.SendQueue.PreventionCount.Inc(1)
		}
	}
	if n := len(event.newEntries); n > 0 {
		if event.newEntries[0].id.index != rss.mu.sendQueue.nextRaftIndex {
			panic(errors.AssertionFailedf("append %d does not match nextRaftIndex %d",
				event.newEntries[0].id.index, rss.mu.sendQueue.nextRaftIndex))
		}
		rss.mu.sendQueue.nextRaftIndex = event.newEntries[n-1].id.index + 1
		var evalTokensToDeduct [admissionpb.NumWorkClasses]kvflowcontrol.Tokens
		for _, entry := range event.newEntries {
			if !entry.usesFlowControl {
				continue
			}
			var pri raftpb.Priority
			inSendQueue := false
			tokens := entry.tokens
			if rss.parent.parent.opts.Knobs != nil {
				if fn := rss.parent.parent.opts.Knobs.OverrideTokenDeduction; fn != nil {
					tokens = fn(tokens)
				}
			}
			if entry.id.index >= rss.mu.sendQueue.indexToSend {
				// Being added to the send-queue.
				inSendQueue = true
				if event.mode == MsgAppPush {
					// NB: we may deduct regular eval tokens, but raft's own flow
					// control may delay sending this, and cause harm to other ranges.
					// That is ok, since in push mode we only subject elastic work to
					// replication flow control (in WaitForEval). That does not mean we
					// will not have regular entries in the send-queue since these could
					// have been evaluated while in pull mode.
					pri = entry.pri
				} else {
					pri = raftpb.LowPri
				}
				rss.applySendQueuePreciseSizeDeltaRaftMuAndStreamLocked(ctx, +tokens)
			} else {
				pri = entry.pri
			}
			if inSendQueue && entry.id.index >= rss.mu.nextRaftIndexInitial {
				// Is in send-queue and will have eval tokens deducted for it.
				rss.mu.sendQueue.originalEvalTokens[WorkClassFromRaftPriority(entry.pri)] += tokens
			}
			wc := WorkClassFromRaftPriority(pri)
			evalTokensToDeduct[wc] += tokens
			rss.mu.eval.tokensDeducted[wc] += tokens
		}
		for wc, tokens := range evalTokensToDeduct {
			if tokens != 0 {
				rss.parent.evalTokenCounter.Deduct(ctx, admissionpb.WorkClass(wc), tokens, AdjNormal)
			}
		}
	}

	if n := len(event.sendingEntries); n > 0 && event.mode == MsgAppPull {
		// NB: this will not do IO since everything here is in the unstable log
		// (see raft.LogSnapshot.unstable).
		slice, err := event.logSnapshot.LogSlice(
			event.sendingEntries[0].id.index-1, event.sendingEntries[n-1].id.index, infinityEntryIndex)
		if err != nil {
			return false, err
		}
		msg, sent := rss.parent.parent.opts.RaftInterface.SendMsgAppRaftMuLocked(
			rss.parent.replicaID, slice)
		if !sent {
			return false,
				errors.Errorf("SendMsgApp could not send for replica %d", rss.parent.replicaID)
		}
		rss.updateInflightRaftMuAndStreamLocked(slice)
		rss.parent.parent.opts.MsgAppSender.SendMsgApp(ctx, msg, false)
	}

	hasEmptySendQ := rss.isEmptySendQueueStreamLocked()
	if event.mode == MsgAppPull && wasEmptySendQ && !hasEmptySendQ &&
		!rss.mu.sendQueue.forceFlushStopIndex.active() {
		rss.startAttemptingToEmptySendQueueViaWatcherStreamLocked(ctx)
	}
	// NB: we don't special case to an empty send-queue in push mode, where Raft
	// is responsible for causing this send-queue. Raft does not keep track of
	// whether the send-queues are causing a loss of quorum, so in the worst
	// case we could stop evaluating because of a majority of voters having a
	// send-queue. But in push mode only elastic work will be subject to
	// replication admission control, and regular work will not call
	// WaitForEval, so we accept this behavior.
	transitionedSendQState = wasEmptySendQ != hasEmptySendQ
	return transitionedSendQState, nil
}

func (rss *replicaSendStream) updateInflightRaftMuAndStreamLocked(ls raft.LogSlice) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	entries := ls.Entries()
	for i := range ls.Entries() {
		// NB: raft.payloadSize also uses len(raftpb.Entry.Data).
		rss.mu.inflightBytes += uint64(len(entries[i].Data))
	}
}

func (rss *replicaSendStream) tryHandleModeChangeRaftMuAndStreamLocked(
	ctx context.Context, mode RaftMsgAppMode, isEmptySendQ bool, toldToForceFlush bool,
) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	if mode == rss.mu.mode {
		// Common case
		return
	}
	rss.mu.mode = mode
	if mode == MsgAppPush {
		// Switching from pull to push. Everything was counted as elastic, but now
		// we want regular to count as regular. So return tokens to elastic and
		// deduct from regular.
		// TODO(kvoli): Should we have a metric for this? It should be rare.
		rss.parent.evalTokenCounter.Deduct(ctx, admissionpb.ElasticWorkClass,
			-rss.mu.sendQueue.originalEvalTokens[admissionpb.RegularWorkClass], AdjNormal)
		rss.mu.eval.tokensDeducted[admissionpb.ElasticWorkClass] -=
			rss.mu.sendQueue.originalEvalTokens[admissionpb.RegularWorkClass]
		rss.parent.evalTokenCounter.Deduct(ctx, admissionpb.RegularWorkClass,
			rss.mu.sendQueue.originalEvalTokens[admissionpb.RegularWorkClass], AdjNormal)
		rss.mu.eval.tokensDeducted[admissionpb.RegularWorkClass] +=
			rss.mu.sendQueue.originalEvalTokens[admissionpb.RegularWorkClass]
		rss.stopAttemptingToEmptySendQueueRaftMuAndStreamLocked(ctx, false)
	} else {
		// Switching from push to pull. Regular needs to be counted as elastic, so
		// return to regular and deduct from elastic.
		rss.parent.evalTokenCounter.Deduct(ctx, admissionpb.ElasticWorkClass,
			rss.mu.sendQueue.originalEvalTokens[admissionpb.RegularWorkClass], AdjNormal)
		rss.mu.eval.tokensDeducted[admissionpb.ElasticWorkClass] +=
			rss.mu.sendQueue.originalEvalTokens[admissionpb.RegularWorkClass]
		rss.parent.evalTokenCounter.Deduct(ctx, admissionpb.RegularWorkClass,
			-rss.mu.sendQueue.originalEvalTokens[admissionpb.RegularWorkClass], AdjNormal)
		rss.mu.eval.tokensDeducted[admissionpb.RegularWorkClass] -=
			rss.mu.sendQueue.originalEvalTokens[admissionpb.RegularWorkClass]
		if !isEmptySendQ && !toldToForceFlush {
			rss.startAttemptingToEmptySendQueueViaWatcherStreamLocked(ctx)
		}
	}
}

func (rss *replicaSendStream) reachedInflightBytesThresholdRaftMuAndStreamLocked() bool {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	return rss.mu.inflightBytes >= rss.parent.parent.opts.RaftMaxInflightBytes
}

func (rss *replicaSendStream) startForceFlushRaftMuAndStreamLocked(
	ctx context.Context, forceFlushStopIndex forceFlushStopIndex,
) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	rss.parent.parent.opts.RangeControllerMetrics.SendQueue.ForceFlushedScheduledCount.Inc(1)
	rss.mu.sendQueue.forceFlushStopIndex = forceFlushStopIndex
	if !rss.reachedInflightBytesThresholdRaftMuAndStreamLocked() {
		rss.parent.parent.scheduleReplica(rss.parent.replicaID)
	}
	rss.stopAttemptingToEmptySendQueueViaWatcherRaftMuAndStreamLocked(ctx, false)
}

// Only called in MsgAppPull mode. Either when force-flushing or when
// rss.mu.sendQueue.deductedFromSchedulerTokens > 0.
func (rss *replicaSendStream) dequeueFromQueueAndSendRaftMuAndStreamLocked(
	ctx context.Context, msg raftpb.Message,
) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	var tokensNeeded kvflowcontrol.Tokens
	var approximatedNumEntries int
	var approximatedNumActualTokens kvflowcontrol.Tokens
	for _, entry := range msg.Entries {
		entryState := getEntryFCStateOrFatal(ctx, entry)
		if entryState.id.index != rss.mu.sendQueue.indexToSend {
			panic(errors.AssertionFailedf("index %d != indexToSend %d",
				entryState.id.index, rss.mu.sendQueue.indexToSend))
		}
		if entryState.id.index >= rss.mu.sendQueue.nextRaftIndex {
			panic(errors.AssertionFailedf("index %d >= nextRaftIndex %d", entryState.id.index,
				rss.mu.sendQueue.nextRaftIndex))
		}
		tokens := entryState.tokens
		if rss.parent.parent.opts.Knobs != nil {
			if fn := rss.parent.parent.opts.Knobs.OverrideTokenDeduction; fn != nil {
				tokens = fn(tokens)
			}
		}
		rss.mu.sendQueue.indexToSend++
		isApproximatedEntry := entryState.id.index < rss.mu.nextRaftIndexInitial
		if isApproximatedEntry {
			approximatedNumEntries++
			if entryState.usesFlowControl {
				approximatedNumActualTokens += tokens
			}
		}
		if entryState.usesFlowControl {
			if !isApproximatedEntry {
				rss.applySendQueuePreciseSizeDeltaRaftMuAndStreamLocked(ctx, -tokens)
				rss.mu.sendQueue.originalEvalTokens[WorkClassFromRaftPriority(entryState.pri)] -=
					tokens
			}
			tokensNeeded += tokens
			rss.raftMu.tracker.Track(ctx, entryState.id, raftpb.LowPri, tokens)
		}
	}
	if approximatedNumEntries > 0 {
		rss.mu.sendQueue.entryTokensApproximator.addStats(
			approximatedNumEntries, approximatedNumActualTokens)
	}
	if !rss.mu.sendQueue.forceFlushStopIndex.active() {
		// Subtract from already deducted tokens.
		beforeDeductedTokens := rss.mu.sendQueue.deductedForSchedulerTokens
		rss.mu.sendQueue.deductedForSchedulerTokens -= tokensNeeded
		if rss.mu.sendQueue.deductedForSchedulerTokens < 0 {
			// Used more than what we had already deducted. Will need to subtract
			// these now.
			tokensNeeded = -rss.mu.sendQueue.deductedForSchedulerTokens
			rss.mu.sendQueue.deductedForSchedulerTokens = 0
		} else {
			tokensNeeded = 0
		}
		afterDeductedTokens := rss.mu.sendQueue.deductedForSchedulerTokens
		if buildutil.CrdbTestBuild && beforeDeductedTokens < afterDeductedTokens {
			panic(errors.AssertionFailedf("beforeDeductedTokens %s < afterDeductedTokens %s",
				beforeDeductedTokens, afterDeductedTokens))
		}
		if beforeDeductedTokens > afterDeductedTokens {
			sendQueueMetrics := rss.parent.parent.opts.RangeControllerMetrics.SendQueue
			sendQueueMetrics.DeductedForSchedulerBytes.Dec(int64(beforeDeductedTokens - afterDeductedTokens))
		}
	}
	if tokensNeeded > 0 {
		flag := AdjNormal
		if rss.mu.sendQueue.forceFlushStopIndex.active() {
			flag = AdjForceFlush
		}
		rss.parent.sendTokenCounter.Deduct(ctx, admissionpb.ElasticWorkClass, tokensNeeded, flag)
	}
	rss.parent.parent.opts.MsgAppSender.SendMsgApp(ctx, msg, true)
}

// NB: raftMu may or may not be held. Specifically, when called from Notify,
// raftMu is not held.
func (rss *replicaSendStream) isEmptySendQueueStreamLocked() bool {
	rss.mu.AssertHeld()
	return rss.mu.sendQueue.indexToSend == rss.mu.sendQueue.nextRaftIndex
}

// INVARIANT: no send-queue, and therefore not force-flushing.
func (rss *replicaSendStream) changeToProbeRaftMuAndStreamLocked(
	ctx context.Context, now time.Time,
) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	if log.ExpensiveLogEnabled(ctx, 1) {
		log.VEventf(ctx, 1, "r%v:%v stream %v changing to probe",
			rss.parent.parent.opts.RangeID, rss.parent.desc, rss.parent.stream)
	}
	// This is the first time we've seen the replica change to StateProbe,
	// update the connected state and start time. If the state doesn't
	// change within probeRecentlyNoSendQDuration, we will close the
	// stream. Also schedule an event, so that even if there are no
	// entries, we will still reliably close the stream if still in
	// StateProbe.
	rss.changeConnectedStateRaftMuAndStreamLocked(probeRecentlyNoSendQ, now)
	rss.parent.parent.opts.CloseTimerScheduler.ScheduleSendStreamCloseRaftMuLocked(
		ctx, rss.parent.parent.opts.RangeID, probeRecentlyNoSendQDuration())
	// Return all tokens since other ranges may need them, and it may be some
	// time before this replica transitions back to StateReplicate.
	rss.returnSendTokensRaftMuAndStreamLocked(
		ctx, rss.raftMu.tracker.UntrackAll(), true /* disconnect */)
	rss.returnAllEvalTokensRaftMuAndStreamLocked(ctx)
	rss.mu.sendQueue.originalEvalTokens = [admissionpb.NumWorkClasses]kvflowcontrol.Tokens{}
	if !rss.isEmptySendQueueStreamLocked() {
		panic(errors.AssertionFailedf("transitioning to probeRecentlyNoSendQ when have a send-queue"))
	}
	if rss.mu.sendQueue.forceFlushStopIndex.active() {
		panic(errors.AssertionFailedf("no send-queue but force-flushing"))
	}
	if rss.mu.sendQueue.deductedForSchedulerTokens != 0 ||
		rss.mu.sendQueue.tokenWatcherHandle != (SendTokenWatcherHandle{}) {
		panic(errors.AssertionFailedf("no send-queue but trying to empty send-queue via watcher"))
	}
}

func (rss *replicaSendStream) stopAttemptingToEmptySendQueueRaftMuAndStreamLocked(
	ctx context.Context, disconnect bool,
) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	if rss.mu.sendQueue.forceFlushStopIndex.active() {
		rss.mu.sendQueue.forceFlushStopIndex = 0
		rss.parent.parent.opts.RangeControllerMetrics.SendQueue.ForceFlushedScheduledCount.Dec(1)
	}
	rss.stopAttemptingToEmptySendQueueViaWatcherRaftMuAndStreamLocked(ctx, disconnect)
}

func (rss *replicaSendStream) stopAttemptingToEmptySendQueueViaWatcherRaftMuAndStreamLocked(
	ctx context.Context, disconnect bool,
) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	if rss.mu.sendQueue.deductedForSchedulerTokens != 0 {
		// Update metrics.
		flag := AdjNormal
		if disconnect {
			flag = AdjDisconnect
		}
		rss.parent.parent.opts.RangeControllerMetrics.
			SendQueue.DeductedForSchedulerBytes.Dec(
			int64(rss.mu.sendQueue.deductedForSchedulerTokens))

		rss.parent.sendTokenCounter.Return(
			ctx, admissionpb.ElasticWorkClass, rss.mu.sendQueue.deductedForSchedulerTokens, flag)
		rss.mu.sendQueue.deductedForSchedulerTokens = 0
	}
	if handle := rss.mu.sendQueue.tokenWatcherHandle; handle != (SendTokenWatcherHandle{}) {
		rss.parent.parent.opts.SendTokenWatcher.CancelHandle(ctx, handle)
		rss.mu.sendQueue.tokenWatcherHandle = SendTokenWatcherHandle{}
	}
}

// Requires that send-queue is non-empty. Note that it is possible that all
// the entries in the send-queue are not subject to replication admission
// control, and we will still wait for non-zero tokens. This is considered
// acceptable for two reasons (a) when nextRaftIndexInitial > indexToSend, the
// replicaSendStream does not know whether entries in [indexToSend,
// nextRaftIndexInitial) are subject to replication AC, (b) even when
// replicaSendStream has precise knowledge of every entry in the send-queue,
// it is arguably reasonable to wait for send tokens > 0, since these entries
// will impose some load on the receiver. Case (b) is going to be rare anyway,
// since very few entries are not subject to replication AC in pull mode
// (since it is active only when replication AC is "apply_to_all"), and
// usually the send-queue won't even form if the entries need zero tokens.
//
// NB: raftMu may or may not be held. Specifically, when called from Notify,
// raftMu is not held.
func (rss *replicaSendStream) startAttemptingToEmptySendQueueViaWatcherStreamLocked(
	ctx context.Context,
) {
	rss.mu.AssertHeld()
	if rss.mu.sendQueue.forceFlushStopIndex.active() {
		panic(errors.AssertionFailedf("already trying to empty send-queue using force-flush"))
	}
	if rss.mu.sendQueue.deductedForSchedulerTokens != 0 ||
		rss.mu.sendQueue.tokenWatcherHandle != (SendTokenWatcherHandle{}) {
		panic(errors.AssertionFailedf("already trying to empty send-queue via watcher"))
	}
	rss.mu.sendQueue.tokenWatcherHandle =
		rss.parent.parent.opts.SendTokenWatcher.NotifyWhenAvailable(ctx, rss.parent.sendTokenCounter, rss)
}

// Notify implements TokenGrantNotification.
func (rss *replicaSendStream) Notify(ctx context.Context) {
	rss.mu.Lock()
	defer rss.mu.Unlock()
	if rss.mu.closed {
		return
	}
	if rss.mu.sendQueue.tokenWatcherHandle == (SendTokenWatcherHandle{}) {
		return
	}
	rss.mu.sendQueue.tokenWatcherHandle = SendTokenWatcherHandle{}
	if rss.mu.sendQueue.deductedForSchedulerTokens != 0 {
		panic(errors.AssertionFailedf("watcher was registered when already had tokens"))
	}
	if rss.isEmptySendQueueStreamLocked() {
		panic(errors.AssertionFailedf("watcher was registered with empty send-queue"))
	}
	// Deduct a bit more, so we can also dequeue things that get enqueued later,
	// and transition to an empty send-queue.
	//
	// TODO(sumeer): refine this heuristic.
	queueSize := rss.approxQueueSizeStreamLocked()
	queueSize = kvflowcontrol.Tokens(float64(queueSize) * 1.1)
	if queueSize < 2048 {
		// NB: queueSize could be 0 if none of the entries were subject to
		// replication AC. Even in that case we grab some tokens.
		queueSize = 4096
	}
	flag := AdjNormal
	if rss.mu.sendQueue.forceFlushStopIndex.active() {
		panic(errors.AssertionFailedf("cannot be force-flushing"))
	}
	tokens := rss.parent.sendTokenCounter.TryDeduct(ctx, admissionpb.ElasticWorkClass, queueSize, flag)
	if tokens == 0 {
		// Rare case: no tokens available despite notification. Register again.
		rss.startAttemptingToEmptySendQueueViaWatcherStreamLocked(ctx)
		return
	}
	rss.mu.sendQueue.deductedForSchedulerTokens = tokens
	rss.parent.parent.opts.RangeControllerMetrics.SendQueue.DeductedForSchedulerBytes.Inc(int64(tokens))
	rss.parent.parent.scheduleReplica(rss.parent.replicaID)
}

// NB: raftMu may or may not be held. Specifically, when called from Notify,
// raftMu is not held.
//
// NB: This can return 0 despite a non-empty send-queue if none of the entries
// are subject to replication admission control.
func (rss *replicaSendStream) approxQueueSizeStreamLocked() kvflowcontrol.Tokens {
	rss.mu.AssertHeld()
	var size kvflowcontrol.Tokens
	countWithApproxStats := int64(rss.mu.nextRaftIndexInitial) - int64(rss.mu.sendQueue.indexToSend)
	if countWithApproxStats > 0 {
		size = kvflowcontrol.Tokens(countWithApproxStats) *
			rss.mu.sendQueue.entryTokensApproximator.meanTokensPerEntry()
	}
	size += rss.mu.sendQueue.preciseSizeSum
	return size
}

func (rss *replicaSendStream) queueLengthRaftMuAndStreamLocked() int64 {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	// NB: INVARIANT nextRaftIndex >= indexToSend, no underflow possible.
	return int64(rss.mu.sendQueue.nextRaftIndex - rss.mu.sendQueue.indexToSend)
}

// returnSendTokensRaftMuAndStreamLocked takes the tokens untracked by the
// tracker and returns them to the send token counters.
func (rss *replicaSendStream) returnSendTokensRaftMuAndStreamLocked(
	ctx context.Context, returned [raftpb.NumPriorities]kvflowcontrol.Tokens, disconnect bool,
) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	flag := AdjNormal
	if disconnect {
		flag = AdjDisconnect
	}
	for pri, tokens := range returned {
		if tokens > 0 {
			pri := WorkClassFromRaftPriority(raftpb.Priority(pri))
			rss.parent.sendTokenCounter.Return(ctx, pri, tokens, flag)
		}
	}
}

// returnEvalTokensRaftMuAndStreamLocked returns tokens to the eval token
// counters.
func (rss *replicaSendStream) returnEvalTokensRaftMuAndStreamLocked(
	ctx context.Context, returnedEval [raftpb.NumPriorities]kvflowcontrol.Tokens,
) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	for pri, tokens := range returnedEval {
		rpri := raftpb.Priority(pri)
		wc := WorkClassFromRaftPriority(rpri)
		if tokens > 0 {
			rss.parent.evalTokenCounter.Return(ctx, wc, tokens, AdjNormal)
			rss.mu.eval.tokensDeducted[wc] -= tokens
			if rss.mu.eval.tokensDeducted[wc] < 0 {
				if buildutil.CrdbTestBuild {
					panic(errors.AssertionFailedf(
						"negative eval tokens %d for r%s/%s", rss.mu.eval.tokensDeducted[wc],
						rss.parent.parent.opts.RangeID.String(), rss.parent.desc.ReplicaID.String()))
				} else {
					rss.mu.eval.tokensDeducted[wc] = 0
				}
			}
		}
	}
}

func (rss *replicaSendStream) returnAllEvalTokensRaftMuAndStreamLocked(ctx context.Context) {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.AssertHeld()
	for wc, tokens := range rss.mu.eval.tokensDeducted {
		if tokens > 0 {
			// NB: This is only called for disconnects.
			rss.parent.evalTokenCounter.Return(ctx, admissionpb.WorkClass(wc), tokens, AdjDisconnect)
		}
		rss.mu.eval.tokensDeducted[wc] = 0
	}
}

func (rss *replicaSendStream) checkConsistencyRaftMuLocked() {
	rss.parent.parent.opts.ReplicaMutexAsserter.RaftMuAssertHeld()
	rss.mu.Lock()
	defer rss.mu.Unlock()
	if rss.mu.connectedState == probeRecentlyNoSendQ {
		if !rss.raftMu.tracker.Empty() {
			panic(errors.AssertionFailedf("tracker is not empty in state probe"))
		}
		for _, tokens := range rss.mu.eval.tokensDeducted {
			if tokens != 0 {
				panic(errors.AssertionFailedf("non-zero eval tokens deducted in state probe"))
			}
		}
		if rss.mu.sendQueue.deductedForSchedulerTokens != 0 {
			panic(errors.AssertionFailedf("non-zero deductedForSchedulerTokens in state probe"))
		}
		return
	}
	// replicate state.

	// tokens is the expected number of eval tokens that have been deducted.
	var tokens [admissionpb.NumWorkClasses]kvflowcontrol.Tokens
	// trackerTokens is all the send tokens in the tracker that have eval tokens
	// deducted. NB: indices < rss.mu.nextRaftIndexInitial were in the
	// send-queue when the replicaSendStream was created, so did not have eval
	// tokens deducted.
	trackerTokens := rss.raftMu.tracker.tokensGE(rss.mu.nextRaftIndexInitial)
	for pri, t := range trackerTokens {
		tokens[WorkClassFromRaftPriority(raftpb.Priority(pri))] += t
	}
	// There are tokens in the send-queue that also have eval tokens deducted.
	// Add them to the expected number.
	for wc, t := range rss.mu.sendQueue.originalEvalTokens {
		effectiveWC := admissionpb.WorkClass(wc)
		if rss.mu.mode == MsgAppPull {
			// NB: regular work deducts elastic (eval and send) tokens in pull mode.
			effectiveWC = admissionpb.ElasticWorkClass
		}
		tokens[effectiveWC] += t
	}
	// Check that the expected number is equal to rss.mu.eval.tokensDeducted.
	for wc, t := range rss.mu.eval.tokensDeducted {
		if t != tokens[wc] {
			panic(errors.AssertionFailedf("%v: eval tokens deducted %v != %v",
				admissionpb.WorkClass(wc), t, tokens[wc]))
		}
	}
	if rss.isEmptySendQueueStreamLocked() && rss.mu.sendQueue.deductedForSchedulerTokens != 0 {
		panic(errors.AssertionFailedf("empty send-queue and non-zero deductedForSchedulerTokens"))
	}
}

// probeRecentlyNoSendQDuration is the duration the controller will wait
// after observing a replica in StateProbe before closing the send stream if
// the replica remains in StateProbe.
//
// TODO(kvoli): We will want to make this a cluster setting eventually.
func probeRecentlyNoSendQDuration() time.Duration {
	return 400 * time.Millisecond
}

type connectedState uint32

// Local replicas are always in state replicate.
//
// Initial state for a replicaSendStream is always replicate, since it is
// created in StateReplicate. We don't care about whether the transport is
// connected or disconnected, since there is buffering capacity in the
// RaftTransport, which allows for some buffering and immediate sending when
// the RaftTransport stream reconnects (which may happen before the next
// HandleRaftEvent), which is desirable.
//
// The first false return value from SendRaftMessage will trigger a
// notification to Raft that the replica is unreachable (see
// Replica.sendRaftMessage calling Replica.addUnreachableRemoteReplica), and
// that raftpb.MsgUnreachable will cause the transition out of StateReplicate
// to StateProbe. The false return value happens either when the (generous)
// RaftTransport buffer is full, or when the circuit breaker opens. The
// circuit breaker opens 3-6s after no more TCP packets are flowing.
//
// A single transient message drop, and nack, can also cause a transition to
// StateProbe. At this layer we don't bother distinguishing on why this
// transition happened and first transition to probeRecentlyNoSendQ, if the
// replica had no send-queue. We stay in this state for a short time interval,
// and then close the replicaSendStream. If the replica transitions back to
// StateReplicate before this time interval elapses, we close the existing
// replicaSendStream and create a new one.
//
// No tokens are held in state probeRecentlyNoSendQ and no MsgApps are sent.
// We simply pretend that the replica has no send-queue.
//
// Initial states: replicate
// State transitions: replicate => probeRecentlyNoSendQ
const (
	replicate connectedState = iota
	probeRecentlyNoSendQ
)

func (cs connectedState) String() string {
	return redact.StringWithoutMarkers(cs)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (cs connectedState) SafeFormat(w redact.SafePrinter, _ rune) {
	switch cs {
	case replicate:
		w.SafeString("replicate")
	case probeRecentlyNoSendQ:
		w.SafeString("probeRecentlyNoSendQ")
	default:
		panic(fmt.Sprintf("unknown connectedState %v", cs))
	}
}

// entryTokensApproximator simply uses a mean of the entries observed to
// approximate the tokens needed. More sophisticated heuristics can be
// devised, if needed.
type entryTokensApproximator struct {
	numEntries int
	numTokens  kvflowcontrol.Tokens
}

// REQUIRES: numEntries > 0.
func (a *entryTokensApproximator) addStats(numEntries int, numTokens kvflowcontrol.Tokens) {
	a.numEntries += numEntries
	a.numTokens += numTokens
}

func (a *entryTokensApproximator) meanTokensPerEntry() kvflowcontrol.Tokens {
	if a.numEntries == 0 {
		return 500
	}
	mean := a.numTokens / kvflowcontrol.Tokens(a.numEntries)
	if mean == 0 {
		mean = 1
	}
	return mean
}

// forceFlushStopIndex is the inclusive index to send before force-flush can
// stop. When set to infinityEntryIndex, force-flush must continue until the
// send-queue is empty. The zero value implies no force-flush, even though
// this index is inclusive, since index 0 is never used in CockroachDB's use
// of Raft (see stateloader.RaftInitialLogIndex).
type forceFlushStopIndex uint64

// active returns whether the stream is force-flushing.
func (i forceFlushStopIndex) active() bool {
	return i != 0
}

// untilInfinity implies active.
func (i forceFlushStopIndex) untilInfinity() bool {
	return uint64(i) == infinityEntryIndex
}
