// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracker

import (
	"fmt"
	"slices"
	"strings"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"golang.org/x/exp/maps"
)

// Progress represents a follower’s progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
//
// NB(tbg): Progress is basically a state machine whose transitions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal.
//
// TODO(pav-kv): consolidate all flow control state changes here. Much of the
// transitions in raft.go logically belong here.
type Progress struct {
	// Match is the index up to which the follower's log is known to match the
	// leader's.
	Match uint64
	// Next is the log index of the next entry to send to this follower. All
	// entries with indices in (Match, Next) interval are already in flight.
	//
	// Invariant: 0 <= Match < Next.
	// NB: it follows that Next >= 1.
	//
	// In StateSnapshot, Next == PendingSnapshot + 1.
	Next uint64

	// SentCommit is the highest commit index in flight to the follower.
	//
	// Generally, it is monotonic, but con regress in some cases, e.g. when
	// converting to `StateProbe` or when receiving a rejection from a follower.
	//
	// In StateSnapshot, SentCommit == PendingSnapshot == Next-1.
	SentCommit uint64

	// MatchCommit is the commit index at which the follower is known to match the
	// leader. It is durable on the follower.
	// Best-effort invariant: MatchCommit <= SentCommit
	// It's a best-effort invariant because it doesn't really affect correctness.
	// The worst case if MatchCommit > SentCommit is that the leader will send
	// and extra MsgApp to the follower.
	MatchCommit uint64

	// State defines how the leader should interact with the follower.
	//
	// When in StateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in StateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in StateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	State StateType

	// PendingSnapshot is used in StateSnapshot and tracks the last index of the
	// leader at the time at which it realized a snapshot was necessary. This
	// matches the index in the MsgSnap message emitted from raft.
	//
	// While there is a pending snapshot, replication to the follower is paused.
	// The follower will transition back to StateReplicate if the leader
	// receives an MsgAppResp from it that reconnects the follower to the
	// leader's log (such an MsgAppResp is emitted when the follower applies a
	// snapshot). It may be surprising that PendingSnapshot is not taken into
	// account here, but consider that complex systems may delegate the sending
	// of snapshots to alternative datasources (i.e. not the leader). In such
	// setups, it is difficult to manufacture a snapshot at a particular index
	// requested by raft and the actual index may be ahead or behind. This
	// should be okay, as long as the snapshot allows replication to resume.
	//
	// The follower will transition to StateProbe if ReportSnapshot is called on
	// the leader; if SnapshotFinish is passed then PendingSnapshot becomes the
	// basis for the next attempt to append. In practice, the first mechanism is
	// the one that is relevant in most cases. However, if this MsgAppResp is
	// lost (fallible network) then the second mechanism ensures that in this
	// case the follower does not erroneously remain in StateSnapshot.
	PendingSnapshot uint64

	// RecentActive is true if the progress is recently active. Receiving any
	// messages from the corresponding follower indicates the progress is active.
	// Also, it's set to true on every heartbeat timeout if the follower is
	// fortifying the leader.
	// RecentActive can be reset to false after an election timeout.
	// This is always true on the leader.
	RecentActive bool

	// MsgAppProbesPaused is used when the MsgApp flow to a node is throttled.
	// This happens in StateProbe, or StateReplicate with saturated Inflights. In
	// both cases, we need to continue sending MsgApp once in a while to guarantee
	// progress, but we only do so when MsgAppProbesPaused is false to avoid
	// spinning.
	// MsgAppProbesPaused is reset on the next MsgHeartbeatResp from the follower,
	// or on next heartbeat timeout if the follower's store supports the leader's
	// store.
	// See IsPaused(), ShouldSendEntries(), and ShouldSendMsgApp().
	MsgAppProbesPaused bool

	// Inflights is a sliding window for the inflight messages.
	// Each inflight message contains one or more log entries.
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// When inflights is Full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.FreeLE with the index of the last
	// received entry.
	Inflights *Inflights

	// IsLearner is true if this progress is tracked for a learner.
	IsLearner bool
}

// ResetState moves the Progress into the specified State, resetting MsgAppProbesPaused,
// PendingSnapshot, and Inflights.
func (pr *Progress) ResetState(state StateType) {
	pr.MsgAppProbesPaused = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.Inflights.reset()
}

// BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
// optionally and if larger, the index of the pending snapshot.
func (pr *Progress) BecomeProbe() {
	// If the original state is StateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	if pr.State == StateSnapshot {
		pendingSnapshot := pr.PendingSnapshot
		pr.ResetState(StateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.ResetState(StateProbe)
		pr.Next = pr.Match + 1
	}
	pr.SentCommit = min(pr.SentCommit, pr.Next-1)
}

// BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
func (pr *Progress) BecomeReplicate() {
	pr.ResetState(StateReplicate)
	pr.Next = pr.Match + 1
}

// BecomeSnapshot moves the Progress to StateSnapshot with the specified pending
// snapshot index.
func (pr *Progress) BecomeSnapshot(snapshoti uint64) {
	pr.ResetState(StateSnapshot)
	pr.PendingSnapshot = snapshoti
	pr.Next = snapshoti + 1
	pr.SentCommit = snapshoti
}

// SentEntries updates the progress on the given number of consecutive entries
// being sent in a MsgApp, with the given total bytes size, appended at log
// indices >= pr.Next.
//
// Must be used with StateProbe or StateReplicate.
func (pr *Progress) SentEntries(entries int, bytes uint64) {
	if pr.State == StateReplicate && entries > 0 {
		pr.Next += uint64(entries)
		pr.Inflights.Add(pr.Next-1, bytes)
	}
	pr.MsgAppProbesPaused = true
}

// CanSendEntries returns true if the flow control state allows sending at least
// one log entry to this follower.
//
// Must be used with StateProbe or StateReplicate.
func (pr *Progress) CanSendEntries(lastIndex uint64) bool {
	return pr.Next <= lastIndex && (pr.State == StateProbe || !pr.Inflights.Full())
}

// CanBumpCommit returns true if sending the given commit index can potentially
// advance the follower's commit index.
func (pr *Progress) CanBumpCommit(index uint64) bool {
	// Sending the given commit index may bump the follower's commit index up to
	// Next-1 in normal operation, or higher in some rare cases. Allow sending a
	// commit index eagerly only if we haven't already sent one that bumps the
	// follower's commit all the way to Next-1.
	return index > pr.SentCommit && pr.SentCommit < pr.Next-1
}

// IsFollowerCommitStale returns true if the follower's commit index it less
// than index.
// If the follower's commit index+1 is pr.Next, it means that sending a larger
// commit index won't change anything, therefore we don't send it.
func (pr *Progress) IsFollowerCommitStale(index uint64) bool {
	return index > pr.MatchCommit && pr.MatchCommit+1 < pr.Next
}

// MaybeUpdateSentCommit updates the SentCommit if it needs to be updated.
func (pr *Progress) MaybeUpdateSentCommit(commit uint64) {
	if commit > pr.SentCommit {
		pr.SentCommit = commit
	}
}

// MaybeUpdate is called when an MsgAppResp arrives from the follower, with the
// index acked by it. The method returns false if the given n index comes from
// an outdated message. Otherwise it updates the progress and returns true.
func (pr *Progress) MaybeUpdate(n uint64) bool {
	if n <= pr.Match {
		return false
	}
	pr.Match = n
	pr.Next = max(pr.Next, n+1) // invariant: Match < Next
	return true
}

// MaybeUpdateMatchCommit updates the match commit from a follower if it's
// larger than the previous match commit.
func (pr *Progress) MaybeUpdateMatchCommit(commit uint64) {
	if commit > pr.MatchCommit {
		pr.MatchCommit = commit
		pr.SentCommit = max(pr.SentCommit, commit) // Best-effort invariant: SentCommit >= MatchCommit
	}
}

// MaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
// arguments are the index of the append message rejected by the follower, and
// the hint that we want to decrease to.
//
// Rejections can happen spuriously as messages are sent out of order or
// duplicated. In such cases, the rejection pertains to an index that the
// Progress already knows were previously acknowledged, and false is returned
// without changing the Progress.
//
// If the rejection is genuine, Next is lowered sensibly, and the Progress is
// cleared for sending log entries.
func (pr *Progress) MaybeDecrTo(rejected, matchHint uint64) bool {
	if pr.State == StateReplicate {
		// The rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match {
			return false
		}
		// Directly decrease next to match + 1.
		//
		// TODO(tbg): why not use matchHint if it's larger?
		pr.Next = pr.Match + 1
		// Regress the SentCommit since it unlikely has been applied.
		pr.SentCommit = min(pr.SentCommit, pr.Next-1)
		return true
	}

	// The rejection must be stale if "rejected" does not match next - 1. This
	// is because non-replicating followers are probed one entry at a time.
	// The check is a best effort assuming message reordering is rare.
	if pr.Next-1 != rejected {
		return false
	}

	pr.Next = max(min(rejected, matchHint+1), pr.Match+1)
	// Regress the SentCommit since it unlikely has been applied.
	pr.SentCommit = min(pr.SentCommit, pr.Next-1)
	pr.MsgAppProbesPaused = false
	return true
}

// IsPaused returns whether sending log entries to this node has been throttled.
// This is done when a node has rejected recent MsgApps, is currently waiting
// for a snapshot, or has reached the MaxInflightMsgs limit. In normal
// operation, this is false. A throttled node will be contacted less frequently
// until it has reached a state in which it's able to accept a steady stream of
// log entries again.
//
// TODO(pav-kv): this method is only used for follower pausing. Find a way to
// remove it.
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case StateProbe:
		return pr.MsgAppProbesPaused
	case StateReplicate:
		return pr.MsgAppProbesPaused && pr.Inflights.Full()
	case StateSnapshot:
		return true
	default:
		panic("unexpected state")
	}
}

// ShouldSendEntries returns true if the leader should send a MsgApp with at
// least one entry, to the follower represented by this Progress. The given last
// index of the leader log helps to determine if there is outstanding work.
//
// In StateProbe, a message is sent periodically. The flow is paused after every
// message, and un-paused on a heartbeat response. This ensures that probes are
// not too frequent, and eventually the MsgApp is either accepted or rejected.
//
// In StateReplicate, generally a message is sent if there are log entries that
// are not yet in-flight, and the in-flight limits are not exceeded. Otherwise,
// we don't send a message, or send a "probe" message in a few situations (see
// ShouldSendPing). If lazyReplication flag is true, entries sending is disabled
// and delegated to the application layer.
//
// In StateSnapshot, we do not send append messages.
func (pr *Progress) ShouldSendEntries(last uint64, lazyReplication bool) bool {
	switch pr.State {
	case StateProbe:
		return !pr.MsgAppProbesPaused && pr.CanSendEntries(last)
	case StateReplicate:
		return !lazyReplication && pr.CanSendEntries(last)
	case StateSnapshot:
		return false
	default:
		panic("unexpected state")
	}
}

// ShouldSendProbe returns true if the leader should send a "probe" MsgApp to
// this peer.
//
// A probe message (containing no log entries) is sent if the peer's Match and
// MatchCommit indices have not converged to the leader's state, and a MsgApp
// has not been sent recently.
//
// We must send a message periodically even if all updates are already in flight
// to this peer, to guarantee that eventually the flow is either accepted or
// rejected.
func (pr *Progress) ShouldSendProbe(last, commit uint64, advanceCommit bool) bool {
	switch pr.State {
	case StateProbe:
		return !pr.MsgAppProbesPaused

	case StateReplicate:
		// If the follower's log is outdated, and we haven't recently sent a MsgApp
		// (according to the MsgAppProbesPaused flag), send one now.
		if pr.Match < last && !pr.MsgAppProbesPaused {
			return true
		}
		// Send an empty MsgApp containing the latest commit index if:
		//	- our commit index exceeds the in-flight commit index, and
		//	- sending it can commit at least one of the follower's entries
		//	  (including the ones still in flight to it).
		if pr.CanBumpCommit(commit) {
			return true
		}

		// Send the latest commit index if we know that the peer's commit index is
		// stale, and we haven't recently sent a MsgApp (according to the
		// MsgAppProbesPaused flag).
		//
		// NOTE: This is a different condition than the one above because we only
		// send this message if pr.MsgAppProbesPaused is false. After this message,
		// pr.MsgAppProbesPaused will be set to true until we receive a heartbeat
		// response from the follower. In contrast, the condition above can keep
		// sending empty MsgApps eagerly until we have sent the latest commit index
		// to the follower.
		// TODO(iskettaneh): Remove the dependency on MsgAppProbesPaused to send
		// MsgApps.
		if advanceCommit {
			return pr.IsFollowerCommitStale(commit) && !pr.MsgAppProbesPaused
		}
		return false

	case StateSnapshot:
		return false
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s match=%d next=%d sentCommit=%d matchCommit=%d", pr.State, pr.Match,
		pr.Next, pr.SentCommit, pr.MatchCommit)
	if pr.IsLearner {
		fmt.Fprint(&buf, " learner")
	}
	if pr.IsPaused() {
		fmt.Fprint(&buf, " paused")
	}
	if pr.PendingSnapshot > 0 {
		fmt.Fprintf(&buf, " pendingSnap=%d", pr.PendingSnapshot)
	}
	if !pr.RecentActive {
		fmt.Fprint(&buf, " inactive")
	}
	if n := pr.Inflights.Count(); n > 0 {
		fmt.Fprintf(&buf, " inflight=%d", n)
		if pr.Inflights.Full() {
			fmt.Fprint(&buf, "[full]")
		}
	}
	return buf.String()
}

// ProgressMap is a map of *Progress.
type ProgressMap map[pb.PeerID]*Progress

// MakeEmptyProgressMap constructs and returns an empty ProgressMap.
func MakeEmptyProgressMap() ProgressMap {
	return make(ProgressMap)
}

// String prints the ProgressMap in sorted key order, one Progress per line.
func (m ProgressMap) String() string {
	ids := maps.Keys(m)
	slices.Sort(ids)
	var buf strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&buf, "%d: %s\n", id, m[id])
	}
	return buf.String()
}

// BasicProgress contains a subset of fields from Progress.
type BasicProgress struct {
	// Match corresponds to Progress.Match.
	Match uint64
	// Next corresponds to Progress.Next.
	Next uint64
	// State corresponds to Progress.State.
	State StateType
}
