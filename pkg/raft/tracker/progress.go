// This code has been modified from its original form by Cockroach Labs, Inc.
// All modifications are Copyright 2024 Cockroach Labs, Inc.
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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
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

	// sentCommit is the highest commit index in flight to the follower.
	//
	// Generally, it is monotonic, but con regress in some cases, e.g. when
	// converting to `StateProbe` or when receiving a rejection from a follower.
	//
	// In StateSnapshot, sentCommit == PendingSnapshot == Next-1.
	sentCommit uint64

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

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	// This is always true on the leader.
	RecentActive bool

	// MsgAppProbesPaused is used when the MsgApp flow to a node is throttled. This
	// happens in StateProbe, or StateReplicate with saturated Inflights. In both
	// cases, we need to continue sending MsgApp once in a while to guarantee
	// progress, but we only do so when MsgAppProbesPaused is false (it is reset on
	// receiving a heartbeat response), to not overflow the receiver. See
	// IsPaused() and ShouldSendMsgApp().
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
	pr.sentCommit = min(pr.sentCommit, pr.Next-1)
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
	pr.sentCommit = snapshoti
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
	return index > pr.sentCommit && pr.sentCommit < pr.Next-1
}

// SentCommit updates the sentCommit.
func (pr *Progress) SentCommit(commit uint64) {
	pr.sentCommit = commit
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
		// Regress the sentCommit since it unlikely has been applied.
		pr.sentCommit = min(pr.sentCommit, pr.Next-1)
		return true
	}

	// The rejection must be stale if "rejected" does not match next - 1. This
	// is because non-replicating followers are probed one entry at a time.
	// The check is a best effort assuming message reordering is rare.
	if pr.Next-1 != rejected {
		return false
	}

	pr.Next = max(min(rejected, matchHint+1), pr.Match+1)
	// Regress the sentCommit since it unlikely has been applied.
	pr.sentCommit = min(pr.sentCommit, pr.Next-1)
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

// ShouldSendMsgApp returns true if the leader should send a MsgApp to the
// follower represented by this Progress. The given last and commit index of the
// leader log help determining if there is outstanding workload, and contribute
// to this decision-making.
//
// In StateProbe, a message is sent periodically. The flow is paused after every
// message, and un-paused on a heartbeat response. This ensures that probes are
// not too frequent, and eventually the MsgApp is either accepted or rejected.
//
// In StateReplicate, generally a message is sent if there are log entries that
// are not yet in-flight, and the in-flight limits are not exceeded. Otherwise,
// we don't send a message, or send a "probe" message in a few situations.
//
// A probe message (containing no log entries) is sent if the follower's commit
// index can be updated, or there hasn't been a probe message recently. We must
// send a message periodically even if all log entries are in-flight, in order
// to guarantee that eventually the flow is either accepted or rejected.
//
// In StateSnapshot, we do not send append messages.
func (pr *Progress) ShouldSendMsgApp(last, commit uint64) bool {
	switch pr.State {
	case StateProbe:
		return !pr.MsgAppProbesPaused

	case StateReplicate:
		// If the in-flight limits are not saturated, and there are pending entries
		// (Next <= lastIndex), send a MsgApp with some entries.
		if pr.CanSendEntries(last) {
			return true
		}
		// We can't send any entries at this point, but we need to be sending a
		// MsgApp periodically, to guarantee liveness of the MsgApp flow: the
		// follower eventually will reply with an ack or reject.
		//
		// If the follower's log is outdated, and we haven't recently sent a MsgApp
		// (according to the MsgAppProbesPaused flag), send one now. This is going
		// to be an empty "probe" MsgApp.
		if pr.Match < last && !pr.MsgAppProbesPaused {
			return true
		}
		// Send an empty MsgApp containing the latest commit index if:
		//	- our commit index exceeds the in-flight commit index, and
		//	- sending it can commit at least one of the follower's entries
		//	  (including the ones still in flight to it).
		return pr.CanBumpCommit(commit)

	case StateSnapshot:
		return false
	default:
		panic("unexpected state")
	}
}

func (pr *Progress) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s match=%d next=%d", pr.State, pr.Match, pr.Next)
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
	ids := make([]pb.PeerID, 0, len(m))
	for k := range m {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	var buf strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&buf, "%d: %s\n", id, m[id])
	}
	return buf.String()
}

// Get implements the ComparableMap interface.
func (m ProgressMap) Get(id pb.PeerID) (quorum.Index, bool) {
	pr, ok := m[id]
	if !ok {
		return 0, false
	}
	return quorum.Index(pr.Match), true
}
