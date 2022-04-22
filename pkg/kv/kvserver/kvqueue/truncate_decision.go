package kvqueue

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

const (
	// RaftLogQueueStaleThreshold is the minimum threshold for stale raft log
	// entries. A stale entry is one which all replicas of the range have
	// progressed past and thus is no longer needed and can be truncated.
	RaftLogQueueStaleThreshold = 100

	// RaftLogQueueStaleSize is the minimum size of the Raft log that we'll
	// truncate even if there are fewer than RaftLogQueueStaleThreshold entries
	// to truncate. The value of 64 KB was chosen experimentally by looking at
	// when Raft log truncation usually occurs when using the number of entries
	// as the sole criteria.
	RaftLogQueueStaleSize = 64 << 10
)

// No assumption should be made about the relationship between
// RaftStatus.Commit, FirstIndex, LastIndex. This is because:
// - In some cases they are not updated or read atomically.
// - FirstIndex is a potentially future first index, after the pending
//   truncations have been applied. Currently, pending truncations are being
//   proposed through raft, so one can be sure that these pending truncations
//   do not refer to entries that are not already in the log. However, this
//   situation may change in the future. In general, we should not make an
//   assumption on what is in the local raft log based solely on FirstIndex,
//   and should be based on whether [FirstIndex,LastIndex] is a non-empty
//   interval.
type TruncateDecisionInput struct {
	RaftStatus            raft.Status
	LogSize, MaxLogSize   int64
	LogSizeTrusted        bool // false when LogSize might be off
	FirstIndex, LastIndex uint64
	PendingSnapshotIndex  uint64
}

func (input TruncateDecisionInput) LogTooLarge() bool {
	return input.LogSize > input.MaxLogSize
}

// TruncateDecision describes a truncation decision.
// Beware: when extending this struct, be sure to adjust .String()
// so that it is guaranteed to not contain any PII or confidential
// cluster data.
type TruncateDecision struct {
	Input       TruncateDecisionInput
	CommitIndex uint64

	NewFirstIndex uint64 // first index of the resulting log after truncation
	ChosenVia     string
}

func (td *TruncateDecision) RaftSnapshotsForIndex(index uint64) int {
	var n int
	for _, p := range td.Input.RaftStatus.Progress {
		if p.State != tracker.StateReplicate {
			// If the follower isn't replicating, we can't trust its Match in
			// the first place. But note that this shouldn't matter in practice
			// as we already take care to not cut off these followers when
			// computing the truncate decision. See:
			_ = truncatableIndexChosenViaProbingFollower // guru ref
			continue
		}

		// When a log truncation happens at the "current log index" (i.e. the
		// most recently committed index), it is often still in flight to the
		// followers not required for quorum, and it is likely that they won't
		// need a truncation to catch up. A follower in that state will have a
		// Match equaling committed-1, but a Next of committed+1 (indicating that
		// an append at 'committed' is already ongoing).
		if p.Match < index && p.Next <= index {
			n++
		}
	}
	if td.Input.PendingSnapshotIndex != 0 && td.Input.PendingSnapshotIndex < index {
		n++
	}

	return n
}

func (td *TruncateDecision) NumNewRaftSnapshots() int {
	return td.RaftSnapshotsForIndex(td.NewFirstIndex) - td.RaftSnapshotsForIndex(td.Input.FirstIndex)
}

// String returns a representation for the decision.
// It is guaranteed to not return PII or confidential
// information from the cluster.
func (td *TruncateDecision) String() string {
	var buf strings.Builder
	_, _ = fmt.Fprintf(&buf, "should truncate: %t [", td.ShouldTruncate())
	_, _ = fmt.Fprintf(
		&buf,
		"truncate %d entries to first index %d (chosen via: %s)",
		td.NumTruncatableIndexes(), td.NewFirstIndex, td.ChosenVia,
	)
	if td.Input.LogTooLarge() {
		_, _ = fmt.Fprintf(
			&buf,
			"; log too large (%s > %s)",
			humanizeutil.IBytes(td.Input.LogSize),
			humanizeutil.IBytes(td.Input.MaxLogSize),
		)
	}
	if n := td.NumNewRaftSnapshots(); n > 0 {
		_, _ = fmt.Fprintf(&buf, "; implies %d Raft snapshot%s", n, util.Pluralize(int64(n)))
	}
	if !td.Input.LogSizeTrusted {
		_, _ = fmt.Fprintf(&buf, "; log size untrusted")
	}
	buf.WriteRune(']')

	return buf.String()
}

func (td *TruncateDecision) NumTruncatableIndexes() int {
	if td.NewFirstIndex < td.Input.FirstIndex {
		return 0
	}
	return int(td.NewFirstIndex - td.Input.FirstIndex)
}

func (td *TruncateDecision) ShouldTruncate() bool {
	n := td.NumTruncatableIndexes()
	return n >= RaftLogQueueStaleThreshold ||
		(n > 0 && td.Input.LogSize >= RaftLogQueueStaleSize)
}

// ProtectIndex attempts to "protect" a position in the log by making sure it's
// not truncated away. Specifically it lowers the proposed truncation point
// (which will be the new first index after the truncation) to the given index
// if it would be truncating at a point past it. If a change is made, the
// ChosenVia is updated with the one given. This protection is not guaranteed if
// the protected index is outside of the existing [FirstIndex,LastIndex] bounds.
func (td *TruncateDecision) ProtectIndex(index uint64, chosenVia string) {
	if td.NewFirstIndex > index {
		td.NewFirstIndex = index
		td.ChosenVia = chosenVia
	}
}

const (
	truncatableIndexChosenViaCommitIndex     = "commit"
	truncatableIndexChosenViaFollowers       = "followers"
	truncatableIndexChosenViaProbingFollower = "probing follower"
	truncatableIndexChosenViaPendingSnap     = "pending snapshot"
	truncatableIndexChosenViaFirstIndex      = "first index"
	truncatableIndexChosenViaLastIndex       = "last index"
)

// ComputeTruncateDecision returns the oldest index that cannot be
// truncated. If there is a behind node, we want to keep old raft logs so it
// can catch up without having to send a full snapshot. However, if a node down
// is down long enough, sending a snapshot is more efficient and we should
// truncate the log to the next behind node or the quorum committed index. We
// currently truncate when the raft log size is bigger than the range
// size.
//
// Note that when a node is behind we continue to let the raft log build up
// instead of truncating to the commit index. Consider what would happen if we
// truncated to the commit index whenever a node is behind and thus needs to be
// caught up via a snapshot. While we're generating the snapshot, sending it to
// the behind node and waiting for it to be applied we would continue to
// truncate the log. If the snapshot generation and application takes too long
// the behind node will be caught up to a point behind the current first index
// and thus require another snapshot, likely entering a never ending loop of
// snapshots. See #8629.
func ComputeTruncateDecision(input TruncateDecisionInput) TruncateDecision {
	decision := TruncateDecision{Input: input}
	decision.CommitIndex = input.RaftStatus.Commit

	// The last index is most aggressive possible truncation that we could do.
	// Everything else in this method makes the truncation less aggressive.
	decision.NewFirstIndex = input.LastIndex
	decision.ChosenVia = truncatableIndexChosenViaLastIndex

	// Start by trying to truncate at the commit index. Naively, you would expect
	// LastIndex to never be smaller than the commit index, but
	// RaftStatus.Progress.Match is updated on the leader when a command is
	// proposed and in a single replica Raft group this also means that
	// RaftStatus.Commit is updated at propose time.
	decision.ProtectIndex(decision.CommitIndex, truncatableIndexChosenViaCommitIndex)

	for _, progress := range input.RaftStatus.Progress {
		// Snapshots are expensive, so we try our best to avoid truncating past
		// where a follower is.

		// First, we never truncate off a recently active follower, no matter how
		// large the log gets. Recently active shares the (currently 10s) constant
		// as the quota pool, so the quota pool should put a bound on how much the
		// raft log can grow due to this.
		//
		// For live followers which are being probed (i.e. the leader doesn't know
		// how far they've caught up), the Match index is too large, and so the
		// quorum index can be, too. We don't want these followers to require a
		// snapshot since they are most likely going to be caught up very soon (they
		// respond with the "right index" to the first probe or don't respond, in
		// which case they should end up as not recently active). But we also don't
		// know their index, so we can't possible make a truncation decision that
		// avoids that at this point and make the truncation a no-op.
		//
		// The scenario in which this is most relevant is during restores, where we
		// split off new ranges that rapidly receive very large log entries while
		// the Raft group is still in a state of discovery (a new leader starts
		// probing followers at its own last index). Additionally, these ranges will
		// be split many times over, resulting in a flurry of snapshots with
		// overlapping bounds that put significant stress on the Raft snapshot
		// queue.
		if progress.RecentActive {
			if progress.State == tracker.StateProbe {
				decision.ProtectIndex(input.FirstIndex, truncatableIndexChosenViaProbingFollower)
			} else {
				decision.ProtectIndex(progress.Match, truncatableIndexChosenViaFollowers)
			}
			continue
		}

		// Second, if the follower has not been recently active, we don't
		// truncate it off as long as the raft log is not too large.
		if !input.LogTooLarge() {
			decision.ProtectIndex(progress.Match, truncatableIndexChosenViaFollowers)
		}

		// Otherwise, we let it truncate to the committed index.
	}

	// The pending snapshot index acts as a placeholder for a replica that is
	// about to be added to the range (or is in Raft recovery). We don't want to
	// truncate the log in a way that will require that new replica to be caught
	// up via yet another Raft snapshot.
	if input.PendingSnapshotIndex > 0 {
		decision.ProtectIndex(input.PendingSnapshotIndex, truncatableIndexChosenViaPendingSnap)
	}

	// If new first index dropped below first index, make them equal (resulting
	// in a no-op).
	if decision.NewFirstIndex < input.FirstIndex {
		decision.NewFirstIndex = input.FirstIndex
		decision.ChosenVia = truncatableIndexChosenViaFirstIndex
	}

	// We've inherited the unfortunate semantics for {First,Last}Index from
	// raft.Storage: both {First,Last}Index are inclusive. The way we've
	// initialized repl.FirstIndex is to set it to the first index in the
	// possibly-empty log (TruncatedState.Index + 1), and allowing LastIndex to
	// fall behind it when the log is empty (TruncatedState.Index). The
	// initialization is done when minting a new replica from either the
	// truncated state of incoming snapshot, or using the default initial log
	// index. This makes for the confusing situation where FirstIndex >
	// LastIndex. We can detect this special empty log case by comparing
	// checking if `FirstIndex == LastIndex + 1`. Similar to this, we can have
	// the case that `FirstIndex = CommitIndex + 1` when there are no committed
	// entries. Additionally, FirstIndex adjusts for the pending log
	// truncations, which allows for FirstIndex to be greater than LastIndex and
	// commited index by more than 1 (see the comment with
	// TruncateDecisionInput). So all invariant checking below is gated on first
	// ensuring that the log is not empty, i.e., FirstIndex <= LastIndex.
	//
	// If the raft log is not empty, and there are committed entries, we can
	// assert on the following invariants:
	//
	//         FirstIndex    <= LastIndex                                    (0)
	//         NewFirstIndex >= FirstIndex                                   (1)
	//         NewFirstIndex <= LastIndex                                    (2)
	//         NewFirstIndex <= CommitIndex                                  (3)
	//
	// (1) asserts that we're not regressing our FirstIndex
	// (2) asserts that our we don't truncate past the last index we can
	//     truncate away, and
	// (3) is similar to (2) in that we assert that we're not truncating past
	//     the last known CommitIndex.
	//
	// TODO(irfansharif): We should consider cleaning up this mess around
	// {First,Last,Commit}Index by using a sentinel value to represent an empty
	// log (like we do with `invalidLastTerm`). It'd be extra nice if we could
	// safeguard access by relying on the type system to force callers to
	// consider the empty case. Something like
	// https://github.com/nvanbenschoten/optional could help us emulate an
	// `option<uint64>` type if we care enough.
	logEmpty := input.FirstIndex > input.LastIndex
	noCommittedEntries := input.FirstIndex > input.RaftStatus.Commit

	logIndexValid := logEmpty ||
		(decision.NewFirstIndex >= input.FirstIndex) && (decision.NewFirstIndex <= input.LastIndex)
	commitIndexValid := noCommittedEntries ||
		(decision.NewFirstIndex <= decision.CommitIndex)
	valid := logIndexValid && commitIndexValid
	if !valid {
		err := fmt.Sprintf("invalid truncation decision: output = %d, input: [%d, %d], commit idx = %d",
			decision.NewFirstIndex, input.FirstIndex, input.LastIndex, decision.CommitIndex)
		panic(err)
	}

	return decision
}
