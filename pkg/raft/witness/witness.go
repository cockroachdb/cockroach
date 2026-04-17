package witness

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
)

/*
# Logless Witness Replicas

## Motivation

A full raft replica receives all entries in the log and materializes them into a
state machine. At least three full replicas are required today to ensure
availability and durability. This is expensive in terms of disk space and
bandwidth utilization, has an associated CPU cost, and, since replicas are
typically spread across AZs or regions, incurs significant network cost.

To reduce savings, it is desirable to be able to replace at least one of the
full replicas with a "logless" witness replica whose only responsibility is to
help elect a leader that can commit log entries even when a quorum of full
replicas is unavailable.

For example, two full replicas and a witness can make progress even if one of
the full replicas becomes unavailable. In general, `n=2k` full replicas and a
witness can make progress even if `k` of the full replicas become unavailable.

Witnesses have been discussed in the past, see:
https://docs.google.com/document/d/13BCAgay8LHF4lWOF1Pc3xp7TyymsWgV6GV2c3VTdtJ4/edit?usp=sharing

This package is an exploration of an approach that attempts to be as simple
as possible by modeling a witness as a full voter that "just happens to be
unavailable" in certain situations. This approach avoids treading new ground
as far as Raft correctness is concerned, and in particular we avoid having
to think deeply about configuration changes.

Concretely, in broad strokes, in this design, a `n=2k` configuration with a
witness is just a `2k+1` configuration in which:

- the witness can only vote when a regular voter would (but may "happen to be
  unavailable") in certain situations
- it can promise the leader to not respond to any other append in the term,
  so the leader can "implicitly" count the witness for commit quorum whenever
  it needs it. (When the witness is not needed, the leader tells it, which
  improves availability on leader failure, see below).
- it "happens to be unavailable" whenever anything would require it to actually
  produce any log entry it may have (implicitly) acked.

As a result of this behavior, the witness doesn't actually need to store the
log, and the leader doesn't actually need to tell it about the log entries it
participated in getting quorum for. In the common case of loss of k=n/2 voters,
the witness can vote (doesn't have to feign unavailability) and elect a new
leader (or maintain the existing leader's ability to reach quorum). Only when
additionally the leader fails is the group unavailable until a majority of
*voters* becomes available again (perhaps surprisingly, the witness is allowed
to fail).

	            ┌──────────── k replicas down ───────────┐
	            │            (witness still up)          │
	            │                                        ▼
	   ┌────────┴───────┐                       ┌────────────────┐◄──┐
	   │ OK (no witness │     quorum up         │ OK (witness    │   │ witness
	   │     needed)    │◄──────────────────────│    in use)     │───┘ down
	   └────────────────┘                       └───────┬────────┘
	            ▲                                       │
	            │                                leader │
	            │                                 dies  │
	            │                                       ▼
	            │          quorum up            ┌──────────────┐
	            └───────────────────────────────┤    Down      │
	                                            └──────────────┘
Interestingly, if quorum is reached through a witness, it's okay if the witness
dies as long as the leader it helped elect remains active - this would not be
the case with a log-based witness.
However, if the leader goes down while using a witness, availability is lost
until quorum is restored (or the same leader campaigns again). A log-based
witness would not have this problem, though in the common case of `n=2`, there
typically isn't an alternative leader available until quorum heals anyway.
*/

type Vote struct {
	Term     raftpb.Term
	VotedFor raftpb.PeerID // may be zero if we never voted in this term
}

// Acked tracks the witness' knowledge of prefix of the log (for an associated
// supported leader term) it may have helped reach quorum for.
type Acked struct {
	// Hi is the (inclusive) high water mark over all entries for which the
	// witness considers itself a part of the commit quorum. Across re-engagements
	// of a leader, this field increases strictly monotonically with each
	// engagement to provide replay protection (see maybeDisengage). On the other
	// hand, the leader is allowed to release at `Hi`.
	//
	// We simplify the engagement mechanism by only ever promising to help achieve
	// commit quorum for entries in the leader's own term (there is always the
	// empty entry the leader proposes, so this is not a meaningful restriction).
	// A motivating example is a leader that gets elected at term 10 but whose last
	// log term is (say) 5. If that leader engaged us to commit these past-term
	// entries, Hi would refer to a past-term mark. The reasoning around winding
	// down engagements after leader failure would become much more subtle, and
	// invoke memories of past raft bugs[1]. Leaving as an exercise to the reader
	// if there is an actual bug here if we don't restrict Hi to the current term.
	// [1]: https://groups.google.com/g/raft-dev/c/t4xj6dJTP6E/m/d2D9LrWRza8J
	Hi raft.LogMark
	// If Engaged is true, the witness has provided the associated leader with a
	// blanket promise that it will consider itself a part of the commit quorum
	// for any possible LogMark{Term:term, Index:∞}. When the leader releases,
	// it will provide a new value of Hi beyond which it releases our promise.
	Engaged bool
}

type State struct {
	Vote  Vote
	Acked Acked
}

func leq(a, b raft.LogMark) bool {
	return a == b || b.After(a)
}

func le(a, b raft.LogMark) bool {
	return b.After(a)
}

func (s State) HandleMsgVote(
	ctx context.Context, from raftpb.PeerID, candTerm raftpb.Term, lm raft.LogMark,
) (State, bool) {
	// We intentionally do NOT call maybeBumpTerm here and instead bump the term
	// only if we can actually grant the vote. This gives us the invariant that
	// while an engagement is active, our Term reflects the engagement and
	// VotedFor the leader that engagement is for. We could track these explicitly
	// with the engagement, but higher cognitive load would result from having two
	// terms and leaders floating around.

	if candTerm == s.Vote.Term && (s.Vote.VotedFor == from || s.Vote.VotedFor == 0) {
		// Either idempotent vote, or we learned about this term via engage/release
		// but haven't voted yet. Grant the vote, preserving existing acked state.
		s.Vote.VotedFor = from
		return s, true
	}

	if candTerm <= s.Vote.Term {
		// Stale vote request, or already voted for someone else this term.
		return State{}, false
	}

	// Campaign is at strictly higher term. Need to check for log compatibility
	// before granting vote. Any log definitely must be up-to-date with
	// `s.Acked.Hi` at a minimum.
	//
	// If engaged, the witness may have implicitly acked any log entry in the
	// engagement term (it must have an entry from a more recent term in its log,
	// which proves that it is up to date with whatever actually happened in the
	// engagement term).
	// The exception we carve out is if we're voting for the engagement's leader:
	// its log is up to date by definition. Importantly, this allows the leader to
	// win an election after it failed while using the witness, when it may be the
	// only possible leader (e.g. 2v+w config with one voter down).
	effectiveHi := s.Acked.Hi
	if s.Acked.Engaged {
		if s.Vote.VotedFor == from {
			effectiveHi = lm
		} else {
			effectiveHi.Index = math.MaxUint64
		}
	}

	if !leq(effectiveHi, lm) {
		// Log-up-to-date check failed.
		return State{}, false
	}

	// We grant the vote. For simplicity, we want the existing engagement (if any)
	// to terminate, since it was from a previous term. We do not know if the
	// candidate will win the election, and its log mark may reflect a higher term
	// and an index that may never actually commit, so we don't want to leak that
	// into the high water mark. Instead, we close the engagement out at term ∞.
	// Note that if the engagement-term leader tries to campaign in the future
	// without having a newer term in its log (which can happen if this vote that
	// unwinds the engagement succeeds but the campaign fails), the witness will
	// not be able to grant the vote. This is a non-issue with pre-vote, but if we
	// explicitly remembered the leader of the most recent engagement, we could
	// waive the log completeness check even in that case - for now we don't.
	var acked Acked
	if s.Acked.Engaged {
		acked.Hi = raft.LogMark{
			Term:  uint64(s.Vote.Term),
			Index: math.MaxUint64,
		}
	}
	next := State{
		Vote: Vote{
			Term:     candTerm,
			VotedFor: from,
		},
		Acked: acked,
	}
	return next, true
}

// maybeBumpTerm reacts to a message from a leader at a higher term by resetting
// state to that leader's term. Only called for leader-sourced messages
// (engage/release), so the leader's identity is recorded. If the term doesn't
// advance but VotedFor is unset, we record the leader — accepting an
// engage/release is effectively acknowledging the leader.
func (s State) maybeBumpTerm(ctx context.Context, term raftpb.Term, lead raftpb.PeerID) State {
	if term < s.Vote.Term {
		return s
	}
	if term == s.Vote.Term {
		if s.Vote.VotedFor == 0 {
			s.Vote.VotedFor = lead
		}
		return s
	}
	// Term advanced. The leader was elected without our help, so its log
	// is up to date with a traditional quorum of voters.
	return State{
		Vote:  Vote{Term: term, VotedFor: lead},
		Acked: Acked{},
	}
}

func (s State) HandleMsgEngage(
	ctx context.Context, from raftpb.PeerID, term raftpb.Term, lm raft.LogMark,
) (State, bool) {
	s = s.maybeBumpTerm(ctx, term, from)

	if term != s.Vote.Term {
		// Stale message.
		return State{}, false
	}

	if s.Acked.Engaged && s.Acked.Hi == lm {
		// Idempotency.
		return s, true
	}

	if lm.Term < uint64(s.Vote.Term) {
		// Refuse to engage directly on behalf of old log terms to keep things
		// simple. See `Acked.Hi`.
		return State{}, false
	}

	if !le(s.Acked.Hi, lm) {
		// Stale request - engagements need to be for a strictly larger log mark
		// than the last release due to replay protection.
		return State{}, false
	}

	return State{
		Vote: s.Vote,
		Acked: Acked{
			Hi:      lm,
			Engaged: true,
		},
	}, true
}

func (s State) HandleMsgRelease(
	ctx context.Context, from raftpb.PeerID, term raftpb.Term, lm raft.LogMark,
) (State, bool) {
	s = s.maybeBumpTerm(ctx, term, from)

	if term != s.Vote.Term {
		// Stale message from old leader.
		return State{}, false
	}

	if !leq(s.Acked.Hi, lm) {
		// Stale message from current leader.
		return State{}, false
	}

	// Newer or idempotent release.
	next := State{
		Vote: s.Vote,
		Acked: Acked{
			Hi:      lm,
			Engaged: false,
		},
	}
	return next, true
}
