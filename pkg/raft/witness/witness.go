package witness

import (
	"context"

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
- it "implicitly" acks every append from a leader it has voted for immediately
  (it happens to be unavailable for many other appends). The leader toggles
  this behavior depending on whether it currently relies on the witness for
  quorum or not.
- it "happens to be unavailable" whenever anything would require it to actually
  produce any log entry it may have (implicitly) acked.

As a result of this behavior, the witness doesn't actually need to store the
log, and the leader doesn't actually need to tell it about the log entries it
participated in getting quorum for. And, thankfully it turns out that in the
common case, the witness can actually vote (doesn't have to feign
unavailability) when it counts: broadly speaking, we have the diagram,
with n=2k voters (i.e. quorum=k+1) and one witness,

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
	ctx context.Context, from raftpb.PeerID, term raftpb.Term, lm raft.LogMark,
) (State, bool) {
	if term == s.Vote.Term && (s.Vote.VotedFor == from || s.Vote.VotedFor == 0) {
		// Either idempotent vote, or we learned about this term via engage/release
		// but haven't voted yet. Grant the vote, preserving existing acked state.
		s.Vote.VotedFor = from
		return s, true
	}

	if term <= s.Vote.Term {
		// Stale vote request, or already voted for someone else this term.
		return State{}, false
	}

	// Term advanced. Need to check for log compatibility before granting vote.

	// If the candidate has entries from a newer term in its log, this proves that
	// our leader's term ended without this witness having cast a vote. This means
	// there must have been a proper leader election for some newer term, and a
	// traditional "log-up-to-date" check on a quorum of regular voters.

	// Witness is still engaged from `s.Vote.Term`. If at all possible, we want
	// to convince ourselves that we can disengage. If the candidate's last log
	// term is not ahead of the term we support, it's possible that the leader
	// relied on us to commit log entries that this candidate is not up to date
	// with.
	// For example, the term 10 leader could have asked us (at t5i100) to
	// support all future appends in this term, so when a follower shows up with
	// their log ending at t5i109, we don't know if we can vote - for all we
	// know, the leader had a longer log and committed it with our help:
	// - term 10 leader's log: t5i100 ... t5i110 relied_on_witness=t5i110
	// - witness state: term=10 hi=t5i100 engaged=true
	// - candidate's request: term=15 lm=t5i109
	// Note that from the witness' perspective, this is indistinguishable from
	// the same situation but with a different leader log, in which the witness
	// could vote, but has no way of discovering the fact.
	// - term 10 leader's log: t5i100 ... t5i105 relied_on_witness=t5i110
	// - (rest unchanged)
	//
	// But if the candidate has an entry from a strictly newer term in its log,
	// we know that there was a traditional-quorum leader election without the
	// witness' involvement. This means that the old leader is unable to commit
	// new entries (a traditional quorum in `n=2k` overlaps all near-quorums of
	// size `k`). Because the candidate's log has an entry with a newer log
	// term, we also know that the candidate's log is up to date with any prefix
	// of the previous leader's log that was ultimately committed. We can thus
	// disregard the engagement while deciding whether to respond to this vote.

	if s.Acked.Engaged && lm.Term <= uint64(s.Vote.Term) {
		return State{}, false
	}

	if !leq(s.Acked.Hi, lm) {
		// Log-up-to-date check failed.
		return State{}, false
	}

	// We can grant the vote. Note that if we disengaged above, the outcome
	// of that will be included in the returned state.
	next := State{
		Vote: Vote{
			Term:     term,
			VotedFor: from,
		},
		Acked: Acked{}, // not engaged
	}
	return next, true
}

// maybeBumpTerm reacts to a message from a leader at a higher term by resetting
// state to that leader's term. Returns identical state if term not
// incremented.
func (s State) maybeBumpTerm(ctx context.Context, term raftpb.Term) State {
	if term <= s.Vote.Term {
		// No-op.
		return s
	}

	// Term changed, save leader term.
	// Note that the
	return State{
		Vote: Vote{Term: term, VotedFor: 0 /* did not vote in this term */},
		// The current leader was elected without our help. This means its log is
		// up to date with a traditional quorum of voters, so it has all the log
		// entries we may have helped commit.
		Acked: Acked{},
	}
}

func (s State) HandleMsgEngage(
	ctx context.Context, term raftpb.Term, lm raft.LogMark,
) (State, bool) {
	s = s.maybeBumpTerm(ctx, term)

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
	ctx context.Context, term raftpb.Term, lm raft.LogMark,
) (State, bool) {
	s = s.maybeBumpTerm(ctx, term)

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
