package witness

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/redact"
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

// Formatting wrapper types for use in log messages. These produce the same
// letter-based notation used in the test harness (a-z for terms 1-26, etc.).

type fmtTerm raftpb.Term

func (t fmtTerm) SafeFormat(w redact.SafePrinter, _ rune) {
	if t >= 1 && t <= 26 {
		w.Printf("%c", rune('a'+byte(t-1)))
	} else {
		w.Printf("%d", uint64(t))
	}
}

func (t fmtTerm) String() string {
	return redact.StringWithoutMarkers(t)
}

type logMark raft.LogMark

func (l logMark) SafeFormat(w redact.SafePrinter, _ rune) {
	if l == (logMark{}) {
		w.SafeString("0")
		return
	}
	if l.Index == math.MaxUint64 {
		w.Printf("%s∞", fmtTerm(l.Term))
		return
	}
	w.Printf("%s%d", fmtTerm(l.Term), l.Index)
}

func (l logMark) String() string {
	return redact.StringWithoutMarkers(l)
}

type Vote struct {
	Term     raftpb.Term
	VotedFor raftpb.PeerID // may be zero if we never voted in this term
}

func (v Vote) SafeFormat(w redact.SafePrinter, _ rune) {
	if v == (Vote{}) {
		w.SafeString("none")
		return
	}
	if v.VotedFor == 0 {
		w.Printf("(%s, -)", fmtTerm(v.Term))
	} else {
		w.Printf("(%s, v%d)", fmtTerm(v.Term), v.VotedFor)
	}
}

func (v Vote) String() string {
	return redact.StringWithoutMarkers(v)
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

func (a Acked) SafeFormat(w redact.SafePrinter, _ rune) {
	if a == (Acked{}) {
		w.SafeString("none")
		return
	}
	eng := "released"
	if a.Engaged {
		eng = "engaged"
	}
	w.Printf("(%s, %s)", logMark(a.Hi), eng)
}

func (a Acked) String() string {
	return redact.StringWithoutMarkers(a)
}

type State struct {
	Vote  Vote
	Acked Acked
}

func (s State) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("vote=%s acked=%s", s.Vote, s.Acked)
}

func (s State) String() string {
	return redact.StringWithoutMarkers(s)
}

func leq(a, b raft.LogMark) bool {
	return a == b || b.After(a)
}

func le(a, b raft.LogMark) bool {
	return b.After(a)
}

func (s State) HandleMsgVote(
	logger raftlogger.Logger, from raftpb.PeerID, candTerm raftpb.Term, lm raft.LogMark,
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
		logger.Infof("granted vote for v%d at term %s (same-term)", from, fmtTerm(candTerm))
		return s, true
	}

	if candTerm <= s.Vote.Term {
		// Stale vote request, or already voted for someone else this term.
		logger.Infof(
			"rejected vote for v%d: candidate term %s <= vote term %s (votedFor=v%d)",
			from, fmtTerm(candTerm), fmtTerm(s.Vote.Term), s.Vote.VotedFor,
		)
		return State{}, false
	}

	// Campaign is at strictly higher term. Need to check for log compatibility
	// before granting vote. The candidate's log must be up-to-date with
	// effectiveHi, which is our best knowledge of what we may have helped commit.
	//
	// If engaged, the witness may have implicitly acked any log entry in the
	// engagement term, so effectiveHi is inflated to (engagementTerm, ∞). The
	// candidate must then have an entry from a strictly newer term in its log,
	// which proves that it is up to date with whatever actually happened in the
	// engagement term.
	//
	// The exception is if we're voting for the engagement's leader: its log is up
	// to date by definition, so we skip the check entirely. Importantly, this
	// allows the leader to win an election after it failed while using the
	// witness, when it may be the only possible leader (e.g. 2v+w config with one
	// voter down). This relies on prevote to prevent disruptive candidates from
	// overwriting VotedFor and breaking the leader identification; without
	// prevote, a competing election can cause availability loss.
	effectiveHi := s.Acked.Hi
	if s.Acked.Engaged {
		effectiveHi.Index = math.MaxUint64
		logger.Infof("engaged: effectiveHi inflated to %s", logMark(effectiveHi))
	}

	if s.Acked.Engaged && s.Vote.VotedFor == from {
		logger.Infof(
			"engaged leader v%d re-campaigning: skipping log check", from,
		)
	} else if !leq(effectiveHi, lm) {
		// Log-up-to-date check failed.
		logger.Infof(
			"rejected vote for v%d: log not up to date (effectiveHi %s > lm %s)",
			from, logMark(effectiveHi), logMark(lm),
		)
		return State{}, false
	}

	// We grant the vote. The engagement (if any) terminates, but we carry
	// effectiveHi forward rather than resetting it: this preserves our knowledge
	// of what we may have helped commit, and keeps Hi monotonically
	// non-decreasing. We intentionally do not use the candidate's log mark as
	// the new Hi, since the candidate may not win the election, and its log mark
	// may reflect entries from higher terms that never actually commit.
	//
	// Note that carrying forward effectiveHi is safe even though we're "losing"
	// the engagement: any future engagement must be at [candTerm, X] or later,
	// which is strictly up to date with any older promises we made. It would also
	// be safe to instead "forget" the past engagement.
	next := State{
		Vote: Vote{
			Term:     candTerm,
			VotedFor: from,
		},
		Acked: Acked{Hi: effectiveHi},
	}
	logger.Infof("granted vote for v%d at term %s, hi carried as %s",
		from, fmtTerm(candTerm), logMark(effectiveHi),
	)
	return next, true
}

// maybeBumpTerm reacts to a message from a leader at a higher term by resetting
// state to that leader's term. Only called for leader-sourced messages
// (engage/release), so the leader's identity is recorded. If the term doesn't
// advance but VotedFor is unset, we record the leader — accepting an
// engage/release is effectively acknowledging the leader.
func (s State) maybeBumpTerm(logger raftlogger.Logger, term raftpb.Term, lead raftpb.PeerID) State {
	if term < s.Vote.Term {
		return s
	}
	if term == s.Vote.Term {
		if s.Vote.VotedFor == 0 {
			s.Vote.VotedFor = lead
			logger.Infof("recorded leader v%d for term %s", lead, fmtTerm(term))
		}
		return s
	}
	// Term advanced. The leader was elected without our help, so its log
	// is up to date with a traditional quorum of voters.
	logger.Infof("term advance %s -> %s (leader v%d)", fmtTerm(s.Vote.Term), fmtTerm(term), lead)
	return State{
		Vote:  Vote{Term: term, VotedFor: lead},
		Acked: Acked{},
	}
}

func (s State) HandleMsgEngage(
	logger raftlogger.Logger, from raftpb.PeerID, term raftpb.Term, lm raft.LogMark,
) (State, bool) {
	s = s.maybeBumpTerm(logger, term, from)

	if term != s.Vote.Term {
		logger.Infof("engage rejected: stale term %s < %s", fmtTerm(term), fmtTerm(s.Vote.Term))
		return State{}, false
	}

	if s.Acked.Engaged && s.Acked.Hi == lm {
		logger.Infof("engage idempotent at %s", logMark(lm))
		return s, true
	}

	if lm.Term < uint64(s.Vote.Term) {
		// Refuse to engage directly on behalf of old log terms to keep things
		// simple. See `Acked.Hi`.
		logger.Infof("engage rejected: lm term %s < vote term %s",
			fmtTerm(raftpb.Term(lm.Term)), fmtTerm(s.Vote.Term))
		return State{}, false
	}

	if !le(s.Acked.Hi, lm) {
		// Stale request - engagements need to be for a strictly larger log mark
		// than the last release due to replay protection.
		logger.Infof("engage rejected: hi %s not ahead of %s",
			logMark(lm), logMark(s.Acked.Hi))
		return State{}, false
	}

	logger.Infof("engaged at %s", logMark(lm))
	return State{
		Vote: s.Vote,
		Acked: Acked{
			Hi:      lm,
			Engaged: true,
		},
	}, true
}

func (s State) HandleMsgRelease(
	logger raftlogger.Logger, from raftpb.PeerID, term raftpb.Term, lm raft.LogMark,
) (State, bool) {
	s = s.maybeBumpTerm(logger, term, from)

	if term != s.Vote.Term {
		logger.Infof("release rejected: stale term %s < %s", fmtTerm(term), fmtTerm(s.Vote.Term))
		return State{}, false
	}

	if !leq(s.Acked.Hi, lm) {
		logger.Infof("release rejected: stale hi %s > %s",
			logMark(s.Acked.Hi), logMark(lm))
		return State{}, false
	}

	logger.Infof("released at %s", logMark(lm))
	next := State{
		Vote: s.Vote,
		Acked: Acked{
			Hi:      lm,
			Engaged: false,
		},
	}
	return next, true
}
