// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package witness

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// testVoter is a minimal model of a raft voter: an ID, a log, and hard state.
type testVoter struct {
	id       raftpb.PeerID
	log      []raft.LogMark // consecutive entries starting at index 1
	term     raftpb.Term
	votedFor raftpb.PeerID
	leader   bool
}

// lastLogMark returns the last entry in the voter's log, or the zero LogMark.
func (v testVoter) lastLogMark() raft.LogMark {
	if len(v.log) == 0 {
		return raft.LogMark{}
	}
	return v.log[len(v.log)-1]
}

// testEnv holds the witness state and any voters added for scenario modeling.
type testEnv struct {
	w      State
	voters []testVoter
}

func (e *testEnv) voter(t *testing.T, id raftpb.PeerID) *testVoter {
	for i := range e.voters {
		if e.voters[i].id == id {
			return &e.voters[i]
		}
	}
	t.Fatalf("unknown voter v%d", id)
	return nil
}

func (e *testEnv) fmtConfig() string {
	var parts []string
	for _, v := range e.voters {
		parts = append(parts, fmt.Sprintf("v%d", v.id))
	}
	parts = append(parts, "w")
	return "(" + strings.Join(parts, " ") + ")"
}

func (e *testEnv) fmtVoters() string {
	cfg := e.fmtConfig()
	var lines []string
	for _, v := range e.voters {
		lines = append(lines, fmtVoterLine(v, cfg))
	}
	return strings.Join(lines, "\n")
}

func fmtVoterLine(v testVoter, cfg string) string {
	var logParts []string
	for _, lm := range v.log {
		logParts = append(logParts, fmtLogMark(lm))
	}
	suffix := ""
	if v.leader {
		suffix = " leader"
	}
	return fmt.Sprintf(
		"v%d: [%s] term=%s cfg=%s%s",
		v.id, strings.Join(logParts, " "), fmtTerm(v.term), cfg, suffix,
	)
}

func TestDataDrivenWitness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		env := testEnv{}
		ctx := context.Background()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "add-voters":
				return handleAddVoters(t, d, &env)
			case "campaign":
				return handleCampaign(t, d, &env, ctx)
			case "append":
				return handleAppend(t, d, &env, ctx)
			case "vote":
				from, term, lm := scanVoteArgs(t, d)
				next, ok := env.w.HandleMsgVote(ctx, from, term, lm)
				return fmtResult(&env.w, next, ok)
			case "engage":
				term, lm := scanEngageReleaseArgs(t, d, &env)
				next, ok := env.w.HandleMsgEngage(ctx, term, lm)
				return fmtResult(&env.w, next, ok)
			case "release":
				term, lm := scanEngageReleaseArgs(t, d, &env)
				next, ok := env.w.HandleMsgRelease(ctx, term, lm)
				return fmtResult(&env.w, next, ok)
			case "state":
				return fmtState(env.w)
			case "set":
				handleSet(t, d, &env.w)
				return fmtState(env.w)
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
				return ""
			}
		})
	})
}

func handleAddVoters(t *testing.T, d *datadriven.TestData, env *testEnv) string {
	for _, line := range strings.Split(d.Input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		colonIdx := strings.IndexByte(line, ':')
		if colonIdx < 0 {
			t.Fatalf("invalid voter line (expected 'vN: [...]'): %s", line)
		}
		id := mustParsePeerID(t, strings.TrimSpace(line[:colonIdx]))
		logStr := strings.TrimSpace(line[colonIdx+1:])
		logStr = strings.TrimPrefix(logStr, "[")
		logStr = strings.TrimSuffix(logStr, "]")
		var log []raft.LogMark
		for _, tok := range strings.Fields(logStr) {
			lm := mustParseLogMark(t, tok)
			log = append(log, lm)
		}
		// Validate consecutive indexes starting at 1 with non-decreasing terms.
		for i, lm := range log {
			if lm.Index != uint64(i+1) {
				t.Fatalf("v%d: expected index %d, got %d (entry %s)", id, i+1, lm.Index, fmtLogMark(lm))
			}
			if i > 0 && lm.Term < log[i-1].Term {
				t.Fatalf("v%d: term decreased at index %d: %s after %s",
					id, lm.Index, fmtLogMark(lm), fmtLogMark(log[i-1]))
			}
		}
		v := testVoter{id: id, log: log}
		// Initialize term from last log entry.
		if last := v.lastLogMark(); last != (raft.LogMark{}) {
			v.term = raftpb.Term(last.Term)
		}
		env.voters = append(env.voters, v)
	}
	return env.fmtVoters()
}

// handleCampaign simulates a voter campaigning for leadership. All peers
// (voters + witness) process the vote request and update their state. Responses
// from peers in the drop set are not counted toward quorum.
func handleCampaign(
	t *testing.T, d *datadriven.TestData, env *testEnv, ctx context.Context,
) string {
	if len(d.CmdArgs) < 1 {
		t.Fatal("usage: campaign <vN> [drop=(<ids>)]")
	}
	candidateID := mustParsePeerID(t, d.CmdArgs[0].Key)
	candidate := env.voter(t, candidateID)

	// Parse drop set.
	dropped := parseDrop(d)

	// Campaign: increment term, vote for self.
	candidate.term++
	campaignTerm := candidate.term
	candidate.votedFor = candidate.id
	candidateLastLM := candidate.lastLogMark()

	totalMembers := len(env.voters) + 1 // voters + witness
	quorum := totalMembers/2 + 1
	yesCount := 0

	var buf strings.Builder
	fmt.Fprintf(&buf, "v%d campaigns at term %s\n", candidateID, fmtTerm(campaignTerm))

	// Candidate's self-vote always counts.
	fmt.Fprintf(&buf, "v%d: yes (self)\n", candidateID)
	yesCount++

	// Other voters vote.
	for i := range env.voters {
		v := &env.voters[i]
		if v.id == candidateID {
			continue
		}
		label := fmt.Sprintf("v%d", v.id)

		granted, reason := voterGrantsVote(v, candidateID, campaignTerm, candidateLastLM)

		if granted {
			if dropped[label] {
				fmt.Fprintf(&buf, "%s: yes (dropped)\n", label)
			} else {
				fmt.Fprintf(&buf, "%s: yes\n", label)
				yesCount++
			}
		} else {
			if dropped[label] {
				fmt.Fprintf(&buf, "%s: no, %s (dropped)\n", label, reason)
			} else {
				fmt.Fprintf(&buf, "%s: no, %s\n", label, reason)
			}
		}
	}

	// Witness votes.
	next, ok := env.w.HandleMsgVote(ctx, candidateID, campaignTerm, candidateLastLM)
	if ok {
		env.w = next
	}
	if ok {
		if dropped["w"] {
			fmt.Fprintf(&buf, "w:  yes (dropped)\n")
		} else {
			fmt.Fprintf(&buf, "w:  yes\n")
			yesCount++
		}
	} else {
		if dropped["w"] {
			fmt.Fprintf(&buf, "w:  no (dropped)\n")
		} else {
			fmt.Fprintf(&buf, "w:  no\n")
		}
	}

	if yesCount >= quorum {
		// Winner becomes leader and appends a noop entry at the campaign term.
		candidate.leader = true
		nextIdx := uint64(len(candidate.log) + 1)
		candidate.log = append(
			candidate.log,
			raft.LogMark{Term: uint64(campaignTerm), Index: nextIdx},
		)
		// Other voters lose leader status.
		for i := range env.voters {
			if env.voters[i].id != candidateID {
				env.voters[i].leader = false
			}
		}
		buf.WriteString("outcome: won")
	} else {
		buf.WriteString("outcome: lost")
	}
	return buf.String()
}

// handleAppend simulates a leader replicating its log to followers and
// optionally engaging the witness. Followers accept the append if their log is
// a prefix of the leader's (simplified log matching). Responses from peers in
// the drop set are not counted.
func handleAppend(t *testing.T, d *datadriven.TestData, env *testEnv, _ context.Context) string {
	if len(d.CmdArgs) < 1 {
		t.Fatal("usage: append <vN> [drop=(<ids>)]")
	}
	leaderID := mustParsePeerID(t, d.CmdArgs[0].Key)
	leader := env.voter(t, leaderID)
	if !leader.leader {
		t.Fatalf("v%d is not the leader", leaderID)
	}

	// Parse drop set.
	dropped := parseDrop(d)

	leaderLastLM := leader.lastLogMark()
	var buf strings.Builder
	fmt.Fprintf(&buf, "v%d appends through %s\n", leaderID, fmtLogMark(leaderLastLM))

	// Replicate to other voters.
	for i := range env.voters {
		v := &env.voters[i]
		if v.id == leaderID {
			continue
		}
		label := fmt.Sprintf("v%d", v.id)

		ok, reason := followerAcceptsAppend(v, leader)
		if ok {
			if dropped[label] {
				fmt.Fprintf(&buf, "%s: ok (dropped)\n", label)
			} else {
				// Recognize sender as leader: bump term, step down.
				if leader.term > v.term {
					v.term = leader.term
					v.votedFor = 0
				}
				v.leader = false
				v.log = append([]raft.LogMark{}, leader.log...)
				fmt.Fprintf(&buf, "%s: ok\n", label)
			}
		} else {
			if dropped[label] {
				fmt.Fprintf(&buf, "%s: rejected, %s (dropped)\n", label, reason)
			} else {
				fmt.Fprintf(&buf, "%s: rejected, %s\n", label, reason)
			}
		}
	}

	buf.WriteString(env.fmtVoters())
	return buf.String()
}

// followerAcceptsAppend checks whether a follower's log is compatible with the
// leader's for a simplified append. The follower's log must be a prefix of the
// leader's log (matching terms at each index).
func followerAcceptsAppend(follower *testVoter, leader *testVoter) (ok bool, reason string) {
	for i, lm := range follower.log {
		if i >= len(leader.log) {
			return false, fmt.Sprintf(
				"follower log longer (%d > %d)", len(follower.log), len(leader.log),
			)
		}
		if lm.Term != leader.log[i].Term {
			return false, fmt.Sprintf(
				"log diverges at index %d (%s vs %s)",
				i+1, fmtLogMark(lm), fmtLogMark(leader.log[i]),
			)
		}
	}
	return true, ""
}

// voterGrantsVote decides whether a voter grants a vote to the candidate.
// It updates the voter's state (term, votedFor) as a side effect.
func voterGrantsVote(
	v *testVoter, candidateID raftpb.PeerID, campaignTerm raftpb.Term, candidateLastLM raft.LogMark,
) (granted bool, reason string) {
	// Bump term if needed.
	if campaignTerm > v.term {
		v.term = campaignTerm
		v.votedFor = 0
	}

	if campaignTerm < v.term {
		return false, fmt.Sprintf("stale term %s < %s", fmtTerm(campaignTerm), fmtTerm(v.term))
	}

	// Already voted for someone else this term.
	if v.votedFor != 0 && v.votedFor != candidateID {
		return false, fmt.Sprintf("already voted for v%d", v.votedFor)
	}

	// Log up-to-date check.
	voterLastLM := v.lastLogMark()
	if voterLastLM != (raft.LogMark{}) && voterLastLM.After(candidateLastLM) {
		return false, fmt.Sprintf("log not up to date (%s > %s)",
			fmtLogMark(voterLastLM), fmtLogMark(candidateLastLM))
	}

	v.votedFor = candidateID
	return true, ""
}

// parseDrop extracts the drop=(<ids>) argument from a datadriven command.
func parseDrop(d *datadriven.TestData) map[string]bool {
	dropped := map[string]bool{}
	for _, arg := range d.CmdArgs[1:] {
		if arg.Key == "drop" {
			for _, v := range arg.Vals {
				dropped[v] = true
			}
		}
	}
	return dropped
}

// scanVoteArgs parses from=<id> term=<letter> lm=<logmark> arguments.
func scanVoteArgs(t *testing.T, d *datadriven.TestData) (raftpb.PeerID, raftpb.Term, raft.LogMark) {
	var fromStr, termStr, lmStr string
	d.ScanArgs(t, "from", &fromStr)
	d.ScanArgs(t, "term", &termStr)
	d.ScanArgs(t, "lm", &lmStr)
	return mustParsePeerID(t, fromStr), mustParseTerm(t, termStr), mustParseLogMark(t, lmStr)
}

// scanEngageReleaseArgs accepts either `v<N>` (derive term and lm from the
// leader's state) or explicit `term=<letter> lm=<logmark>` arguments.
func scanEngageReleaseArgs(
	t *testing.T, d *datadriven.TestData, env *testEnv,
) (raftpb.Term, raft.LogMark) {
	if len(d.CmdArgs) > 0 && strings.HasPrefix(d.CmdArgs[0].Key, "v") {
		id := mustParsePeerID(t, d.CmdArgs[0].Key)
		v := env.voter(t, id)
		if !v.leader {
			t.Fatalf("v%d is not the leader", id)
		}
		return v.term, v.lastLogMark()
	}
	var termStr, lmStr string
	d.ScanArgs(t, "term", &termStr)
	d.ScanArgs(t, "lm", &lmStr)
	return mustParseTerm(t, termStr), mustParseLogMark(t, lmStr)
}

func handleSet(t *testing.T, d *datadriven.TestData, w *State) {
	*w = State{}
	for _, tok := range strings.Fields(d.Input) {
		k, v, ok := strings.Cut(tok, "=")
		if !ok {
			t.Fatalf("invalid set token: %s", tok)
		}
		switch k {
		case "term":
			w.Vote.Term = mustParseTerm(t, v)
		case "voted-for":
			w.Vote.VotedFor = mustParsePeerID(t, v)
		case "hi":
			w.Acked.Hi = mustParseLogMark(t, v)
		case "engaged":
			w.Acked.Engaged = v == "true"
		default:
			t.Fatalf("unknown set field: %s", k)
		}
	}
}

func fmtResult(w *State, next State, ok bool) string {
	if ok {
		*w = next
		return "ok\n" + fmtState(*w)
	}
	return "rejected"
}

func fmtState(s State) string {
	var buf strings.Builder
	if s.Vote == (Vote{}) {
		buf.WriteString("vote:  none\n")
	} else if s.Vote.VotedFor == 0 {
		fmt.Fprintf(&buf, "vote:  (%s, -)\n", fmtTerm(s.Vote.Term))
	} else {
		fmt.Fprintf(&buf, "vote:  (%s, v%d)\n", fmtTerm(s.Vote.Term), s.Vote.VotedFor)
	}
	if s.Acked == (Acked{}) {
		buf.WriteString("acked: none")
	} else if s.Acked.Engaged {
		fmt.Fprintf(&buf, "acked: (%s, engaged)", fmtLogMark(s.Acked.Hi))
	} else {
		fmt.Fprintf(&buf, "acked: (%s, released)", fmtLogMark(s.Acked.Hi))
	}
	return buf.String()
}

// mustParsePeerID converts "v<N>" to a raftpb.PeerID.
func mustParsePeerID(t *testing.T, s string) raftpb.PeerID {
	if len(s) < 2 || s[0] != 'v' {
		t.Fatalf("invalid peer ID %q: expected v<N>", s)
	}
	id, err := strconv.ParseUint(s[1:], 10, 64)
	if err != nil {
		t.Fatalf("invalid peer ID %q: %v", s, err)
	}
	return raftpb.PeerID(id)
}

// mustParseTerm converts a single letter (a-z) to a raftpb.Term (1-26).
func mustParseTerm(t *testing.T, s string) raftpb.Term {
	if len(s) != 1 || s[0] < 'a' || s[0] > 'z' {
		t.Fatalf("invalid term %q: must be a single letter a-z", s)
	}
	return raftpb.Term(s[0] - 'a' + 1)
}

// mustParseLogMark converts "[a-z][0-9]+" to a raft.LogMark, or "0" to the
// zero value.
func mustParseLogMark(t *testing.T, s string) raft.LogMark {
	if s == "0" {
		return raft.LogMark{}
	}
	if len(s) < 2 || s[0] < 'a' || s[0] > 'z' {
		t.Fatalf("invalid log mark %q: expected [a-z][0-9]+", s)
	}
	idx, err := strconv.ParseUint(s[1:], 10, 64)
	if err != nil {
		t.Fatalf("invalid log mark %q: %v", s, err)
	}
	return raft.LogMark{Term: uint64(s[0]-'a') + 1, Index: idx}
}

func fmtTerm(term raftpb.Term) string {
	if term >= 1 && term <= 26 {
		return string(rune('a' + byte(term-1)))
	}
	return fmt.Sprintf("%d", term)
}

func fmtLogMark(lm raft.LogMark) string {
	if lm == (raft.LogMark{}) {
		return "0"
	}
	if lm.Term >= 1 && lm.Term <= 26 {
		return fmt.Sprintf("%c%d", 'a'+byte(lm.Term-1), lm.Index)
	}
	return fmt.Sprintf("t%di%d", lm.Term, lm.Index)
}
