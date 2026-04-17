// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package witness

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// testVoter is a minimal model of a raft voter: an ID, a log, and hard state.
type testVoter struct {
	id             raftpb.PeerID
	log            []raft.LogMark // consecutive entries starting at index 1
	term           raftpb.Term
	votedFor       raftpb.PeerID
	leader         bool
	committed      uint64
	witnessEngaged bool   // leader's view of witness engagement
	cfg            string // e.g. "(v1 v2 w)"
	down           bool   // true when the replica is unreachable
	// matchIndex is only meaningful when leader==true. It tracks what this
	// leader believes each peer's match index to be. Reset on election.
	matchIndex map[raftpb.PeerID]uint64
}

// testWitness wraps the witness State with per-replica test metadata.
type testWitness struct {
	State
	down bool
}

// testMsg carries a single message between replicas.
type testMsg struct {
	from, to string
	msg      any
}

// testExchange pairs a request with its response, along with any log output
// produced by the witness state machine during message handling.
type testExchange struct {
	req, resp testMsg
	trace     []string // log lines from the witness, if any
}

// Message payloads. Each type corresponds to a specific message kind; deliver
// routes based on the concrete type.
type (
	msgVoteReq struct {
		candidate raftpb.PeerID
		term      raftpb.Term
		lastLog   raft.LogMark
		prevote   bool
	}
	msgVoteResp struct {
		granted bool
		reason  string
	}
	msgAppendReq struct {
		leader testVoter
	}
	msgAppendResp struct {
		ok     bool
		detail string
	}
	msgEngage struct {
		term raftpb.Term
		hi   raft.LogMark
	}
	msgRelease struct {
		term raftpb.Term
		hi   raft.LogMark
	}
	msgWitnessResp struct {
		ok bool
	}
	msgDropped struct{}
)

// lastLogMark returns the last entry in the voter's log, or the zero LogMark.
func (v testVoter) lastLogMark() raft.LogMark {
	if len(v.log) == 0 {
		return raft.LogMark{}
	}
	return v.log[len(v.log)-1]
}

// testLogger is a simple logger that captures output without level prefixes.
type testLogger struct {
	strings.Builder
}

var _ raftlogger.Logger = (*testLogger)(nil)

func (l *testLogger) logf(format string, v ...interface{}) {
	fmt.Fprintf(&l.Builder, format, v...)
	if n := len(format); n > 0 && format[n-1] != '\n' {
		l.Builder.WriteByte('\n')
	}
}

func (l *testLogger) Debug(v ...interface{})                 { fmt.Fprintln(&l.Builder, v...) }
func (l *testLogger) Debugf(format string, v ...interface{}) { l.logf(format, v...) }
func (l *testLogger) Info(v ...interface{})                  { fmt.Fprintln(&l.Builder, v...) }
func (l *testLogger) Infof(format string, v ...interface{})  { l.logf(format, v...) }
func (l *testLogger) Warning(v ...interface{})               { fmt.Fprintln(&l.Builder, v...) }
func (l *testLogger) Warningf(format string, v ...interface{}) {
	l.logf(format, v...)
}
func (l *testLogger) Error(v ...interface{})                 { fmt.Fprintln(&l.Builder, v...) }
func (l *testLogger) Errorf(format string, v ...interface{}) { l.logf(format, v...) }
func (l *testLogger) Fatal(v ...interface{})                 { panic(fmt.Sprint(v...)) }
func (l *testLogger) Fatalf(format string, v ...interface{}) { panic(fmt.Sprintf(format, v...)) }
func (l *testLogger) Panic(v ...interface{})                 { panic(fmt.Sprint(v...)) }
func (l *testLogger) Panicf(format string, v ...interface{}) { panic(fmt.Sprintf(format, v...)) }

// testEnv holds the witness state and any voters added for scenario modeling.
type testEnv struct {
	w             testWitness
	hasWitness    bool
	voters        []testVoter
	lastExchanges []testExchange // populated by deliver; reset each command
	logger        testLogger
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

func (e *testEnv) leader() *testVoter {
	for i := range e.voters {
		if e.voters[i].leader {
			return &e.voters[i]
		}
	}
	return nil
}

// updateLeaderCommitted recomputes the leader's committed index based on
// match indices and witness engagement.
func (e *testEnv) updateLeaderCommitted() {
	ldr := e.leader()
	if ldr == nil {
		return
	}
	var matches []uint64
	for _, v := range e.voters {
		matches = append(matches, ldr.matchIndex[v.id])
	}
	// Witness: if the leader believes it's engaged, it matches everything.
	if e.hasWitness {
		if ldr.witnessEngaged {
			matches = append(matches, uint64(len(ldr.log)))
		} else {
			matches = append(matches, 0)
		}
	}
	slices.Sort(matches)
	quorum := len(matches)/2 + 1
	quorumMatch := matches[len(matches)-quorum]
	// Only commit entries from the current term (raft safety).
	if quorumMatch > 0 &&
		ldr.log[quorumMatch-1].Term == uint64(ldr.term) &&
		quorumMatch > ldr.committed {
		ldr.committed = quorumMatch
	}
}

func (e *testEnv) fmtConfig() string {
	var parts []string
	for _, v := range e.voters {
		parts = append(parts, fmt.Sprintf("v%d", v.id))
	}
	if e.hasWitness {
		parts = append(parts, "w")
	}
	return "(" + strings.Join(parts, " ") + ")"
}

func (e *testEnv) fmtVoters() string {
	// Elide cfg= when all voters agree on the config.
	showCfg := false
	for _, v := range e.voters[1:] {
		if v.cfg != e.voters[0].cfg {
			showCfg = true
			break
		}
	}
	var lines []string
	for _, v := range e.voters {
		cfg := ""
		if showCfg {
			cfg = v.cfg
		}
		lines = append(lines, fmtVoterLine(v, cfg))
	}
	return strings.Join(lines, "\n")
}

func fmtVoterLine(v testVoter, cfg string) string {
	var logParts []string
	for _, lm := range v.log {
		logParts = append(logParts, logMark(lm).String())
	}
	suffix := ""
	if v.leader {
		if v.committed > 0 {
			suffix = fmt.Sprintf(" committed=%s leader", logMark(v.log[v.committed-1]))
		} else {
			suffix = " leader"
		}
	}
	cfgStr := ""
	if cfg != "" {
		cfgStr = " cfg=" + cfg
	}
	return fmt.Sprintf(
		"v%d: [%s] term=%s%s%s",
		v.id, strings.Join(logParts, " "), fmtTerm(v.term), cfgStr, suffix,
	)
}

func (e *testEnv) voterByLabel(label string) *testVoter {
	for i := range e.voters {
		if fmt.Sprintf("v%d", e.voters[i].id) == label {
			return &e.voters[i]
		}
	}
	return nil
}

// deliver routes outbound messages to their recipients, collecting exchanges.
// Messages to downed or dropped peers produce msgDropped responses.
func (e *testEnv) deliver(outbound []testMsg, dropped map[string]bool) {
	e.lastExchanges = e.lastExchanges[:0]
	for _, m := range outbound {
		var resp testMsg
		var trace []string
		if dropped[m.to] || e.isDown(m.to) {
			resp = testMsg{from: m.to, to: m.from, msg: msgDropped{}}
		} else {
			resp, trace = e.route(m)
		}
		e.lastExchanges = append(e.lastExchanges, testExchange{
			req: m, resp: resp, trace: trace,
		})
	}
}

func (e *testEnv) isDown(label string) bool {
	if label == "w" {
		return e.w.down
	}
	if v := e.voterByLabel(label); v != nil {
		return v.down
	}
	return false
}

// route dispatches a single message to its recipient and returns the response
// along with any trace lines produced by the witness logger.
func (e *testEnv) route(m testMsg) (testMsg, []string) {
	switch req := m.msg.(type) {
	case msgVoteReq:
		if m.to == "w" {
			// A pre-vote is modeled as a vote at req.term (already candidate.term+1)
			// but without mutating anyone's state. This mirrors raft.
			e.logger.Reset()
			next, ok := e.w.State.HandleMsgVote(
				&e.logger, req.candidate, req.term, req.lastLog,
			)
			trace := captureTrace(&e.logger)
			if ok && !req.prevote {
				e.w.State = next
			}
			return testMsg{from: "w", to: m.from, msg: msgVoteResp{granted: ok}}, trace
		}
		v := e.voterByLabel(m.to)
		voter := *v
		// req.term is already candidate.term+1 for prevotes, so handleVoteReq
		// naturally takes the term-advance path when appropriate.
		e.logger.Reset()
		next, granted, reason := voter.handleVoteReq(
			&e.logger, req.candidate, req.term, req.lastLog,
		)
		trace := captureTrace(&e.logger)
		if !req.prevote {
			*v = next
		}
		return testMsg{
			from: m.to, to: m.from, msg: msgVoteResp{granted: granted, reason: reason},
		}, trace

	case msgAppendReq:
		v := e.voterByLabel(m.to)
		e.logger.Reset()
		next, ok, detail := v.handleAppendReq(&e.logger, req.leader)
		trace := captureTrace(&e.logger)
		*v = next
		return testMsg{
			from: m.to, to: m.from, msg: msgAppendResp{ok: ok, detail: detail},
		}, trace

	case msgEngage:
		from := parsePeerID(m.from)
		e.logger.Reset()
		next, ok := e.w.State.HandleMsgEngage(&e.logger, from, req.term, req.hi)
		trace := captureTrace(&e.logger)
		if ok {
			e.w.State = next
		}
		return testMsg{from: "w", to: m.from, msg: msgWitnessResp{ok: ok}}, trace

	case msgRelease:
		from := parsePeerID(m.from)
		e.logger.Reset()
		next, ok := e.w.State.HandleMsgRelease(&e.logger, from, req.term, req.hi)
		trace := captureTrace(&e.logger)
		if ok {
			e.w.State = next
		}
		return testMsg{from: "w", to: m.from, msg: msgWitnessResp{ok: ok}}, trace

	default:
		panic(fmt.Sprintf("unknown message type: %T", m.msg))
	}
}

// captureTrace extracts non-empty lines from the logger output.
func captureTrace(logger *testLogger) []string {
	s := logger.String()
	if s == "" {
		return nil
	}
	var lines []string
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestDataDrivenWitness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		env := testEnv{}
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return runCommand(t, d, &env)
		})
	})
}

// runCommand dispatches a single datadriven command and returns the text output.
func runCommand(t *testing.T, d *datadriven.TestData, env *testEnv) string {
	env.lastExchanges = nil
	switch d.Cmd {
	case "setup":
		return handleSetup(t, d, env)
	case "campaign":
		return handleCampaign(t, d, env, false /* prevote */)
	case "prevote":
		return handleCampaign(t, d, env, true /* prevote */)
	case "propose":
		return handlePropose(t, d, env)
	case "append":
		return handleAppend(t, d, env)
	case "vote":
		return handleVote(t, d, env)
	case "engage":
		return handleEngage(t, d, env)
	case "release":
		return handleRelease(t, d, env)
	case "state":
		return fmtWitness(env.w.State)
	case "set":
		handleSet(t, d, &env.w.State)
		return fmtWitness(env.w.State)
	case "set-avail":
		return handleSetAvail(t, d, env)
	case "set-term":
		return handleSetTerm(t, d, env)
	default:
		t.Fatalf("unknown command: %s", d.Cmd)
		return ""
	}
}

func handleSetup(t *testing.T, d *datadriven.TestData, env *testEnv) string {
	for _, line := range strings.Split(d.Input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// "w" on its own line adds the witness.
		if line == "w" {
			env.hasWitness = true
			continue
		}
		colonIdx := strings.IndexByte(line, ':')
		if colonIdx < 0 {
			t.Fatalf("invalid voter line (expected 'vN: [...]' or 'w'): %s", line)
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
				t.Fatalf("v%d: expected index %d, got %d (entry %s)", id, i+1, lm.Index, logMark(lm))
			}
			if i > 0 && lm.Term < log[i-1].Term {
				t.Fatalf("v%d: term decreased at index %d: %s after %s",
					id, lm.Index, logMark(lm), logMark(log[i-1]))
			}
		}
		v := testVoter{id: id, log: log}
		// Initialize term from last log entry.
		if last := v.lastLogMark(); last != (raft.LogMark{}) {
			v.term = raftpb.Term(last.Term)
		}
		env.voters = append(env.voters, v)
	}
	// Set the config on all voters now that the full membership is known.
	cfg := env.fmtConfig()
	for i := range env.voters {
		env.voters[i].cfg = cfg
	}
	return env.fmtVoters()
}

// handleCampaign simulates a voter campaigning for leadership. The candidate
// sends vote requests to all peers via the delivery layer. When prevote is
// true, no state is mutated — it's a speculative check.
func handleCampaign(t *testing.T, d *datadriven.TestData, env *testEnv, prevote bool) string {
	if len(d.CmdArgs) < 1 {
		t.Fatal("usage: campaign/prevote <vN> [drop=(<ids>)]")
	}
	candidateID := mustParsePeerID(t, d.CmdArgs[0].Key)
	candidate := env.voter(t, candidateID)
	dropped := parseDrop(d)

	// Campaign: increment term, vote for self. Prevote: use term+1 without
	// actually bumping.
	var campaignTerm raftpb.Term
	if prevote {
		campaignTerm = candidate.term + 1
	} else {
		candidate.term++
		campaignTerm = candidate.term
		candidate.votedFor = candidate.id
	}
	candidateLastLM := candidate.lastLogMark()

	// Build outbound vote requests to all peers.
	from := fmt.Sprintf("v%d", candidateID)
	var outbound []testMsg
	for _, v := range env.voters {
		if v.id == candidateID {
			continue
		}
		outbound = append(outbound, testMsg{
			from: from,
			to:   fmt.Sprintf("v%d", v.id),
			msg: msgVoteReq{
				candidate: candidateID, term: campaignTerm,
				lastLog: candidateLastLM, prevote: prevote,
			},
		})
	}
	if env.hasWitness {
		outbound = append(outbound, testMsg{
			from: from, to: "w",
			msg: msgVoteReq{
				candidate: candidateID, term: campaignTerm,
				lastLog: candidateLastLM, prevote: prevote,
			},
		})
	}

	env.deliver(outbound, dropped)

	// Format output from exchanges.
	verb := "campaigns"
	if prevote {
		verb = "prevotes"
	}
	var buf strings.Builder
	fmt.Fprintf(&buf, "v%d %s at term %s\n", candidateID, verb, fmtTerm(campaignTerm))
	fmt.Fprintf(&buf, "v%d: yes (self)\n", candidateID)

	totalMembers := len(env.voters)
	if env.hasWitness {
		totalMembers++
	}
	quorum := totalMembers/2 + 1
	yesCount := 1 // self

	for _, ex := range env.lastExchanges {
		label := ex.resp.from
		switch resp := ex.resp.msg.(type) {
		case msgDropped:
			fmt.Fprintf(&buf, "%s: (dropped)\n", label)
		case msgVoteResp:
			var verdict string
			if resp.granted {
				if prevote {
					verdict = "yes (prevote, no state change)"
				} else {
					verdict = "yes"
				}
				yesCount++
			} else if resp.reason != "" {
				verdict = "no, " + resp.reason
			} else {
				verdict = "no"
			}
			fmtExchangeResult(&buf, label, verdict, ex.trace)
		}
	}

	if yesCount >= quorum {
		if !prevote {
			// Winner becomes leader and appends a noop entry at the campaign term.
			// Other voters already stepped down via handleVoteReq (term bump sets
			// leader=false). Dropped voters retain their stale leader belief.
			candidate.leader = true
			candidate.witnessEngaged = false
			nextIdx := uint64(len(candidate.log) + 1)
			candidate.log = append(
				candidate.log,
				raft.LogMark{Term: uint64(campaignTerm), Index: nextIdx},
			)
			candidate.matchIndex = map[raftpb.PeerID]uint64{
				candidateID: uint64(len(candidate.log)),
			}
		}
		buf.WriteString("outcome: won")
	} else {
		buf.WriteString("outcome: lost")
	}
	return buf.String()
}

// handlePropose adds a new entry to the leader's log at the current term.
func handlePropose(t *testing.T, d *datadriven.TestData, env *testEnv) string {
	if len(d.CmdArgs) < 1 {
		t.Fatal("usage: propose <vN>")
	}
	id := mustParsePeerID(t, d.CmdArgs[0].Key)
	v := env.voter(t, id)
	if !v.leader {
		t.Fatalf("v%d is not the leader", id)
	}
	nextIdx := uint64(len(v.log) + 1)
	v.log = append(v.log, raft.LogMark{Term: uint64(v.term), Index: nextIdx})
	v.matchIndex[id] = uint64(len(v.log))
	env.updateLeaderCommitted()
	return env.fmtVoters()
}

// handleAppend simulates a leader replicating its log to followers via the
// delivery layer.
func handleAppend(t *testing.T, d *datadriven.TestData, env *testEnv) string {
	if len(d.CmdArgs) < 1 {
		t.Fatal("usage: append <vN> [drop=(<ids>)]")
	}
	leaderID := mustParsePeerID(t, d.CmdArgs[0].Key)
	leader := env.voter(t, leaderID)
	if !leader.leader {
		t.Fatalf("v%d is not the leader", leaderID)
	}
	dropped := parseDrop(d)

	from := fmt.Sprintf("v%d", leaderID)
	var outbound []testMsg
	for _, v := range env.voters {
		if v.id == leaderID {
			continue
		}
		outbound = append(outbound, testMsg{
			from: from,
			to:   fmt.Sprintf("v%d", v.id),
			msg:  msgAppendReq{leader: *leader},
		})
	}

	env.deliver(outbound, dropped)

	leaderLastLM := leader.lastLogMark()
	var buf strings.Builder
	fmt.Fprintf(&buf, "v%d appends through %s\n", leaderID, logMark(leaderLastLM))

	for _, ex := range env.lastExchanges {
		label := ex.resp.from
		switch resp := ex.resp.msg.(type) {
		case msgDropped:
			fmt.Fprintf(&buf, "%s: (dropped)\n", label)
		case msgAppendResp:
			fmtExchangeResult(&buf, label, resp.detail, ex.trace)
			if resp.ok {
				v := env.voterByLabel(label)
				leader.matchIndex[v.id] = uint64(len(leader.log))
			}
		}
	}

	env.updateLeaderCommitted()
	buf.WriteString(env.fmtVoters())
	return buf.String()
}

// handleSetAvail sets availability for replicas.
// Usage: set-avail up=(v1,w) down=(v2)
// handleSetTerm bumps a voter's term. The term must not regress. If the term
// advances, votedFor is reset (the voter hasn't voted in the new term).
//
// Syntax: set-term <voter> <term>
func handleSetTerm(t *testing.T, d *datadriven.TestData, env *testEnv) string {
	if len(d.CmdArgs) != 2 {
		t.Fatalf("set-term: expected 2 args (voter, term), got %d", len(d.CmdArgs))
	}
	v := env.voterByLabel(d.CmdArgs[0].Key)
	newTerm := mustParseTerm(t, d.CmdArgs[1].Key)
	if newTerm < v.term {
		t.Fatalf("set-term: term %s < current term %s (regression not allowed)",
			fmtTerm(newTerm), fmtTerm(v.term))
	}
	if newTerm > v.term {
		v.term = newTerm
		v.votedFor = 0
		v.leader = false
	}
	return "ok"
}

func handleSetAvail(t *testing.T, d *datadriven.TestData, env *testEnv) string {
	for _, arg := range d.CmdArgs {
		var down bool
		switch arg.Key {
		case "up":
			down = false
		case "down":
			down = true
		default:
			t.Fatalf("set-avail: unknown key %q, expected up or down", arg.Key)
		}
		for _, id := range arg.Vals {
			if id == "w" {
				if !env.hasWitness {
					t.Fatal("no witness configured")
				}
				env.w.down = down
			} else {
				v := env.voter(t, mustParsePeerID(t, id))
				v.down = down
			}
		}
	}
	return "ok"
}

// handleVoteReq processes a vote request and returns the voter's updated state.
// The caller decides whether to apply the result: skip for prevotes (speculative
// check) and for dropped peers (never received the message). This mirrors the
// witness's HandleMsgVote pattern.
func (v testVoter) handleVoteReq(
	logger raftlogger.Logger,
	candidateID raftpb.PeerID,
	campaignTerm raftpb.Term,
	candidateLastLM raft.LogMark,
) (next testVoter, granted bool, reason string) {
	// Bump term if needed; step down if we were leader.
	if campaignTerm > v.term {
		logger.Infof("term advance %s -> %s", fmtTerm(v.term), fmtTerm(campaignTerm))
		wasLeader := v.leader
		v.term = campaignTerm
		v.votedFor = 0
		if wasLeader {
			v.leader = false
			logger.Infof("stepped down as leader")
		}
	}
	if campaignTerm < v.term {
		reason = fmt.Sprintf("stale term %s < %s", fmtTerm(campaignTerm), fmtTerm(v.term))
		logger.Infof("rejected vote for v%d: %s", candidateID, reason)
		return v, false, reason
	}
	// Already voted for someone else this term.
	if v.votedFor != 0 && v.votedFor != candidateID {
		reason = fmt.Sprintf("already voted for v%d", v.votedFor)
		logger.Infof("rejected vote for v%d: %s", candidateID, reason)
		return v, false, reason
	}
	// Log up-to-date check.
	voterLastLM := v.lastLogMark()
	if voterLastLM != (raft.LogMark{}) && voterLastLM.After(candidateLastLM) {
		reason = fmt.Sprintf("log not up to date (%s > %s)",
			logMark(voterLastLM), logMark(candidateLastLM))
		logger.Infof("rejected vote for v%d: %s", candidateID, reason)
		return v, false, reason
	}
	v.votedFor = candidateID
	logger.Infof("granted vote for v%d at term %s", candidateID, fmtTerm(campaignTerm))
	return v, true, ""
}

// handleAppendReq processes an append from a leader and returns the voter's
// updated state. On success the voter adopts the leader's log, truncating any
// divergent suffix. The caller applies the result and updates env-level state
// (match indices, committed).
func (v testVoter) handleAppendReq(
	logger raftlogger.Logger, leader testVoter,
) (next testVoter, ok bool, detail string) {
	if leader.term < v.term {
		detail = fmt.Sprintf("rejected, stale leader term %s < %s",
			fmtTerm(leader.term), fmtTerm(v.term))
		logger.Infof("%s", detail)
		return v, false, detail
	}
	if leader.term > v.term {
		logger.Infof("term advance %s -> %s (leader v%d)",
			fmtTerm(v.term), fmtTerm(leader.term), leader.id)
		v.term = leader.term
		v.votedFor = 0
	}
	if v.leader {
		v.leader = false
		logger.Infof("stepped down as leader")
	}
	// Find common prefix, then truncate and adopt leader's log. Clone first
	// to avoid sharing the backing array with the caller's copy.
	common := 0
	for common < len(v.log) && common < len(leader.log) {
		if v.log[common].Term != leader.log[common].Term {
			break
		}
		common++
	}
	replaced := len(v.log) - common
	v.log = slices.Clone(v.log[:common])
	v.log = append(v.log, leader.log[common:]...)
	if replaced > 0 {
		detail = fmt.Sprintf("ok, replaced %d entries", replaced)
	} else {
		detail = "ok"
	}
	logger.Infof("accepted append through %s (%s)", logMark(leader.lastLogMark()), detail)
	return v, true, detail
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

// handleVote sends a single vote request to the witness.
func handleVote(t *testing.T, d *datadriven.TestData, env *testEnv) string {
	var fromStr, termStr, lmStr string
	d.ScanArgs(t, "from", &fromStr)
	d.ScanArgs(t, "term", &termStr)
	d.ScanArgs(t, "lm", &lmStr)
	from := mustParsePeerID(t, fromStr)
	term := mustParseTerm(t, termStr)
	lm := mustParseLogMark(t, lmStr)

	fromLabel := fmt.Sprintf("v%d", from)
	env.deliver([]testMsg{{
		from: fromLabel, to: "w",
		msg: msgVoteReq{candidate: from, term: term, lastLog: lm},
	}}, nil)
	ex := env.lastExchanges[0]
	resp := ex.resp.msg.(msgVoteResp)
	var buf strings.Builder
	if resp.granted {
		fmtExchangeResult(&buf, "w", "yes", ex.trace)
	} else {
		fmtExchangeResult(&buf, "w", "no", ex.trace)
	}
	buf.WriteString(fmtWitness(env.w.State))
	return buf.String()
}

// handleEngage sends an engage request to the witness. Accepts either `v<N>`
// (derive term and lm from the leader) or explicit `term=<letter> lm=<logmark>`.
func handleEngage(t *testing.T, d *datadriven.TestData, env *testEnv) string {
	ldr, fromLabel, term, hi := scanWitnessReqArgs(t, d, env)
	env.deliver([]testMsg{{
		from: fromLabel, to: "w",
		msg: msgEngage{term: term, hi: hi},
	}}, nil)
	ex := env.lastExchanges[0]
	resp := ex.resp.msg.(msgWitnessResp)
	var buf strings.Builder
	if resp.ok {
		fmtExchangeResult(&buf, "w", "ok", ex.trace)
		buf.WriteString(fmtWitness(env.w.State))
		if ldr != nil {
			ldr.witnessEngaged = true
			env.updateLeaderCommitted()
			buf.WriteString("\n" + fmtVoterLine(*ldr, ""))
		}
	} else {
		fmtExchangeResult(&buf, "w", "rejected", ex.trace)
	}
	return buf.String()
}

// handleRelease sends a release request to the witness.
func handleRelease(t *testing.T, d *datadriven.TestData, env *testEnv) string {
	ldr, fromLabel, term, hi := scanWitnessReqArgs(t, d, env)
	env.deliver([]testMsg{{
		from: fromLabel, to: "w",
		msg: msgRelease{term: term, hi: hi},
	}}, nil)
	ex := env.lastExchanges[0]
	resp := ex.resp.msg.(msgWitnessResp)
	var buf strings.Builder
	if resp.ok {
		fmtExchangeResult(&buf, "w", "ok", ex.trace)
		buf.WriteString(fmtWitness(env.w.State))
		if ldr != nil {
			ldr.witnessEngaged = false
		}
	} else {
		fmtExchangeResult(&buf, "w", "rejected", ex.trace)
	}
	return buf.String()
}

// scanWitnessReqArgs accepts either `v<N>` (derive term and lm from the
// leader's state) or explicit `from=<vN> term=<letter> lm=<logmark>` arguments.
func scanWitnessReqArgs(
	t *testing.T, d *datadriven.TestData, env *testEnv,
) (ldr *testVoter, fromLabel string, term raftpb.Term, hi raft.LogMark) {
	if len(d.CmdArgs) > 0 && strings.HasPrefix(d.CmdArgs[0].Key, "v") {
		id := mustParsePeerID(t, d.CmdArgs[0].Key)
		v := env.voter(t, id)
		if !v.leader {
			t.Fatalf("v%d is not the leader", id)
		}
		return v, fmt.Sprintf("v%d", id), v.term, v.lastLogMark()
	}
	var fromStr, termStr, lmStr string
	d.ScanArgs(t, "from", &fromStr)
	d.ScanArgs(t, "term", &termStr)
	d.ScanArgs(t, "lm", &lmStr)
	return nil, fromStr, mustParseTerm(t, termStr), mustParseLogMark(t, lmStr)
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

// fmtExchangeResult formats a response line. If trace is non-empty (witness
// interactions), a multi-line block is emitted with indented trace lines:
//
//	w:
//	  - trace line 1
//	  yes
//
// Otherwise a single line: "v2: yes"
func fmtExchangeResult(buf *strings.Builder, label, verdict string, trace []string) {
	if len(trace) == 0 {
		fmt.Fprintf(buf, "%s: %s\n", label, verdict)
		return
	}
	fmt.Fprintf(buf, "%s:\n", label)
	for _, line := range trace {
		fmt.Fprintf(buf, "  - %s\n", line)
	}
	fmt.Fprintf(buf, "  %s\n", verdict)
}

func fmtWitness(s State) string {
	var vote, acked string
	if s.Vote == (Vote{}) {
		vote = "none"
	} else if s.Vote.VotedFor == 0 {
		vote = fmt.Sprintf("(%s, -)", fmtTerm(s.Vote.Term))
	} else {
		vote = fmt.Sprintf("(%s, v%d)", fmtTerm(s.Vote.Term), s.Vote.VotedFor)
	}
	if s.Acked == (Acked{}) {
		acked = "none"
	} else if s.Acked.Engaged {
		acked = fmt.Sprintf("(%s, engaged)", logMark(s.Acked.Hi))
	} else {
		acked = fmt.Sprintf("(%s, released)", logMark(s.Acked.Hi))
	}
	return fmt.Sprintf("w:  vote=%s acked=%s", vote, acked)
}

// parsePeerID converts "v<N>" to a raftpb.PeerID. Panics on invalid input.
func parsePeerID(s string) raftpb.PeerID {
	if len(s) < 2 || s[0] != 'v' {
		panic(fmt.Sprintf("invalid peer ID %q: expected v<N>", s))
	}
	id, err := strconv.ParseUint(s[1:], 10, 64)
	if err != nil {
		panic(fmt.Sprintf("invalid peer ID %q: %v", s, err))
	}
	return raftpb.PeerID(id)
}

// mustParsePeerID converts "v<N>" to a raftpb.PeerID.
func mustParsePeerID(t *testing.T, s string) raftpb.PeerID {
	t.Helper()
	return parsePeerID(s)
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
