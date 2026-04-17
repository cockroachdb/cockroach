// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package witness

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sniffarg"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

//go:embed viz_template.html
var vizHTMLTemplate string

// ---------------------------------------------------------------------------
// JSON schema types
// ---------------------------------------------------------------------------

type vizTrace struct {
	TestFile string    `json:"testFile"`
	Steps    []vizStep `json:"steps"`
}

type vizStep struct {
	Index   int    `json:"index"`
	Command string `json:"command"`
	Input   string `json:"input,omitempty"`
	Comment string `json:"comment,omitempty"`
	Output  string `json:"output"`

	Voters   map[string]*vizVoterDelta `json:"voters,omitempty"`
	Witness  *vizWitnessDelta          `json:"witness,omitempty"`
	Messages []vizMessage              `json:"messages,omitempty"`
}

type vizMessage struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Type    string `json:"type"`
	Label   string `json:"label"`
	Dropped bool   `json:"dropped,omitempty"`
}

type vizVoterDelta struct {
	Log            *[]vizLogMark `json:"log,omitempty"`
	Term           *string       `json:"term,omitempty"`
	VotedFor       *string       `json:"votedFor,omitempty"`
	Leader         *bool         `json:"leader,omitempty"`
	Committed      *uint64       `json:"committed,omitempty"`
	WitnessEngaged *bool         `json:"witnessEngaged,omitempty"`
	Cfg            *string       `json:"cfg,omitempty"`
	MatchIndex     *uint64       `json:"matchIndex,omitempty"`
	Down           *bool         `json:"down,omitempty"`
}

type vizLogMark struct {
	Term  string `json:"term"`
	Index string `json:"index"`
}

type vizWitnessDelta struct {
	Vote  *vizVoteDelta  `json:"vote,omitempty"`
	Acked *vizAckedDelta `json:"acked,omitempty"`
	Down  *bool          `json:"down,omitempty"`
}

type vizVoteDelta struct {
	Term     *string `json:"term,omitempty"`
	VotedFor *string `json:"votedFor,omitempty"`
}

type vizAckedDelta struct {
	Hi      *vizLogMark `json:"hi,omitempty"`
	Engaged *bool       `json:"engaged,omitempty"`
}

// ---------------------------------------------------------------------------
// Snapshot and diff helpers
// ---------------------------------------------------------------------------

func fmtVizIndex(idx uint64) string {
	if idx == math.MaxUint64 {
		return "∞"
	}
	return strconv.FormatUint(idx, 10)
}

func toVizLog(log []raft.LogMark) []vizLogMark {
	out := make([]vizLogMark, len(log))
	for i, lm := range log {
		out[i] = vizLogMark{Term: fmtTerm(raftpb.Term(lm.Term)), Index: fmtVizIndex(lm.Index)}
	}
	return out
}

func toVizLogMark(lm raft.LogMark) *vizLogMark {
	if lm == (raft.LogMark{}) {
		return nil
	}
	return &vizLogMark{Term: fmtTerm(raftpb.Term(lm.Term)), Index: fmtVizIndex(lm.Index)}
}

func vizLogEqual(a, b []vizLogMark) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func fmtVotedFor(id raftpb.PeerID) string {
	if id == 0 {
		return "-"
	}
	return fmt.Sprintf("v%d", id)
}

func ptr[T any](v T) *T { return &v }

// snapshotVoter returns a full delta (every field set) for a voter.
func snapshotVoter(v testVoter) *vizVoterDelta {
	log := toVizLog(v.log)
	term := fmtTerm(v.term)
	votedFor := fmtVotedFor(v.votedFor)
	var mi uint64
	if v.matchIndex != nil {
		mi = v.matchIndex[v.id]
	}
	return &vizVoterDelta{
		Log:            &log,
		Term:           &term,
		VotedFor:       &votedFor,
		Leader:         &v.leader,
		Committed:      &v.committed,
		WitnessEngaged: &v.witnessEngaged,
		Cfg:            &v.cfg,
		MatchIndex:     &mi,
		Down:           &v.down,
	}
}

// diffVoter returns only the fields that changed. Returns nil if nothing changed.
func diffVoter(prev, next testVoter) *vizVoterDelta {
	d := &vizVoterDelta{}
	changed := false

	prevLog := toVizLog(prev.log)
	nextLog := toVizLog(next.log)
	if !vizLogEqual(prevLog, nextLog) {
		d.Log = &nextLog
		changed = true
	}
	if prev.term != next.term {
		d.Term = ptr(fmtTerm(next.term))
		changed = true
	}
	if prev.votedFor != next.votedFor {
		d.VotedFor = ptr(fmtVotedFor(next.votedFor))
		changed = true
	}
	if prev.leader != next.leader {
		d.Leader = ptr(next.leader)
		changed = true
	}
	if prev.committed != next.committed {
		d.Committed = ptr(next.committed)
		changed = true
	}
	if prev.witnessEngaged != next.witnessEngaged {
		d.WitnessEngaged = ptr(next.witnessEngaged)
		changed = true
	}
	if prev.cfg != next.cfg {
		d.Cfg = ptr(next.cfg)
		changed = true
	}
	// Compare match indices for this voter's own ID (leader's self-match).
	var prevMI, nextMI uint64
	if prev.matchIndex != nil {
		prevMI = prev.matchIndex[prev.id]
	}
	if next.matchIndex != nil {
		nextMI = next.matchIndex[next.id]
	}
	if prevMI != nextMI {
		d.MatchIndex = ptr(nextMI)
		changed = true
	}
	if prev.down != next.down {
		d.Down = ptr(next.down)
		changed = true
	}

	if !changed {
		return nil
	}
	return d
}

func snapshotWitness(w testWitness) *vizWitnessDelta {
	term := fmtTerm(w.Vote.Term)
	votedFor := fmtVotedFor(w.Vote.VotedFor)
	return &vizWitnessDelta{
		Vote: &vizVoteDelta{
			Term:     &term,
			VotedFor: &votedFor,
		},
		Acked: &vizAckedDelta{
			Hi:      toVizLogMark(w.Acked.Hi),
			Engaged: &w.Acked.Engaged,
		},
		Down: &w.down,
	}
}

func diffWitness(prev, next testWitness) *vizWitnessDelta {
	var vd *vizVoteDelta
	if prev.Vote != next.Vote {
		vd = &vizVoteDelta{}
		if prev.Vote.Term != next.Vote.Term {
			vd.Term = ptr(fmtTerm(next.Vote.Term))
		}
		if prev.Vote.VotedFor != next.Vote.VotedFor {
			vd.VotedFor = ptr(fmtVotedFor(next.Vote.VotedFor))
		}
	}
	var ad *vizAckedDelta
	if prev.Acked != next.Acked {
		ad = &vizAckedDelta{}
		if prev.Acked.Hi != next.Acked.Hi {
			ad.Hi = toVizLogMark(next.Acked.Hi)
		}
		if prev.Acked.Engaged != next.Acked.Engaged {
			ad.Engaged = ptr(next.Acked.Engaged)
		}
	}
	var dd *bool
	if prev.down != next.down {
		dd = ptr(next.down)
	}
	if vd == nil && ad == nil && dd == nil {
		return nil
	}
	return &vizWitnessDelta{Vote: vd, Acked: ad, Down: dd}
}

// ---------------------------------------------------------------------------
// Exchange to viz message conversion
// ---------------------------------------------------------------------------

func exchangesToVizMessages(exchanges []testExchange) []vizMessage {
	if len(exchanges) == 0 {
		return nil
	}
	msgs := make([]vizMessage, 0, len(exchanges)*2)
	for _, ex := range exchanges {
		// Request message.
		reqType, reqLabel := fmtMsgPayload(ex.req.msg)
		msgs = append(msgs, vizMessage{
			From: ex.req.from, To: ex.req.to,
			Type: reqType, Label: reqLabel,
		})
		// Response message.
		_, dropped := ex.resp.msg.(msgDropped)
		respType, respLabel := fmtMsgPayload(ex.resp.msg)
		msgs = append(msgs, vizMessage{
			From: ex.resp.from, To: ex.resp.to,
			Type: respType, Label: respLabel, Dropped: dropped,
		})
	}
	return msgs
}

func fmtMsgPayload(msg any) (typ, label string) {
	switch m := msg.(type) {
	case msgVoteReq:
		typ = "VoteReq"
		if m.prevote {
			typ = "PreVoteReq"
		}
		label = fmt.Sprintf("%s(term=%s, log=%s)",
			typ, fmtTerm(m.term), fmtLogMark(raft.LogMark(m.lastLog)),
		)
		return typ, label
	case msgVoteResp:
		if m.granted {
			return "VoteResp", "yes"
		}
		if m.reason != "" {
			return "VoteResp", "no: " + m.reason
		}
		return "VoteResp", "no"
	case msgAppendReq:
		last := m.leader.lastLogMark()
		return "AppendReq", fmt.Sprintf("AppendReq([... %s])", fmtLogMark(last))
	case msgAppendResp:
		return "AppendResp", m.detail
	case msgEngage:
		return "Engage", fmt.Sprintf("Engage(term=%s, hi=%s)",
			fmtTerm(m.term), fmtLogMark(raft.LogMark(m.hi)),
		)
	case msgRelease:
		return "Release", fmt.Sprintf("Release(term=%s, hi=%s)",
			fmtTerm(m.term), fmtLogMark(raft.LogMark(m.hi)),
		)
	case msgWitnessResp:
		if m.ok {
			return "WitnessResp", "ok"
		}
		return "WitnessResp", "rejected"
	case msgDropped:
		return "Dropped", "(dropped)"
	default:
		return "?", fmt.Sprintf("%T", msg)
	}
}

// ---------------------------------------------------------------------------
// Deep copy helpers
// ---------------------------------------------------------------------------

func cloneVoters(voters []testVoter) []testVoter {
	out := make([]testVoter, len(voters))
	for i, v := range voters {
		v.log = slices.Clone(v.log)
		if v.matchIndex != nil {
			mi := make(map[raftpb.PeerID]uint64, len(v.matchIndex))
			for k, val := range v.matchIndex {
				mi[k] = val
			}
			v.matchIndex = mi
		}
		out[i] = v
	}
	return out
}

// ---------------------------------------------------------------------------
// Comment parsing
// ---------------------------------------------------------------------------

// parseComments reads a test file and extracts # comment blocks preceding each
// command. Returns a map from line number (of the command) to the comment text
// (with # prefixes and leading whitespace stripped, lines joined by \n).
func parseComments(path string) (map[int]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	result := map[int]string{}
	var commentLines []string
	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			text := strings.TrimPrefix(trimmed, "# ")
			if text == "#" {
				text = "" // bare "#" is a blank line in comments
			}
			commentLines = append(commentLines, text)
		} else if trimmed == "" {
			// Blank lines between comments and commands are fine; keep accumulating.
		} else {
			// This is a command or data line. If we have accumulated comments,
			// associate them with this line number.
			if len(commentLines) > 0 {
				result[lineNum] = strings.Join(commentLines, "\n")
				commentLines = nil
			}
		}
	}
	return result, scanner.Err()
}

// lineFromPos extracts the line number from a datadriven Pos string ("file:line").
func lineFromPos(pos string) int {
	_, after, ok := strings.Cut(pos, ":")
	if !ok {
		return 0
	}
	n, _ := strconv.Atoi(after)
	return n
}

// ---------------------------------------------------------------------------
// fmtCommandLine reconstructs the full command line from TestData.
// ---------------------------------------------------------------------------

func fmtCommandLine(d *datadriven.TestData) string {
	var parts []string
	parts = append(parts, d.Cmd)
	for _, arg := range d.CmdArgs {
		if len(arg.Vals) == 0 {
			parts = append(parts, arg.Key)
		} else {
			parts = append(parts, fmt.Sprintf("%s=(%s)", arg.Key, strings.Join(arg.Vals, ", ")))
		}
	}
	return strings.Join(parts, " ")
}

// ---------------------------------------------------------------------------
// TestGenerateViz
// ---------------------------------------------------------------------------

func TestGenerateViz(t *testing.T) {
	var rewrite bool
	require.NoError(t, sniffarg.DoEnv("rewrite", &rewrite))
	if !rewrite {
		t.Skip("viz generation only runs with --rewrite")
	}

	defer leaktest.AfterTest(t)()
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		comments, err := parseComments(path)
		require.NoError(t, err)

		env := testEnv{}
		ctx := context.Background()
		var trace vizTrace
		trace.TestFile = path

		prevVoters := []testVoter{}
		prevWitness := testWitness{}
		stepIdx := 0

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			// Snapshot state before the command.
			prevVoters = cloneVoters(env.voters)
			prevWitness = env.w

			output := runCommand(t, d, &env, ctx)

			// Build step.
			step := vizStep{
				Index:   stepIdx,
				Command: fmtCommandLine(d),
				Input:   d.Input,
				Output:  output,
				Comment: comments[lineFromPos(d.Pos)],
			}

			// Compute voter deltas.
			voterDeltas := map[string]*vizVoterDelta{}
			for i, next := range env.voters {
				id := fmt.Sprintf("v%d", next.id)
				if i < len(prevVoters) {
					if d := diffVoter(prevVoters[i], next); d != nil {
						voterDeltas[id] = d
					}
				} else {
					voterDeltas[id] = snapshotVoter(next)
				}
			}
			// Check for voter deletion (not expected).
			if len(env.voters) < len(prevVoters) {
				t.Fatal("voter deletion detected; not supported by viz")
			}
			if len(voterDeltas) > 0 {
				step.Voters = voterDeltas
			}

			// Compute witness delta.
			if env.hasWitness {
				if stepIdx == 0 || prevWitness != env.w {
					if stepIdx == 0 {
						step.Witness = snapshotWitness(env.w)
					} else {
						step.Witness = diffWitness(prevWitness, env.w)
					}
				}
			}

			// Capture message exchanges.
			step.Messages = exchangesToVizMessages(env.lastExchanges)

			trace.Steps = append(trace.Steps, step)
			stepIdx++
			return output
		})

		writeVizHTML(t, path, &trace)
	})
}

// ---------------------------------------------------------------------------
// HTML generation
// ---------------------------------------------------------------------------

func writeVizHTML(t *testing.T, testPath string, trace *vizTrace) {
	t.Helper()

	jsonBytes, err := json.Marshal(trace)
	require.NoError(t, err)

	// Compute output path: testdata/foo/bar.txt -> generated/foo/bar.html.
	// RewritableDataPath resolves to the source tree even under Bazel.
	rel, err := filepath.Rel("testdata", testPath)
	require.NoError(t, err)
	outPath := datapathutils.RewritableDataPath(
		t, "pkg", "raft", "witness", "generated", strings.TrimSuffix(rel, ".txt")+".html",
	)
	require.NoError(t, os.MkdirAll(filepath.Dir(outPath), 0755))

	tmpl, err := template.New("viz").Parse(vizHTMLTemplate)
	require.NoError(t, err)

	f, err := os.Create(outPath)
	require.NoError(t, err)
	defer f.Close()

	// Build breadcrumb links from the test file path. The generated file
	// is at generated/<subdir>/<file>.html, so "../" goes to the generated
	// root and "./" stays in the current subdirectory.
	type breadcrumb struct {
		Name string
		Href string
	}
	parts := strings.Split(filepath.Dir(trace.TestFile), string(filepath.Separator))
	// parts is e.g. ["testdata", "basic"]. Link the first to "../", second to "./".
	crumbs := make([]breadcrumb, len(parts))
	for i, p := range parts {
		href := "./"
		for j := i; j < len(parts)-1; j++ {
			href = "../" + href
		}
		crumbs[i] = breadcrumb{Name: p, Href: href}
	}

	err = tmpl.Execute(f, struct {
		TestFile    string
		FileName    string
		Breadcrumbs []breadcrumb
		JSONData    template.JS
	}{
		TestFile:    trace.TestFile,
		FileName:    filepath.Base(trace.TestFile),
		Breadcrumbs: crumbs,
		JSONData:    template.JS(jsonBytes),
	})
	require.NoError(t, err)
	t.Logf("wrote %s", outPath)
}
