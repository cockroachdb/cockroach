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

func TestDataDrivenWitness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		var w State
		ctx := context.Background()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "vote":
				from, term, lm := scanVoteArgs(t, d)
				next, ok := w.HandleMsgVote(ctx, from, term, lm)
				return fmtResult(&w, next, ok)
			case "engage":
				term, lm := scanReleaseArgs(t, d)
				next, ok := w.HandleMsgEngage(ctx, term, lm)
				return fmtResult(&w, next, ok)
			case "release":
				term, lm := scanReleaseArgs(t, d)
				next, ok := w.HandleMsgRelease(ctx, term, lm)
				return fmtResult(&w, next, ok)
			case "state":
				return fmtState(w)
			case "set":
				handleSet(t, d, &w)
				return fmtState(w)
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
				return ""
			}
		})
	})
}

// scanVoteArgs parses from=<id> term=<letter> lm=<logmark> arguments.
func scanVoteArgs(t *testing.T, d *datadriven.TestData) (raftpb.PeerID, raftpb.Term, raft.LogMark) {
	var fromID uint64
	var termStr, lmStr string
	d.ScanArgs(t, "from", &fromID)
	d.ScanArgs(t, "term", &termStr)
	d.ScanArgs(t, "lm", &lmStr)
	return raftpb.PeerID(fromID), mustParseTerm(t, termStr), mustParseLogMark(t, lmStr)
}

// scanReleaseArgs parses term=<letter> lm=<logmark> arguments.
func scanReleaseArgs(t *testing.T, d *datadriven.TestData) (raftpb.Term, raft.LogMark) {
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
			id, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				t.Fatalf("invalid voted-for %q: %v", v, err)
			}
			w.Vote.VotedFor = raftpb.PeerID(id)
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
	return "rejected\n" + fmtState(*w)
}

func fmtState(s State) string {
	var buf strings.Builder
	if s.Vote == (Vote{}) {
		buf.WriteString("vote:  none\n")
	} else if s.Vote.VotedFor == 0 {
		fmt.Fprintf(&buf, "vote:  (%s, -)\n", fmtTerm(s.Vote.Term))
	} else {
		fmt.Fprintf(&buf, "vote:  (%s, %d)\n", fmtTerm(s.Vote.Term), s.Vote.VotedFor)
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
