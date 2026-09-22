// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package issues

import (
	"fmt"
	"regexp"
	"strings"
)

func firstNlines(input string, n int) string {
	if input == "" {
		return ""
	}
	pos := 0
	for pos < len(input) && n > 0 {
		n--
		pos += strings.Index(input[pos:], "\n") + 1
	}
	return input[:pos]
}

func lastNlines(input string, n int) string {
	if input == "" {
		return ""
	}
	pos := len(input) - 1
	for pos >= 0 && n > 0 {
		n--
		pos = strings.LastIndex(input[:pos], "\n")
	}
	return input[pos+1:]
}

// FatalOrPanic contains a fatal error or panic obtained from a test log.
type FatalOrPanic struct {
	LastLines, // last log lines preceding the error
	Error, // the error (i.e. the panic or fatal error log lines)
	FirstStack string // the first stack, i.e. the goroutine relevant to error
}

// RSGCrash contains information about a crash during random syntax tests
// obtained from a test log.
type RSGCrash struct {
	Error, // the error message from the crash
	Query, // the query that induced the crash
	Schema string // the schema that the crash was induced with
}

// FatalNodeRoachtest contains a fatal error from a node from a roachtest
type FatalNodeRoachtest struct {
	Message,
	FatalLogs string
}

// NodeToIpMappingRoachtest contains the node to ip mapping from a roachtest
// cluster
type NodeToIpMappingRoachtest struct {
	Message,
	NodeToIpMapping string
}

// DataRace contains a data race report obtained from a test log.
type DataRace struct {
	LastLines, // last log lines preceding the report
	Report string // the report, starting at the WARNING line
}

// A CondensedMessage is a test log output garnished with useful helper methods
// that extract concise information for seamless debugging.
type CondensedMessage string

// dataRaceRE matches one race detector report. The trailing delimiter is
// optional: with GORACE halt_on_error=1 the report is the last thing the
// process prints, and the log may also have been truncated mid-report.
var dataRaceRE = regexp.MustCompile(`(?ms)^(?:={10,}\n)?(WARNING: DATA RACE\n.*?)(?:^={10,}\n|\z)`)

var panicRE = regexp.MustCompile(`(?ms)^(panic:.*?\n)(goroutine \d+.*?\n)\n`)
var fatalRE = regexp.MustCompile(`(?ms)(^F\d{6}.*?\n)(goroutine \d+.*?\n)\n`)

// Note: These must be kept in-sync with the crash output of
// tests_test.testRandomSyntax.
var crasherRE = regexp.MustCompile(`(?s)( *rsg_test.go:\d{3}: Crash detected:.*?\n)(.*?;\n)`)
var reproRE = regexp.MustCompile(`(?s)( *rsg_test.go:\d{3}: To reproduce, use schema:)`)

var nodeFatalRoachtestRE = regexp.MustCompile(`(?ms)\A(.*?\n)((?:^F\d{6}\b[^\n]*(?:\n|$))+)`)

// nodeToIpRoachtestRE matches an entire Markdown table block
// - including header, separator, and data rows:
// - handles an arbitrary number of columns
//
// | Node | Public IP | Private IP |
// | --- | --- | --- |
// | node-0001 | 1.1.1.0 | 1.1.1.1 |
var nodeToIpRoachtestRE = regexp.MustCompile(`(?m)^[[:space:]]*\|(?:[^|\n]*\|)+[[:space:]]*\n^[[:space:]]*\|(?:[[:space:]]*:?-{3,}:?[[:space:]]*\|)+[[:space:]]*\n(?:^[[:space:]]*\|(?:[^|\n]*\|)+[[:space:]]*(?:\n|$))+`)

// FatalOrPanic constructs a FatalOrPanic. If no fatal or panic occurred in the
// test, ok=false is returned.
func (s CondensedMessage) FatalOrPanic(numPrecedingLines int) (fop FatalOrPanic, ok bool) {
	ss := string(s)
	add := func(matches []int) {
		fop.LastLines = lastNlines(ss[:matches[2]], numPrecedingLines)
		fop.Error += ss[matches[2]:matches[3]]
		fop.FirstStack += ss[matches[4]:matches[5]]
		ok = true
	}
	if sl := panicRE.FindStringSubmatchIndex(ss); sl != nil {
		add(sl)
	}
	if sl := fatalRE.FindStringSubmatchIndex(ss); sl != nil {
		add(sl)
	}
	return fop, ok
}

// RSGCrash constructs an RSGCrash. The query and reproduction SQL are limited
// to the first lineLimit lines. If no random syntax test crash occurred in the
// test, ok=false is returned.
func (s CondensedMessage) RSGCrash(lineLimit int) (c RSGCrash, ok bool) {
	ss := string(s)
	if cm := crasherRE.FindStringSubmatchIndex(ss); cm != nil {
		c.Error = ss[cm[2]:cm[3]]
		c.Query = firstNlines(ss[cm[4]:cm[5]], lineLimit)
		if rm := reproRE.FindStringSubmatchIndex(ss); rm != nil {
			// The "To reproduce" log is always near the end of the log file, so
			// collect all lines after the first match position.
			c.Schema = firstNlines(ss[rm[2]:], lineLimit)
		}
		return c, true
	}
	return RSGCrash{}, false
}

// FatalNodeRoachtest constructs a FatalNodeRoachtest which is used to
// construct an issue with node fatal logs in a Roachtest. If not found, or if
// regex matching doesn't return the exact expected number of matches,
// ok=false is returned
func (s CondensedMessage) FatalNodeRoachtest() (fnr FatalNodeRoachtest, ok bool) {
	ss := string(s)
	if matches := nodeFatalRoachtestRE.FindStringSubmatchIndex(ss); matches != nil {
		if len(matches) != 6 {
			return FatalNodeRoachtest{}, false
		}
		msg := ss[matches[2]:matches[3]]
		fnr.Message = strings.TrimRight(msg, "\n")
		fnr.FatalLogs = ss[matches[4]:matches[5]]
		return fnr, true
	}
	return FatalNodeRoachtest{}, false
}

// NodeToIpMappingRoachtest constructs a NodeToIpMappingRoachtest which is
// used to construct an issue that contains cluster information
func (s CondensedMessage) NodeToIpMappingRoachtest() (nodeIpMap NodeToIpMappingRoachtest, ok bool) {
	ss := string(s)
	if matches := nodeToIpRoachtestRE.FindStringSubmatchIndex(ss); matches != nil {
		if len(matches) != 2 {
			// only expecting a single match
			return NodeToIpMappingRoachtest{}, false
		}
		msg := ss[0:matches[0]]
		nodeIpMap.Message = strings.TrimRight(msg, "\n")
		nodeIpMap.NodeToIpMapping = ss[matches[0]:matches[1]]
		return nodeIpMap, true
	}
	return NodeToIpMappingRoachtest{}, false
}

// HasDataRace reports whether the log contains a race detector report. It is
// the same test DataRace applies, so callers deciding what to do with a
// failure and callers rendering it cannot disagree.
func (s CondensedMessage) HasDataRace() bool {
	return dataRaceRE.MatchString(string(s))
}

// DataRace constructs a DataRace from the first race detector report in the
// log, preceded by up to numPrecedingLines lines of context. If the log holds
// no report, ok=false is returned.
func (s CondensedMessage) DataRace(numPrecedingLines int) (dr DataRace, ok bool) {
	ss := string(s)
	sl := dataRaceRE.FindStringSubmatchIndex(ss)
	if sl == nil {
		return DataRace{}, false
	}
	dr.LastLines = lastNlines(ss[:sl[0]], numPrecedingLines)
	dr.Report = elideRepeatedStackFrames(ss[sl[2]:sl[3]])
	return dr, true
}

// dataRaceReportLineLimit is the report size, in lines, above which
// elideRepeatedStackFrames kicks in. A race detector stack is capped at 256
// frames (ThreadSanitizer's kStackTraceMax) and prints two lines per frame, so
// a longer report has a stack that hit the cap.
//
// Such a stack is not a real call chain. TSan reconstructs it by replaying a
// bounded ring of function entry and exit events; for a goroutine that has
// been looping since the start of the test the ring has long since wrapped, so
// exits lose their matching entries and the call sites of the loop body pile
// up on themselves until the cap. Reports like that run to a thousand lines
// and crowd the rest of the issue body out past the GitHub length limit.
const dataRaceReportLineLimit = 512

// dataRaceStackMaxFrameCopies is how many copies of a frame one stack may
// contribute; past that the frame is omitted.
const dataRaceStackMaxFrameCopies = 3

// elideRepeatedStackFrames shortens an oversized race detector report, keeping
// at most dataRaceStackMaxFrameCopies copies of any given frame within a
// single stack. Each gap left behind is marked with an ellipsis, and the total
// number of lines dropped is reported once per stack. Blank lines delimit the
// stacks, so the bookkeeping resets at each one. Reports at or below
// dataRaceReportLineLimit are returned unchanged.
//
// Frames are dropped wherever they recur, not only where they pile up, so one
// that legitimately appears at several depths -- an interceptor chain wrapper,
// say -- keeps only its first three. The linked test log has the unabridged
// report.
func elideRepeatedStackFrames(report string) string {
	lines := strings.Split(report, "\n")
	if len(lines) <= dataRaceReportLineLimit {
		return report
	}
	out := make([]string, 0, len(lines))
	emitted := make(map[string]int)
	omitted, skipping := 0, false
	endStack := func() {
		if omitted > 0 {
			out = append(out, fmt.Sprintf("  [%d repeated lines omitted; see full output]", omitted))
		}
		omitted, skipping = 0, false
		clear(emitted)
	}
	for i := 0; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "" {
			endStack()
			out = append(out, lines[i])
			continue
		}
		// A frame is a function line plus the location line beneath it. The two
		// are counted together because dropping one would orphan the other: a
		// function inlined into several call sites prints one identical function
		// line per site but a distinct location line for each, so the function
		// line runs out of budget first.
		frame := lines[i : i+1]
		if i+1 < len(lines) && strings.HasPrefix(lines[i+1], "      ") {
			frame = lines[i : i+2]
			i++
		}
		key := strings.Join(frame, "\n")
		if emitted[key] >= dataRaceStackMaxFrameCopies {
			if !skipping {
				out = append(out, "  [...]")
				skipping = true
			}
			omitted += len(frame)
			continue
		}
		skipping = false
		emitted[key]++
		out = append(out, frame...)
	}
	endStack()
	return strings.Join(out, "\n")
}

// String calls .Digest(30).
func (s CondensedMessage) String() string {
	return s.Digest(30)
}

// Digest returns the last n lines of the test log. If a data race was
// reported, or a panic or fatal error occurred, it instead returns the last n
// lines preceding that event followed by the event itself: the race report, or
// the panic or fatal error plus its first stack trace. If a crash occurred during a random syntax
// test, it returns the error and up to n lines of both the query that caused
// the crash and the database schema.
func (s CondensedMessage) Digest(n int) string {
	if dr, ok := s.DataRace(n); ok {
		return dr.LastLines + dr.Report
	}
	if fop, ok := s.FatalOrPanic(n); ok {
		return fop.LastLines + fop.Error + fop.FirstStack
	}
	if c, ok := s.RSGCrash(n); ok {
		return c.Error + c.Query + c.Schema
	}
	// TODO(tbg): consider adding some smarts around the FAIL line here to handle
	// it similarly to FatalOrPanic (but without a stack trace).
	return lastNlines(string(s), n)
}
