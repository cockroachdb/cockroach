// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracing

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/pmezard/go-difflib/difflib"
)

// FindMsgInRecording returns the index of the first Span containing msg in its
// logs, or -1 if no Span is found.
func FindMsgInRecording(recording tracingpb.Recording, msg string) int {
	for i, sp := range recording {
		if LogsContainMsg(sp, msg) {
			return i
		}
	}
	return -1
}

// LogsContainMsg returns true if a Span's logs contain the given message.
func LogsContainMsg(sp tracingpb.RecordedSpan, msg string) bool {
	for _, l := range sp.Logs {
		if strings.Contains(l.Msg().StripMarkers(), msg) {
			return true
		}
	}
	return false
}

// CountLogMessages counts the messages containing msg.
func CountLogMessages(sp tracingpb.RecordedSpan, msg string) int {
	res := 0
	for _, l := range sp.Logs {
		if strings.Contains(l.Msg().StripMarkers(), msg) {
			res++
		}
	}
	return res
}

// CheckRecordedSpans checks whether a recording looks like an expected
// one represented by a string with one line per expected span and one line per
// expected event (i.e. log message), with a tab-indentation for child spans.
//
//	if err := CheckRecordedSpans(Span.GetRecording(), `
//	    span: root
//	        event: a
//	        span: child
//	            event: [ambient] b
//	            event: c
//	`); err != nil {
//	  t.Fatal(err)
//	}
//
// The event lines can (and generally should) omit the file:line part that they
// might contain (depending on the level at which they were logged).
//
// Note: this test function is in this file because it needs to be used by
// both tests in the tracing package and tests outside of it, and the function
// itself depends on tracing.
func CheckRecordedSpans(rec tracingpb.Recording, expected string) error {
	normalize := func(rec string) string {
		// normalize the string form of a recording for ease of comparison.
		//
		// 1. Strip out any leading new lines.
		rec = strings.TrimLeft(rec, "\n")
		// 2. Strip out trailing whitespace.
		rec = strings.TrimRight(rec, "\n\t ")
		// 3. Strip out file:line information from the recordings.
		//
		// 	 Before |  "event: util/log/trace_test.go:111 log"
		// 	 After  |  "event: log"
		re := regexp.MustCompile(`event: .*:[0-9]*`)
		rec = string(re.ReplaceAll([]byte(rec), []byte("event:")))
		// 4. Change all tabs to four spaces.
		rec = strings.ReplaceAll(rec, "\t", "    ")
		// 5. Compute the outermost indentation.
		indent := strings.Repeat(" ", len(rec)-len(strings.TrimLeft(rec, " ")))
		// 6. Outdent each line by that amount.
		var lines []string
		for _, line := range strings.Split(rec, "\n") {
			lines = append(lines, strings.TrimPrefix(line, indent))
		}
		// 7. Stitch everything together.
		return strings.Join(lines, "\n")
	}

	var rows []string
	row := func(depth int, format string, args ...interface{}) {
		rows = append(rows, strings.Repeat("    ", depth)+fmt.Sprintf(format, args...))
	}

	mapping := make(map[tracingpb.SpanID]tracingpb.SpanID) // spanID -> parentSpanID
	for _, rs := range rec {
		mapping[rs.SpanID] = rs.ParentSpanID
	}
	depth := func(spanID tracingpb.SpanID) int {
		// Traverse up the parent links until one is not found.
		curSpanID := spanID
		d := 0
		for {
			var ok bool
			curSpanID, ok = mapping[curSpanID]
			if !ok {
				break
			}
			d++
		}
		return d
	}

	for _, rs := range rec {
		d := depth(rs.SpanID)
		row(d, "span: %s", rs.Operation)
		var tags []string
		for _, tagGroup := range rs.TagGroups {
			for _, tag := range tagGroup.Tags {
				tags = append(tags, fmt.Sprintf("%s=%v", tag.Key, tag.Value))
			}
		}
		if len(tags) > 0 {
			sort.Strings(tags)
			row(d, "    tags: %s", strings.Join(tags, " "))
		}

		for _, l := range rs.Logs {
			row(d, "    event: %s", l.Msg().StripMarkers())
		}
	}

	exp := normalize(expected)
	got := normalize(strings.Join(rows, "\n"))
	if got != exp {
		diff := difflib.UnifiedDiff{
			A:        difflib.SplitLines(exp),
			FromFile: "exp",
			B:        difflib.SplitLines(got),
			ToFile:   "got",
			Context:  4,
		}
		diffText, _ := difflib.GetUnifiedDiffString(diff)
		return errors.Newf("unexpected diff:\n%s\n\nrecording:\n%s", diffText, rec.String())
	}
	return nil
}

// T is a subset of testing.TB.
type T interface {
	Fatalf(format string, args ...interface{})
}

// checkRecording checks whether a recording looks like the expected
// one. The expected string is allowed to elide timing information, and the
// outer-most indentation level is adjusted for when comparing.
//
//	checkRecording(t, sp.GetRecording(), `
//	    === operation:root
//	    [childrenMetadata]
//	    event:root 1
//	        === operation:remote child
//	        event:remote child 1
//	`)
func checkRecording(t T, rec tracingpb.Recording, expected string) {
	checkRecordingWithRedact(t, rec, expected, false)
}

func checkRecordingWithRedact(t T, rec tracingpb.Recording, expected string, redactValues bool) {
	normalize := func(rec string) string {
		// normalize the string form of a recording for ease of comparison.
		//
		// 1. Strip out any leading new lines.
		rec = strings.TrimLeft(rec, "\n")
		// 2. Strip out trailing newlines.
		rec = strings.TrimRight(rec, "\n")
		// 3. Strip out all timing information from the recordings.
		//
		// 	 Before |  "0.007ms      0.007ms    event:root 1"
		// 	 After  |  "event:root 1"
		re := regexp.MustCompile(`.*s.*s\s{4}`)
		rec = string(re.ReplaceAll([]byte(rec), nil))
		// 4. Strip out all the metadata from each ChildrenMetadata entry.
		//
		// 	 Before |  [operation: {Count:<count>, Duration:<duration>}]
		// 	 After  |  [operation]
		re = regexp.MustCompile(`: .*]`)
		rec = string(re.ReplaceAll([]byte(rec), []byte("]")))
		// 5. Change all tabs to four spaces.
		rec = strings.ReplaceAll(rec, "\t", "    ")
		// 6. Compute the outermost indentation.
		indent := strings.Repeat(" ", len(rec)-len(strings.TrimLeft(rec, " ")))
		// 7. Outdent each line by that amount.
		var lines []string
		for _, line := range strings.Split(rec, "\n") {
			lines = append(lines, strings.TrimPrefix(line, indent))
		}
		// 8. Stitch everything together.
		return strings.Join(lines, "\n")
	}

	sortChildrenMetadataByName := func(m map[string]tracingpb.OperationMetadata) {
		// Sort the OperationMetadata of s' children alphabetically.
		childrenMetadata := make([]tracingpb.OperationAndMetadata, 0, len(m))
		for operation, metadata := range m {
			childrenMetadata = append(childrenMetadata,
				tracingpb.OperationAndMetadata{Operation: operation, Metadata: metadata})
		}
		sort.Slice(childrenMetadata, func(i, j int) bool {
			return childrenMetadata[i].Operation < childrenMetadata[j].Operation
		})

		for i, cm := range childrenMetadata {
			metadata := m[cm.Operation]
			metadata.Duration = time.Duration(float64(i) * time.Second.Seconds())
			m[cm.Operation] = metadata
		}
	}

	// ChildrenMetadata are sorted in descending order of duration when returned.
	// To ensure a stable sort in tests, we set the durations to sort in an
	// alphabetical descending order.
	for i := range rec {
		sortChildrenMetadataByName(rec[i].ChildrenMetadata)
	}

	actual := redact.Sprint(rec)
	if redactValues {
		actual = actual.Redact()
	}
	exp := normalize(expected)
	got := normalize(string(actual))
	if got != exp {
		diff := difflib.UnifiedDiff{
			A:        difflib.SplitLines(exp),
			FromFile: "exp",
			B:        difflib.SplitLines(got),
			ToFile:   "got",
			Context:  4,
		}
		diffText, _ := difflib.GetUnifiedDiffString(diff)
		t.Fatalf("unexpected diff:\n%s", redact.RedactableString(diffText))
	}
}
