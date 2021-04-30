// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package issues

import (
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

// A CondensedMessage is a test log output garnished with useful helper methods
// that extract concise information for seamless debugging.
type CondensedMessage string

var panicRE = regexp.MustCompile(`(?ms)^(panic:.*?\n)(goroutine \d+.*?\n)\n`)
var fatalRE = regexp.MustCompile(`(?ms)(^F\d{6}.*?\n)(goroutine \d+.*?\n)\n`)

// Note: These must be kept in-sync with the crash output of
// tests_test.testRandomSyntax.
var crasherRE = regexp.MustCompile(`(?s)( *rsg_test.go:\d{3}: Crash detected:.*?\n)(.*?;\n)`)
var reproRE = regexp.MustCompile(`(?s)( *rsg_test.go:\d{3}: To reproduce, use schema:)`)

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

// String calls .Digest(30).
func (s CondensedMessage) String() string {
	return s.Digest(30)
}

// Digest returns the last n lines of the test log. If a panic or fatal error
// occurred, it instead returns the last n lines preceding that event, the event
// itself, and the first stack trace. If a crash occurred during a random syntax
// test, it returns the error and up to n lines of both the query that caused
// the crash and the database schema.
func (s CondensedMessage) Digest(n int) string {
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
