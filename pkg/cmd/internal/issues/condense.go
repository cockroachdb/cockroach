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

// A CondensedMessage is a test log output garnished with useful helper methods
// that extract concise information for seamless debugging.
type CondensedMessage string

var panicRE = regexp.MustCompile(`(?ms)^(panic:.*?\n)(goroutine \d+.*?\n)\n`)
var fatalRE = regexp.MustCompile(`(?ms)(^F\d{6}.*?\n)(goroutine \d+.*?\n)\n`)

// FatalOrPanic constructs a FatalOrPanic. If no fatal or panic occurred in the
// test, the zero value is returned.
func (s CondensedMessage) FatalOrPanic(numPrecedingLines int) FatalOrPanic {
	ss := string(s)
	var fop FatalOrPanic
	add := func(matches []int) {
		fop.LastLines = lastNlines(ss[:matches[2]], numPrecedingLines)
		fop.Error += ss[matches[2]:matches[3]]
		fop.FirstStack += ss[matches[4]:matches[5]]
	}
	if sl := panicRE.FindStringSubmatchIndex(ss); sl != nil {
		add(sl)
	}
	if sl := fatalRE.FindStringSubmatchIndex(ss); sl != nil {
		add(sl)
	}
	return fop
}

// String calls .Digest(30).
func (s CondensedMessage) String() string {
	return s.Digest(30)
}

// Digest returns the last n lines of the test log. If a panic or fatal error
// occurred, it instead returns the last n lines preceding that event, the
// event itself, and the first stack trace.
func (s CondensedMessage) Digest(n int) string {
	if fop := s.FatalOrPanic(n); fop.Error != "" {
		return fop.LastLines + fop.Error + fop.FirstStack
	}
	// TODO(tbg): consider adding some smarts around the FAIL line here to handle
	// it similarly to FatalOrPanic (but without a stack trace).
	return lastNlines(string(s), n)
}
