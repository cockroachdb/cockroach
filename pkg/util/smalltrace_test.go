// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"strings"
	"testing"
)

// Renumber lines so they're stable no matter what changes above. (We
// could make the regexes accept any string of digits, but we also
// want to make sure that the correct line numbers get captured).
//
//line smalltrace_test.go:1000

func testSmallTrace2(t *testing.T) {
	s := GetSmallTrace(2)
	if !strings.Contains(string(s), "smalltrace_test.go:‹1002›:util.testSmallTrace2,smalltrace_test.go:‹1009›:util.testSmallTrace,smalltrace_test.go:‹1013›:util.TestGenerateSmallTrace") {
		t.Fatalf("trace not generated properly: %q", s)
	}
}

func testSmallTrace(t *testing.T) {
	testSmallTrace2(t)
}

func TestGenerateSmallTrace(t *testing.T) {
	testSmallTrace(t)
}
