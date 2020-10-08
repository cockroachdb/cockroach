// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	if !strings.Contains(s, "smalltrace_test.go:1009:util.testSmallTrace,smalltrace_test.go:1013:util.TestGenerateSmallTrace") {
		t.Fatalf("trace not generated properly: %q", s)
	}
}

func testSmallTrace(t *testing.T) {
	testSmallTrace2(t)
}

func TestGenerateSmallTrace(t *testing.T) {
	testSmallTrace(t)
}
