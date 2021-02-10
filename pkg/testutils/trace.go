// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// MakeAmbientCtx creates an AmbientContext with a Tracer in it.
func MakeAmbientCtx() log.AmbientContext {
	return log.AmbientContext{
		Tracer: tracing.NewTracer(),
	}
}

// MatchInOrder matches interprets the given slice of strings as a slice of
// regular expressions and checks that they match, in order and without overlap,
// against the given string. For example, if s=abcdefg and res=ab,cd,fg no error
// is returned, whereas res=abc,cde would return a descriptive error about
// failing to match cde.
func MatchInOrder(s string, res ...string) error {
	sPos := 0
	for i := range res {
		reStr := "(?ms)" + res[i]
		re, err := regexp.Compile(reStr)
		if err != nil {
			return errors.Errorf("regexp %d (%q) does not compile: %s", i, reStr, err)
		}
		loc := re.FindStringIndex(s[sPos:])
		if loc == nil {
			// Not found.
			return errors.Errorf(
				"unable to find regexp %d (%q) in remaining string:\n\n%s\n\nafter having matched:\n\n%s",
				i, reStr, s[sPos:], s[:sPos],
			)
		}
		sPos += loc[1]
	}
	return nil
}

// MatchEach matches interprets the given slice of strings as a slice of
// regular expressions and checks that they individually match against the given string.
// For example, if s=abcdefg and res=bc,ab,fg no error is returned, whereas
// res=abc,cdg would return a descriptive error about failing to match cde.
func MatchEach(s string, res ...string) error {
	for i := range res {
		reStr := "(?ms)" + res[i]
		re, err := regexp.Compile(reStr)
		if err != nil {
			return errors.Errorf("regexp %d (%q) does not compile: %s", i, reStr, err)
		}
		if re.FindStringIndex(s) == nil {
			// Not found.
			return errors.Errorf(
				"unable to find regexp %d (%q) in string:\n\n%s",
				i, reStr, s,
			)
		}
	}
	return nil
}
