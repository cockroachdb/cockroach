// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"regexp"

	"github.com/cockroachdb/errors"
)

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
			return errors.Wrapf(err, "regexp %d (%q) does not compile", i, reStr)
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
