// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

import (
	"regexp/syntax"
	"strings"
	"testing"
)

func TestRegexBackspacePattern(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		want    string
		flags   syntax.Flags
	}{
		{`\b`, `\x08`, syntax.Perl},
		{`^a\b$`, `^a\x08$`, syntax.Perl},
		{`[\b]`, `[\x08]`, syntax.Perl},
		{`\\b`, `\\b`, syntax.Perl},
		{`\\\b`, `\\\x08`, syntax.Perl},
		{`\\\\b`, `\\\\b`, syntax.Perl},
		{`\Q\b\E`, `\Q\b\E`, syntax.Perl},
		{`\Q\b\E\b`, `\Q\b\E\x08`, syntax.Perl},
		{`\Q\\E\b`, `\Q\\E\x08`, syntax.Perl},
		{`\\Q\b`, `\\Q\x08`, syntax.Perl},
		{`\Q\b`, `\Q\b`, syntax.Perl},
		{`\b`, `\b`, syntax.Perl | syntax.Literal},
		{`\Q\b\E`, `\Q\b\E`, syntax.Perl | syntax.Literal},
		{`é\b`, `é\x08`, syntax.Perl | syntax.FoldCase},
		{`\n\d+\B`, `\n\d+\B`, syntax.Perl},
		{`plain`, `plain`, syntax.Perl},
		{`\b\`, `\x08\`, syntax.Perl},
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			r := Regex{Regex: tc.pattern, Flags: tc.flags}
			var before, after strings.Builder
			r.ToString(&before, false, false)
			got, err := r.Pattern()
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("expected pattern %q, got %q", tc.want, got)
			}
			r.ToString(&after, false, false)
			if before.String() != after.String() {
				t.Errorf("pattern compilation changed the formatted expression")
			}
		})
	}
}
