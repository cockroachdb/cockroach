// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracingpb

import (
	"testing"

	"github.com/cockroachdb/redact"
)

func TestCondensePathLinePrefix(t *testing.T) {
	for _, test := range []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no_directory",
			input:    "foo.go:1234 message",
			expected: "message",
		},
		{
			name:     "full_path",
			input:    "pkg/foo/bar.go:123 message",
			expected: "message",
		},
		{
			// Won't redact because there are redaction markers in the path.
			// This doesn't happen in the real world but we want to make sure
			// condensePathLinePrefix doesn't mess with redaction at all.
			name:     "redaction_in_path",
			input:    "pkg/‹foo›/bar.go:123 message",
			expected: "pkg/‹foo›/bar.go:123 message",
		},
		{
			// Redaction in message is expected and okay, since
			// condensePathLinePrefix is written in a way to
			// preserve that.
			name:     "redaction_in_message",
			input:    "pkg/foo/bar.go:123 ‹sensitive› message",
			expected: "‹sensitive› message",
		},
		{
			// Not expecting it in the real world since we only call
			// condensePathLinePrefix in formatMinimal, but if we get formatFull, it
			// also works.
			name:     "full_format",
			input:    "event: pkg/foo/bar.go:1234 ‹sensitive› message",
			expected: "‹sensitive› message",
		},
		{
			name:     "unredacted_trace",
			input:    "‹pkg/foo/bar.go:1234 some message›",
			expected: "‹some message›",
		},
		{
			// Should never happen (there would be escaping). But either way, works
			// just the same.
			name:     "unredacted_trace_nested_unescaped",
			input:    "‹pkg/foo/bar.go:1234 some ‹message››",
			expected: "‹some ‹message››",
		},
		{
			name:     "unredacted_trace_nested_escaped",
			input:    "‹pkg/foo/bar.go:1234 " + string(redact.EscapeMarkers([]byte("some ‹message›"))) + "›",
			expected: "‹some ?message?›", // NB: this is verbatim the escaped string inserted above
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := string(condensePathLinePrefix(redact.RedactableString(test.input)))
			if got != test.expected {
				t.Errorf("expected %q, got %q", test.expected, got)
			}
		})
	}
}
