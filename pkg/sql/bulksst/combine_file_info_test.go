// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulksst

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestGetMergeSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()

	span := func(key, end string) roachpb.Span {
		return roachpb.Span{
			Key:    []byte(key),
			EndKey: []byte(end),
		}
	}

	tests := []struct {
		name         string
		schemaSpans  []roachpb.Span
		sortedSample []roachpb.Key
		expected     []roachpb.Span
	}{
		{
			name:         "empty samples",
			schemaSpans:  []roachpb.Span{span("a", "z")},
			sortedSample: []roachpb.Key{},
			expected:     []roachpb.Span{span("a", "z")},
		},
		{
			name:        "single schema span with samples",
			schemaSpans: []roachpb.Span{span("a", "z")},
			sortedSample: []roachpb.Key{
				[]byte("c"),
				[]byte("f"),
				[]byte("h"),
			},
			expected: []roachpb.Span{
				span("a", "c"),
				span("c", "f"),
				span("f", "h"),
				span("h", "z"),
			},
		},
		{
			name: "multiple schema spans",
			schemaSpans: []roachpb.Span{
				span("a", "m"),
				span("m", "z"),
			},
			sortedSample: []roachpb.Key{
				[]byte("c"),
				[]byte("f"),
				[]byte("n"),
				[]byte("r"),
			},
			expected: []roachpb.Span{
				span("a", "c"),
				span("c", "f"),
				span("f", "m"),
				span("m", "n"),
				span("n", "r"),
				span("r", "z"),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := getMergeSpans(tc.schemaSpans, tc.sortedSample)
			require.Equal(t, tc.expected, result)
		})
	}
}
