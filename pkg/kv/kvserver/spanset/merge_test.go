// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanset

import (
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestMergeSpans(t *testing.T) {
	for _, tc := range []struct {
		spans    string
		expected string
	}{
		{"", ""},
		{"a", "a"},
		{"a,b", "a,b"},
		{"b,a", "a,b"},
		{"a,a", "a"},
		{"a-b", "a-b"},
		{"a-b,b-c", "a-c"},
		{"a-c,a-b", "a-c"},
		{"a,b-c", "a,b-c"},
		{"a,a-c", "a-c"},
		{"a-c,b", "a-c"},
		{"a-c,c", "a-c\x00"},
		{"a-c,b-bb", "a-c"},
		{"a-c,b-c", "a-c"},
		{"a-c,b-d", "a-d"},

		{"a-b,b-b\x00", "a-b\x00"},
		{"b,b\x00", "b,b\x00"},     // TODO(pav-kv): should merge here too
		{"b,b\x00-c", "b,b\x00-c"}, // TODO(pav-kv): should merge here too

		{"a@10", "a@10"},
		{"a@10,b@10", "a@10,b@10"},
		{"a@10,b@20", "a@10,b@20"},
		{"b@20,a@10", "a@10,b@20"},
		{"a@10,a-b@10", "a-b@10"},
		{"a@10,a-b@20", "a@10,a-b@20"},
		{"a-b@20,b@20", "a-b\x00@20"},
		{"a-b@20,b@30", "a-b@20,b@30"},
		{"a-c@20,m-n@10,c-o@10,o-z@20", "a-c@20,c-o@10,o-z@20"},
	} {
		t.Run("", func(t *testing.T) {
			spans := mergeSpans(makeSpans(t, tc.spans))
			expected := makeSpans(t, tc.expected)
			require.Equal(t, expected, spans, tc.spans)
		})
	}
}

func makeSpan(t *testing.T, s string) Span {
	var ts hlc.Timestamp
	if idx := strings.IndexByte(s, '@'); idx != -1 {
		i, err := strconv.ParseInt(s[idx+1:], 10, 63)
		require.NoError(t, err)
		ts = hlc.Timestamp{WallTime: i}
		s = s[:idx]
	}
	parts := strings.Split(s, "-")
	if len(parts) != 2 {
		return Span{Span: roachpb.Span{Key: roachpb.Key(s)}, Timestamp: ts}
	}
	return Span{
		Span: roachpb.Span{
			Key:    roachpb.Key(parts[0]),
			EndKey: roachpb.Key(parts[1]),
		},
		Timestamp: ts,
	}
}

func makeSpans(t *testing.T, s string) []Span {
	var spans []Span
	for _, p := range strings.Split(s, ",") {
		spans = append(spans, makeSpan(t, p))
	}
	return spans
}
