// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func makeSpan(s string) Span {
	parts := strings.Split(s, "-")
	if len(parts) == 2 {
		return Span{Key: Key(parts[0]), EndKey: Key(parts[1])}
	}
	return Span{Key: Key(s)}
}

func makeSpans(s string) Spans {
	var spans Spans
	if len(s) > 0 {
		for _, p := range strings.Split(s, ",") {
			spans = append(spans, makeSpan(p))
		}
	}
	return spans
}

func TestMergeSpans(t *testing.T) {
	testCases := []struct {
		spans    string
		expected string
		distinct bool
	}{
		{"", "", true},
		{"a", "a", true},
		{"a,b", "a,b", true},
		{"b,a", "a,b", true},
		{"a,a", "a", false},
		{"a-b", "a-b", true},
		{"a-b,b-c", "a-c", true},
		{"a-c,a-b", "a-c", false},
		{"a,b-c", "a,b-c", true},
		{"a,a-c", "a-c", false},
		{"a-c,b", "a-c", false},
		{"a-c,c", "a-c\x00", true},
		{"a-c,b-bb", "a-c", false},
		{"a-c,b-c", "a-c", false},
	}
	for _, c := range testCases {
		spans := makeSpans(c.spans)
		spans, distinct := MergeSpans((*[]Span)(&spans))
		expected := makeSpans(c.expected)
		require.Equal(t, expected, spans)
		require.Equal(t, c.distinct, distinct)
	}
}

func makeRandomParitialCovering(r *rand.Rand, maxKey int) Spans {
	var ret Spans
	for i := randutil.RandIntInRange(r, 0, maxKey); i < maxKey-1; {
		var s Span
		s.Key = encoding.EncodeVarintAscending(nil, int64(i))
		i = randutil.RandIntInRange(r, i, maxKey)
		s.EndKey = encoding.EncodeVarintAscending(nil, int64(i))
		if i < maxKey && randutil.RandIntInRange(r, 0, 10) > 5 {
			i = randutil.RandIntInRange(r, i, maxKey)
		}
		ret = append(ret, s)
	}
	return ret
}

func TestSubtractSpans(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		testCases := []struct {
			input, remove, expected string
		}{
			{"", "", ""},                               // noop.
			{"a-z", "", "a-z"},                         // noop.
			{"a-z", "a-z", ""},                         // exactly covers everything.
			{"a-z", "a-c", "c-z"},                      // covers a prefix.
			{"a-z", "t-z", "a-t"},                      // covers a suffix.
			{"a-z", "m-p", "a-m,p-z"},                  // covers a proper subspan.
			{"a-z", "a-c,t-z", "c-t"},                  // covers a prefix and suffix.
			{"f-t", "a-f,z-y, ", "f-t"},                // covers a non-covered prefix.
			{"a-b,b-c,d-m,m-z", "", "a-b,b-c,d-m,m-z"}, // noop, but with more spans.
			{"a-b,b-c,d-m,m-z", "a-b,b-c,d-m,m-z", ""}, // everything again. more spans.
			{"a-b,b-c,d-m,m-z", "a-c", "d-m,m-z"},      // subspan spanning input spans.
			{"a-c,c-e,k-m,q-v", "b-d,k-l,q-t", "a-b,d-e,l-m,t-v"},
		}
		for _, tc := range testCases {
			got := SubtractSpans(makeSpans(tc.input), makeSpans(tc.remove))
			if len(got) == 0 {
				got = nil
			}
			require.Equalf(t, makeSpans(tc.expected), got, "testcase: %q - %q", tc.input, tc.remove)
		}
	})

	t.Run("random", func(t *testing.T) {
		const iterations = 100
		for i := 0; i < iterations; i++ {
			r, s := randutil.NewPseudoRand()
			t.Logf("random seed: %d", s)
			const max = 1000
			before := makeRandomParitialCovering(r, max)
			covered := makeRandomParitialCovering(r, max)
			after := SubtractSpans(append(Spans(nil), before...), append(Spans(nil), covered...))
			for i := 0; i < max; i++ {
				k := Key(encoding.EncodeVarintAscending(nil, int64(i)))
				expected := before.ContainsKey(k) && !covered.ContainsKey(k)
				if actual := after.ContainsKey(k); actual != expected {
					t.Errorf("key %q in before? %t in remove? %t in result? %t",
						k, before.ContainsKey(k), covered.ContainsKey(k), after.ContainsKey(k),
					)
				}
			}
		}
	})
}
