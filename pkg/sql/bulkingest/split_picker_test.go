// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulkingest

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestPickSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name          string
		spans         []roachpb.Span
		ssts          []execinfrapb.BulkMergeSpec_SST
		expected      []roachpb.Span
		expectedError string
	}{
		{
			name:     "empty ssts",
			spans:    []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}},
			ssts:     nil,
			expected: []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}},
		},
		{
			name:          "empty spans",
			spans:         nil,
			ssts:          []execinfrapb.BulkMergeSpec_SST{{StartKey: "a", EndKey: "b"}},
			expectedError: "no spans provided",
		},
		{
			name: "unordered spans",
			spans: []roachpb.Span{
				{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			},
			ssts:          []execinfrapb.BulkMergeSpec_SST{{StartKey: "a", EndKey: "b"}},
			expectedError: "spans not ordered: \"c\" >= \"a\"",
		},
		{
			name: "overlapping spans",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
				{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")},
			},
			ssts:          []execinfrapb.BulkMergeSpec_SST{{StartKey: "a", EndKey: "b"}},
			expectedError: "spans are overlapping: \"c\" overlaps with \"b\"",
		},
		{
			name: "unordered ssts",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "c", EndKey: "d"},
				{StartKey: "a", EndKey: "b"},
			},
			expectedError: "SSTs not in order",
		},
		{
			name: "overlapping ssts",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "a", EndKey: "c"},
				{StartKey: "b", EndKey: "d"},
			},
			expectedError: "overlapping SSTs",
		},
		{
			name: "sst extends beyond span",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "a", EndKey: "d"},
			},
			expectedError: "SST ending at \"d\" extends beyond containing span ending at \"c\"",
		},
		{
			name: "sst starts before span",
			spans: []roachpb.Span{
				{Key: roachpb.Key("b"), EndKey: roachpb.Key("d")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "a", EndKey: "c"},
			},
			expectedError: "SST starting at \"a\" begins before containing span starting at \"b\"",
		},
		{
			name: "single span with no ssts",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "single span with one sst",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "c", EndKey: "d"},
			},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "single span with multiple ssts",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "c", EndKey: "d"},
				{StartKey: "f", EndKey: "g"},
				{StartKey: "i", EndKey: "j"},
			},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("f")},
				{Key: roachpb.Key("f"), EndKey: roachpb.Key("i")},
				{Key: roachpb.Key("i"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "multiple spans with multiple ssts",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("m")},
				{Key: roachpb.Key("m"), EndKey: roachpb.Key("z")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "c", EndKey: "d"},
				{StartKey: "f", EndKey: "g"},
				{StartKey: "o", EndKey: "p"},
				{StartKey: "r", EndKey: "s"},
			},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("f")},
				{Key: roachpb.Key("f"), EndKey: roachpb.Key("m")},
				{Key: roachpb.Key("m"), EndKey: roachpb.Key("r")},
				{Key: roachpb.Key("r"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "span with no ssts followed by span with ssts",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("e")},
				{Key: roachpb.Key("e"), EndKey: roachpb.Key("z")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "f", EndKey: "g"},
				{StartKey: "i", EndKey: "j"},
			},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("e")},
				{Key: roachpb.Key("e"), EndKey: roachpb.Key("i")},
				{Key: roachpb.Key("i"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "span with ssts followed by span with no ssts",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("k")},
				{Key: roachpb.Key("k"), EndKey: roachpb.Key("z")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "c", EndKey: "d"},
				{StartKey: "f", EndKey: "g"},
			},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("f")},
				{Key: roachpb.Key("f"), EndKey: roachpb.Key("k")},
				{Key: roachpb.Key("k"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "sst at span boundary",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("k")},
				{Key: roachpb.Key("k"), EndKey: roachpb.Key("z")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "a", EndKey: "b"},
				{StartKey: "j", EndKey: "k"},
				{StartKey: "k", EndKey: "l"},
			},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("j")},
				{Key: roachpb.Key("j"), EndKey: roachpb.Key("k")},
				{Key: roachpb.Key("k"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "sst completely outside all spans",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
				{Key: roachpb.Key("c"), EndKey: roachpb.Key("f")},
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "a", EndKey: "b"},
				{StartKey: "d", EndKey: "e"},
				{StartKey: "x", EndKey: "z"}, // This SST is completely outside all spans
			},
			expectedError: "SST starting at x not contained in any span",
		},
		{
			name: "sst in gap between non-contiguous spans",
			spans: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
				{Key: roachpb.Key("e"), EndKey: roachpb.Key("g")}, // Gap from [c, e)
			},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "a", EndKey: "b"},
				{StartKey: "c", EndKey: "d"}, // This SST is in the gap [c, e)
			},
			expectedError: "SST starting at \"c\" begins before containing span starting at \"e\"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := pickSplits(tc.spans, tc.ssts)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestPickSplitsForSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name     string
		span     roachpb.Span
		ssts     []execinfrapb.BulkMergeSpec_SST
		expected []roachpb.Span
	}{
		{
			name: "empty ssts",
			span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			ssts: nil,
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "single sst",
			span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "c", EndKey: "d"},
			},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "two ssts",
			span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "c", EndKey: "d"},
				{StartKey: "f", EndKey: "g"},
			},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("f")},
				{Key: roachpb.Key("f"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "three ssts",
			span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "c", EndKey: "d"},
				{StartKey: "f", EndKey: "g"},
				{StartKey: "i", EndKey: "j"},
			},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("f")},
				{Key: roachpb.Key("f"), EndKey: roachpb.Key("i")},
				{Key: roachpb.Key("i"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "sst at span boundary",
			span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "a", EndKey: "b"},
				{StartKey: "y", EndKey: "z"},
			},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("y")},
				{Key: roachpb.Key("y"), EndKey: roachpb.Key("z")},
			},
		},
		{
			name: "adjacent ssts",
			span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")},
			ssts: []execinfrapb.BulkMergeSpec_SST{
				{StartKey: "c", EndKey: "d"},
				{StartKey: "d", EndKey: "e"},
				{StartKey: "e", EndKey: "f"},
			},
			expected: []roachpb.Span{
				{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")},
				{Key: roachpb.Key("d"), EndKey: roachpb.Key("e")},
				{Key: roachpb.Key("e"), EndKey: roachpb.Key("z")},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := pickSplitsForSpan(tc.span, tc.ssts)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestPickSplitsForSpanErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name          string
		span          roachpb.Span
		ssts          []execinfrapb.BulkMergeSpec_SST
		expectedError string
	}{
		{
			name: "SST with column family suffix - not at safe split point",
			span: func() roachpb.Span {
				// Create a span covering table 50, index 1
				codec := keys.SystemSQLCodec
				start := codec.IndexPrefix(50, 1)
				end := codec.IndexPrefix(50, 2)
				return roachpb.Span{Key: start, EndKey: end}
			}(),
			ssts: func() []execinfrapb.BulkMergeSpec_SST {
				codec := keys.SystemSQLCodec
				rowPrefix := encoding.EncodeUvarintAscending(codec.IndexPrefix(50, 1), 100)
				// Create SSTs where the second SST starts at a column family boundary
				// (not a safe split point)
				sst1Start := codec.IndexPrefix(50, 1)
				sst1End := encoding.EncodeUvarintAscending(codec.IndexPrefix(50, 1), 50)
				// SST 2 starts at row 100, column family 2 (not a safe split point)
				sst2Start := keys.MakeFamilyKey(rowPrefix, 2)
				sst2End := codec.IndexPrefix(50, 2)

				return []execinfrapb.BulkMergeSpec_SST{
					{StartKey: string(sst1Start), EndKey: string(sst1End)},
					{StartKey: string(sst2Start), EndKey: string(sst2End)},
				}
			}(),
			expectedError: "SST 1 start key .* is not at a safe split point.*SST writer should have ensured safe boundaries",
		},
		{
			name: "SST with malformed table key that EnsureSafeSplitKey cannot handle",
			span: func() roachpb.Span {
				codec := keys.SystemSQLCodec
				start := codec.IndexPrefix(50, 1)
				end := codec.IndexPrefix(50, 2)
				return roachpb.Span{Key: start, EndKey: end}
			}(),
			ssts: func() []execinfrapb.BulkMergeSpec_SST {
				codec := keys.SystemSQLCodec
				// Create a properly formed first SST
				sst1Start := codec.IndexPrefix(50, 1)
				sst1End := encoding.EncodeUvarintAscending(codec.IndexPrefix(50, 1), 100)

				// Create a malformed second SST with truncated/invalid encoding
				// This will start with the table prefix but have invalid varint encoding
				sst2Start := append(append(roachpb.Key(nil), codec.IndexPrefix(50, 1)...), 0xff, 0xff, 0xff, 0xff, 0xff)
				sst2End := codec.IndexPrefix(50, 2)

				return []execinfrapb.BulkMergeSpec_SST{
					{StartKey: string(sst1Start), EndKey: string(sst1End)},
					{StartKey: string(sst2Start), EndKey: string(sst2End)},
				}
			}(),
			expectedError: "SST 1 has unsafe start key",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pickSplitsForSpan(tc.span, tc.ssts)
			require.Error(t, err)
			require.Regexp(t, tc.expectedError, err.Error())
		})
	}
}
