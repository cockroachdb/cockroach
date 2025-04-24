// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobspb_test

import (
	"maps"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestTimestampSpansMap(t *testing.T) {
	type expected struct {
		minTimestamp   hlc.Timestamp
		timestampCount int
		spanCount      int
	}

	for name, tc := range map[string]struct {
		m        map[hlc.Timestamp]roachpb.Spans
		expected expected
	}{
		"empty map": {
			m: map[hlc.Timestamp]roachpb.Spans{},
			expected: expected{
				minTimestamp:   hlc.Timestamp{},
				timestampCount: 0,
				spanCount:      0,
			},
		},
		"map with single timestamp": {
			m: map[hlc.Timestamp]roachpb.Spans{
				{WallTime: 1}: {
					{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
			},
			expected: expected{
				minTimestamp:   hlc.Timestamp{WallTime: 1},
				timestampCount: 1,
				spanCount:      2,
			},
		},
		"map with multiple timestamps": {
			m: map[hlc.Timestamp]roachpb.Spans{
				{WallTime: 1}: {
					{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
				},
				{WallTime: 2}: {
					{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
				},
			},
			expected: expected{
				minTimestamp:   hlc.Timestamp{WallTime: 1},
				timestampCount: 2,
				spanCount:      3,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := jobspb.NewTimestampSpansMap(tc.m)
			require.Equal(t, tc.m, maps.Collect(m.All()))
			require.Equal(t, tc.expected.minTimestamp, m.MinTimestamp())
			require.Equal(t, tc.expected.timestampCount, m.TimestampCount())
			require.Equal(t, tc.expected.spanCount, m.SpanCount())
			require.True(t, m.Equal(jobspb.NewTimestampSpansMap(tc.m)))
			require.Equal(t, len(tc.m) == 0, m.IsEmpty())
		})
	}
}
