// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestTickEndFor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const tickWidth = 10 * time.Second
	tests := []struct {
		name     string
		ts       hlc.Timestamp
		expected hlc.Timestamp
	}{
		{
			name:     "exactly on boundary",
			ts:       hlc.Timestamp{WallTime: 100 * int64(time.Second)},
			expected: hlc.Timestamp{WallTime: 100 * int64(time.Second)},
		},
		{
			name:     "on boundary with logical",
			ts:       hlc.Timestamp{WallTime: 100 * int64(time.Second), Logical: 1},
			expected: hlc.Timestamp{WallTime: 110 * int64(time.Second)},
		},
		{
			name:     "just after boundary",
			ts:       hlc.Timestamp{WallTime: 100*int64(time.Second) + 1},
			expected: hlc.Timestamp{WallTime: 110 * int64(time.Second)},
		},
		{
			name:     "just before boundary",
			ts:       hlc.Timestamp{WallTime: 110*int64(time.Second) - 1},
			expected: hlc.Timestamp{WallTime: 110 * int64(time.Second)},
		},
		{
			name:     "mid tick",
			ts:       hlc.Timestamp{WallTime: 105 * int64(time.Second)},
			expected: hlc.Timestamp{WallTime: 110 * int64(time.Second)},
		},
		{
			name:     "zero timestamp",
			ts:       hlc.Timestamp{},
			expected: hlc.Timestamp{},
		},
		{
			name:     "one nanosecond",
			ts:       hlc.Timestamp{WallTime: 1},
			expected: hlc.Timestamp{WallTime: 10 * int64(time.Second)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tickEndFor(tc.ts, tickWidth)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestNextTickEndAfter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const tickWidth = 10 * time.Second
	tests := []struct {
		name     string
		prev     hlc.Timestamp
		expected hlc.Timestamp
	}{
		{
			name:     "on boundary",
			prev:     hlc.Timestamp{WallTime: 100 * int64(time.Second)},
			expected: hlc.Timestamp{WallTime: 110 * int64(time.Second)},
		},
		{
			name:     "on boundary with logical",
			prev:     hlc.Timestamp{WallTime: 100 * int64(time.Second), Logical: 5},
			expected: hlc.Timestamp{WallTime: 110 * int64(time.Second)},
		},
		{
			name:     "mid tick",
			prev:     hlc.Timestamp{WallTime: 105 * int64(time.Second)},
			expected: hlc.Timestamp{WallTime: 110 * int64(time.Second)},
		},
		{
			name:     "zero",
			prev:     hlc.Timestamp{},
			expected: hlc.Timestamp{WallTime: 10 * int64(time.Second)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := nextTickEndAfter(tc.prev, tickWidth)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestNewProducerValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("empty spans", func(t *testing.T) {
		_, err := NewProducer(nil, nil, hlc.Timestamp{}, time.Second, nil, nil,
			ResumeState{}, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "at least one span")
	})

	t.Run("zero tick width", func(t *testing.T) {
		_, err := NewProducer(nil, testSpans(), hlc.Timestamp{}, 0, nil, nil,
			ResumeState{}, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "tickWidth must be positive")
	})

	t.Run("nil sink", func(t *testing.T) {
		_, err := NewProducer(nil, testSpans(), hlc.Timestamp{}, time.Second,
			&testFileIDs{}, nil, ResumeState{}, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "sink must be non-nil")
	})

	t.Run("negative forward threshold", func(t *testing.T) {
		_, err := NewProducer(nil, testSpans(), hlc.Timestamp{}, time.Second,
			&testFileIDs{}, &noopSink{}, ResumeState{}, -1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "forwardThreshold must be non-negative")
	})
}

// helpers for internal tests

type testFileIDs struct{ n int64 }

func (t *testFileIDs) Next() int64 { t.n++; return t.n }

type noopSink struct{}

func (noopSink) Flush(context.Context, *revlogpb.Flush) error { return nil }

func testSpans() []roachpb.Span {
	return []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}}
}
