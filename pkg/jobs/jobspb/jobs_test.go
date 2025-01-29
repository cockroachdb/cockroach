// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobspb_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestTimestampSpansMapRoundTrip(t *testing.T) {
	ts := func(wt int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wt}
	}

	span := func(start, end string) roachpb.Span {
		return roachpb.Span{
			Key:    roachpb.Key(start),
			EndKey: roachpb.Key(end),
		}
	}

	for name, input := range map[string]map[hlc.Timestamp]roachpb.Spans{
		"nil map": nil,
		"map with one timestamp": {
			ts(1): {span("a", "b")},
		},
		"map with multiple timestamps": {
			ts(1): {span("a", "b"), span("c", "d")},
			ts(2): {span("b", "c")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			tsm := jobspb.NewTimestampSpansMap(input)
			output := tsm.ToGoMap()
			require.Equal(t, input, output)
		})
	}
}
