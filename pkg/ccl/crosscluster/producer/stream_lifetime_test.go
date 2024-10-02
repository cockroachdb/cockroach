// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRepartition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := func(node, parts, start int) sql.SpanPartition {
		spans := make([]roachpb.Span, parts)
		for i := range spans {
			spans[i].Key = roachpb.Key(fmt.Sprintf("n%d-%d-a", node, i+start))
			spans[i].EndKey = roachpb.Key(fmt.Sprintf("n%d-%d-b", node, i+start))
		}
		return sql.SpanPartition{SQLInstanceID: base.SQLInstanceID(node), Spans: spans}
	}
	for _, parts := range []int{1, 4, 100} {
		for _, input := range [][]sql.SpanPartition{
			{p(1, 43, 0), p(2, 44, 0), p(3, 41, 0)},
			{p(1, 1, 0), p(2, 1, 0), p(3, 1, 0)},
			{p(1, 43, 0), p(2, 44, 0), p(3, 38, 0)},
		} {
			got := repartitionSpans(input, parts)

			var expectedParts int
			var expectedSpans, gotSpans roachpb.Spans
			for _, part := range input {
				expectedParts += min(parts, len(part.Spans))
				expectedSpans = append(expectedSpans, part.Spans...)
			}
			for _, part := range got {
				gotSpans = append(gotSpans, part.Spans...)
			}
			require.Equal(t, expectedParts, len(got))

			// Regardless of how we partitioned, make sure we have all the spans.
			sort.Sort(expectedSpans)
			sort.Sort(gotSpans)
			require.Equal(t, len(expectedSpans), len(gotSpans))
			require.Equal(t, expectedSpans, gotSpans)
		}
	}
}
