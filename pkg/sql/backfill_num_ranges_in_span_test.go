// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestNumRangesInSpanContainedBy tests the function with that name.
func TestNumRangesInSpanContainedBy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// In each test case, we'll split off ranges in the outer span
	// at each of the split point suffixes. We'll then verify that
	// the numRangesInSpanContainedBy returns the expected value for
	// all of the subTests which state a set of spans to query.
	type span [2]string
	type subTest struct {
		subSpans  []span
		contained int
	}
	type testCase struct {
		outer    span
		splits   []string
		total    int
		subtests []subTest
	}
	testCases := []testCase{
		{
			outer:  span{"", ""},
			splits: []string{"a", "b", "c"},
			total:  4,
			subtests: []subTest{
				{
					subSpans:  []span{{"aa", "bb"}},
					contained: 0,
				},
				{
					subSpans:  []span{{"aa", "cc"}},
					contained: 1,
				},
				{
					subSpans:  []span{{"a", "cc"}},
					contained: 2,
				},
				{
					subSpans:  []span{{"cc", ""}, {"b", "cc"}},
					contained: 2,
				},
				{
					subSpans:  []span{{"cc", ""}, {"c", "cc"}},
					contained: 1,
				},
				{
					subSpans:  []span{{"c", ""}},
					contained: 1,
				},
				{
					subSpans:  []span{{"", ""}},
					contained: 4,
				},
			},
		},
	}

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	ctx := context.Background()
	defer srv.Stopper().Stop(ctx)

	// Use a tenant-aware scratch key so this test works in multitenant mode.
	// The key is prefixed with the tenant prefix so range iteration stays
	// within the tenant's keyspace.
	ts := srv.ApplicationLayer()
	kvDB := ts.DB()
	scratchKey := append(ts.Codec().TenantPrefix(), keys.ScratchRangeMin...)
	scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
	_, _, err := srv.SplitRange(scratchKey)
	require.NoError(t, err)
	mkKey := func(prefix roachpb.Key, k string) roachpb.Key {
		return append(prefix[:len(prefix):len(prefix)], k...)
	}
	mkEndKey := func(prefix roachpb.Key, k string) roachpb.Key {
		if k == "" {
			return prefix.PrefixEnd()
		}
		return mkKey(prefix, k)
	}
	mkSpan := func(prefix roachpb.Key, sp span) roachpb.Span {
		return roachpb.Span{
			Key:    mkKey(prefix, sp[0]),
			EndKey: mkEndKey(prefix, sp[1]),
		}
	}
	dsp := ts.ExecutorConfig().(sql.ExecutorConfig).DistSQLPlanner
	spanString := func(sp span) string {
		return sp[0] + "-" + sp[1]
	}
	spanStrings := func(spans []span) string {
		var strs []string
		for _, sp := range spans {
			strs = append(strs, spanString(sp))
		}
		return fmt.Sprintf("{%s}", strings.Join(strs, ","))
	}
	run := func(t *testing.T, c testCase) {
		prefix := encoding.EncodeStringAscending(scratchKey, t.Name())
		for _, split := range c.splits {
			_, _, err = srv.SplitRange(mkKey(prefix, split))
			require.NoError(t, err)
		}
		outerSpan := mkSpan(prefix, c.outer)
		// Do a scan to populate the tenant's range cache with the new range
		// descriptors after splitting.
		_, err = kvDB.Scan(ctx, outerSpan.Key, outerSpan.EndKey, 0)
		require.NoError(t, err)
		for _, sc := range c.subtests {
			t.Run(spanStrings(sc.subSpans), func(t *testing.T) {
				var spans []roachpb.Span
				for _, sp := range sc.subSpans {
					spans = append(spans, mkSpan(prefix, sp))
				}
				total, contained, err := sql.NumRangesInSpanContainedBy(ctx, kvDB, dsp, outerSpan, spans)
				require.NoError(t, err)
				require.Equal(t, c.total, total)
				require.Equal(t, sc.contained, contained)
			})
		}
	}

	for _, c := range testCases {
		t.Run(
			fmt.Sprintf(
				"%s@{%s}",
				spanString(c.outer), strings.Join(c.splits, ","),
			),
			func(t *testing.T) { run(t, c) })
	}
}
