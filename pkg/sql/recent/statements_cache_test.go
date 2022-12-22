// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package recent

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

// TestRecentStatementsCacheBasic tests the operations for creating a new
// StatementsCache, adding statements, and reading from the cache.
func TestRecentStatementsCacheBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := &cluster.Settings{}
	monitor := mon.NewUnlimitedMonitor(ctx, "test", mon.MemoryResource, nil, nil, math.MaxInt64, st)
	statementsCache := NewStatementsCache(st, monitor, time.Now)
	capacity := 4
	timeToLive := 10 * time.Second
	StatementsCacheCapacity.Override(ctx, &st.SV, int64(capacity))
	StatementsCacheTimeToLive.Override(ctx, &st.SV, timeToLive)

	// Add 6 statements with IDs ranging from 0 to 6, inclusive.
	stmtIDs := [7]string{"0", "1", "2", "3", "4", "5", "6"}
	for i := 0; i < 6; i++ {
		err := statementsCache.Add(ctx, serverpb.ActiveQuery{ID: stmtIDs[i]})
		require.NoError(t, err)
	}

	// There have been 6 statements added but the capacity is 4. The statements with
	// IDs 0 and 1 should have been evicted.
	var stmts []serverpb.ActiveQuery
	statementsCache.IterateRecentStatements(ctx, func(ctx context.Context, stmt *serverpb.ActiveQuery) {
		stmts = append(stmts, *stmt)
	})
	assertIDs(t, stmts, []string{"5", "4", "3", "2"})

	// Wait for a period longer than the time to live.
	statementsCache.timeSrc = addSeconds(statementsCache.timeSrc, 12)

	err := statementsCache.Add(ctx, serverpb.ActiveQuery{ID: stmtIDs[6]})
	if err != nil {
		return
	}
	var stmtsTwo []serverpb.ActiveQuery
	statementsCache.IterateRecentStatements(ctx, func(ctx context.Context, stmt *serverpb.ActiveQuery) {
		stmtsTwo = append(stmtsTwo, *stmt)
	})
	// The only statement in the cache should be the most recently added one,
	// since the others have reached the TTL.
	assertIDs(t, stmtsTwo, []string{"6"})
}

// assertIDs checks that the given ActiveQueries have statement IDs
// that match the expected IDs.
func assertIDs(t *testing.T, queries []serverpb.ActiveQuery, expectedIDs []string) {
	var actualIDs []string
	for _, query := range queries {
		actualIDs = append(actualIDs, query.ID)
	}
	require.ElementsMatch(t, expectedIDs, actualIDs)
}

// addSeconds is used to adjust the cache's perception of time without having
// the CI actually do the waiting.
func addSeconds(curr func() time.Time, seconds int) func() time.Time {
	return func() time.Time {
		return curr().Local().Add(time.Duration(seconds) * time.Second)
	}
}
