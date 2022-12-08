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

	// Add 6 statements with IDs ranging from 0 to 5, inclusive.
	stmtIDs := [6]string{"0", "1", "2", "3", "4", "5"}
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

	// Wait for a period longer than the time to live. All the statements
	// should be evicted after this period.
	statementsCache.timeSrc = addSeconds(statementsCache.timeSrc, 12)
	statementsCache.IterateRecentStatements(ctx, func(ctx context.Context, stmt *serverpb.ActiveQuery) {
		t.Errorf("There should not be any statements in the cache")
	})
}

// TestRecentStatementsCacheTimeToLive tests that statements are
// correctly evicted after the specified time to live.
func TestRecentStatementsCacheTimeToLive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := &cluster.Settings{}
	monitor := mon.NewUnlimitedMonitor(ctx, "test", mon.MemoryResource, nil, nil, math.MaxInt64, st)
	statementsCache := NewStatementsCache(st, monitor, time.Now)
	capacity := 4
	timeToLive := 10 * time.Second
	StatementsCacheCapacity.Override(ctx, &st.SV, int64(capacity))
	StatementsCacheTimeToLive.Override(ctx, &st.SV, timeToLive)
	stmtIDs := [6]string{"0", "1"}

	// Add a statement, wait 7 seconds, add another statement, and wait
	// another 7 seconds. The first statement should be evicted, leaving
	// only the second statement in the cache.
	err := statementsCache.Add(ctx, serverpb.ActiveQuery{ID: stmtIDs[0]})
	require.NoError(t, err)
	statementsCache.timeSrc = addSeconds(statementsCache.timeSrc, 7)

	err = statementsCache.Add(ctx, serverpb.ActiveQuery{ID: stmtIDs[1]})
	require.NoError(t, err)
	statementsCache.timeSrc = addSeconds(statementsCache.timeSrc, 7)

	var stmts []serverpb.ActiveQuery
	statementsCache.IterateRecentStatements(ctx, func(ctx context.Context, stmt *serverpb.ActiveQuery) {
		stmts = append(stmts, *stmt)
	})
	assertIDs(t, stmts, []string{"1"})
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
