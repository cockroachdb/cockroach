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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/stretchr/testify/require"
)

// TestRecentStatementsCacheBasic tests the operations for creating a new
// StatementsCache, adding statements to one session, and reading
// from the cache.
func TestRecentStatementsCacheBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := &cluster.Settings{}
	cache := NewStatementsCache(st, time.Now)
	capacity := 4
	timeToLive := 10
	StatementsCacheCapacity.Override(ctx, &st.SV, int64(capacity))
	StatementsCacheTimeToLive.Override(ctx, &st.SV, int64(timeToLive))
	sessionID := intToClusteruniqueID(0)

	if len(cache.GetRecentStatementsForSession(sessionID)) != 0 {
		t.Errorf("Expected capacity is: %d. Got capacity of %d\n", 0, len(cache.GetRecentStatementsForSession(sessionID)))
	}

	// Add 6 statements with IDs ranging from 0 to 5, inclusive. These statements will
	// be part of the same session ID.
	stmtIDs := [6]string{"0", "1", "2", "3", "4", "5"}
	for i := 0; i < 6; i++ {
		cache.Add(sessionID, serverpb.ActiveQuery{ID: stmtIDs[i]})
	}

	// There have been 6 statements added but the capacity is 4. The statements with
	// IDs 0 and 1 should have been evicted.
	activeQueries := cache.GetRecentStatementsForSession(sessionID)
	assertStatementIDs(t, activeQueries, []string{"5", "4", "3", "2"})

	// Wait for a period longer than the time to live. All the statements
	// should be evicted after this period.
	cache.timeSrc = addSeconds(cache.timeSrc, timeToLive*2)
	if len(cache.GetRecentStatementsForSession(sessionID)) != 0 {
		t.Errorf("Expected capacity is: %d. Got capacity of %d\n", 0, len(cache.GetRecentStatementsForSession(sessionID)))
	}
}

// TestRecentStatementsCacheMultipleSessions tests the same as above, except
// adds the statements to different sessions in order to test reading
// and filtering by Session ID.
func TestRecentStatementsCacheMultipleSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := &cluster.Settings{}
	cache := NewStatementsCache(st, time.Now)
	capacity := 4
	StatementsCacheCapacity.Override(ctx, &st.SV, int64(capacity))

	// Add 6 statements with IDs ranging from 0 to 5, inclusive. These statements will
	// be part of two different sessions.
	sessionIDOne := intToClusteruniqueID(0)
	sessionIDTwo := intToClusteruniqueID(1)

	stmtIDs := [6]string{"0", "1", "2", "3", "4", "5"}
	for i := 0; i < 6; i += 2 {
		cache.Add(sessionIDOne, serverpb.ActiveQuery{ID: stmtIDs[i]})
		cache.Add(sessionIDTwo, serverpb.ActiveQuery{ID: stmtIDs[i+1]})
	}

	// There have been 6 statements added but the capacity is 4. The statements with
	// IDs 0 and 1 should have been evicted.
	// The first session should have statements with IDs 2 and 4.
	activeQueriesOne := cache.GetRecentStatementsForSession(sessionIDOne)
	assertStatementIDs(t, activeQueriesOne, []string{"4", "2"})

	// The second session should have statements with IDs 3 and 5.
	activeQueriesTwo := cache.GetRecentStatementsForSession(sessionIDTwo)
	assertStatementIDs(t, activeQueriesTwo, []string{"5", "3"})
}

// TestRecentStatementsCacheTimeToLive tests that statements are
// correctly evicted after the specified time to live.
func TestRecentStatementsCacheTimeToLive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := &cluster.Settings{}
	cache := NewStatementsCache(st, time.Now)
	capacity := 4
	timeToLive := 10
	StatementsCacheCapacity.Override(ctx, &st.SV, int64(capacity))
	StatementsCacheTimeToLive.Override(ctx, &st.SV, int64(timeToLive))
	sessionID := intToClusteruniqueID(0)
	stmtIDs := [6]string{"0", "1"}

	// Add a statement, wait 7 seconds, add another statement, and wait
	// another 6 seconds. The first statement should be evicted, leaving
	// only the second statement in the cache.
	cache.Add(sessionID, serverpb.ActiveQuery{ID: stmtIDs[0]})
	cache.timeSrc = addSeconds(cache.timeSrc, 7)
	cache.Add(sessionID, serverpb.ActiveQuery{ID: stmtIDs[1]})
	cache.timeSrc = addSeconds(cache.timeSrc, 6)

	activeQueries := cache.GetRecentStatementsForSession(sessionID)
	assertStatementIDs(t, activeQueries, []string{"1"})
}

// intToClusteruniqueID converts from int to clusterunique.ID.
func intToClusteruniqueID(x int) clusterunique.ID {
	val := uint128.FromInts(uint64(x), uint64(x))
	return clusterunique.ID{Uint128: val}
}

// assertStatementIDs checks that the given ActiveQueries have IDs that match
// the expected IDs.
func assertStatementIDs(t *testing.T, queries []serverpb.ActiveQuery, expectedIDs []string) {
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
