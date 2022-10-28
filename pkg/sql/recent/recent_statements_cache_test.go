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
	cache := NewStatementsCache(st)
	capacity := 4
	StatementsCacheCapacity.Override(ctx, &st.SV, int64(capacity))

	// Make sure the cache is empty on initialization.
	if cache.len() != 0 {
		t.Errorf("Expected capacity is: %d. Got capacity of %d\n", 0, cache.len())
	}

	// Add 6 statements with IDs ranging from 0 to 5, inclusive. These statements will
	// be part of the same session ID.
	sessionID := intToClusteruniqueID(0)
	stmtIDVals := [6]clusterunique.ID{}

	for i := 0; i < 6; i++ {
		stmtID := intToClusteruniqueID(i)
		stmtIDVals[i] = stmtID
		cache.Add(sessionID, stmtID, serverpb.ActiveQuery{ID: stmtID.String()})
	}

	// There have been 6 statements added but the capacity is 4. The statements with
	// id=0 an id=1 should have been evicted.
	activeQueries := cache.GetRecentStatementsForSession(sessionID)
	assertStatementIDs(t, activeQueries, []int{5, 4, 3, 2})
}

// TestRecentStatementsCacheMultipleSessions tests the same as above, except
// adds the statements to different sessions in order to test reading
// and filtering by Session ID.
func TestRecentStatementsCacheMultipleSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := &cluster.Settings{}
	cache := NewStatementsCache(st)
	capacity := 4
	StatementsCacheCapacity.Override(ctx, &st.SV, int64(capacity))

	// Add 6 statements with IDs ranging from 0 to 5, inclusive. These statements will
	// be part of two different sessions.
	sessionIDOne := intToClusteruniqueID(0)
	sessionIDTwo := intToClusteruniqueID(1)

	stmtIDVals := [6]clusterunique.ID{}
	for i := 0; i < 6; i += 2 {
		stmtIDOne := intToClusteruniqueID(i)
		stmtIDTwo := intToClusteruniqueID(i + 1)
		cache.Add(sessionIDOne, stmtIDOne, serverpb.ActiveQuery{ID: stmtIDOne.String()})
		cache.Add(sessionIDTwo, stmtIDTwo, serverpb.ActiveQuery{ID: stmtIDTwo.String()})
		stmtIDVals[i] = stmtIDOne
		stmtIDVals[i+1] = stmtIDTwo
	}

	// There have been 6 statements added but the capacity is 4. The statements with
	// id=0 an id=1 should have been evicted.
	if cache.len() != capacity {
		t.Errorf("Expected capacity is: %d. Got capacity of %d\n", capacity, cache.len())
	}

	// The first session should have statements with IDs 2 and 4.
	activeQueriesOne := cache.GetRecentStatementsForSession(sessionIDOne)
	assertStatementIDs(t, activeQueriesOne, []int{4, 2})

	// The second session should have statements with IDs 3 and 5.
	activeQueriesTwo := cache.GetRecentStatementsForSession(sessionIDTwo)
	assertStatementIDs(t, activeQueriesTwo, []int{5, 3})
}

// intToClusteruniqueID converts from int to clusterunique.ID.
func intToClusteruniqueID(x int) clusterunique.ID {
	val := uint128.FromInts(uint64(x), uint64(x))
	return clusterunique.ID{Uint128: val}
}

// assertStatementIDs checks that the given ActiveQueries have IDs that match
// the expected IDs.
func assertStatementIDs(t *testing.T, queries []serverpb.ActiveQuery, expectedIDsInt []int) {
	var actualIDs []string
	var expectedIDs []string
	for _, query := range queries {
		actualIDs = append(actualIDs, query.ID)
	}
	for _, id := range expectedIDsInt {
		expectedIDs = append(expectedIDs, intToClusteruniqueID(id).String())
	}
	require.ElementsMatch(t, expectedIDs, actualIDs)
}
