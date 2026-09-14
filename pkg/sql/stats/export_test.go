// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

// TestingNumInternalQueries returns the number of times the cache has read
// statistics from the database.
func (sc *TableStatisticsCache) TestingNumInternalQueries() int64 {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.mu.numInternalQueries
}
