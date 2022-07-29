// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxrecommendations

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// indexRecKey is used to determine if we should generate an index recommendation.
type indexRecKey struct {
	stmtNoConstants string
	database        string
	planHash        uint64
}

type indexRecInfo struct {
	lastGeneratedTs time.Time
	recommendations []string
	executionCount  int64
}

type IndexRecCache struct {
	st *cluster.Settings

	// uniqueIndexRecInfoLimit is the limit on number of unique index
	// recommendations info we can store in memory.
	uniqueIndexRecInfoLimit *settings.IntSetting

	mu struct {
		syncutil.RWMutex

		// idxRecommendations stores index recommendations per indexRecKey.
		idxRecommendations map[indexRecKey]indexRecInfo
	}

	atomic struct {
		// uniqueIndexRecInfo is the number of unique index recommendations info
		// we are storing in memory.
		uniqueIndexRecInfo *int64
	}
}

func NewIndexRecommendationsCache(
	setting *cluster.Settings, uniqueIndexRecInfoLimit *settings.IntSetting,
) *IndexRecCache {
	idxRecCache := &IndexRecCache{
		st:                      setting,
		uniqueIndexRecInfoLimit: uniqueIndexRecInfoLimit,
	}
	idxRecCache.mu.idxRecommendations = make(map[indexRecKey]indexRecInfo)
	zeroVal := int64(0)
	idxRecCache.atomic.uniqueIndexRecInfo = &zeroVal
	return idxRecCache
}

// statementCanHaveRecommendation returns is that type of statement can have recommendations
// generated for it. E.g. PREPARE statement can't execute index recommendations.
func statementCanHaveRecommendation(fingerprint string) bool {
	return strings.HasPrefix(strings.ToUpper(fingerprint), "SELECT ") // TODO (marylia) using for test
	//return !strings.HasPrefix(strings.ToUpper(fingerprint), "PREPARE ")
}

// ShouldGenerateIndexRecommendation returns true if there was no generation in the past hour
// and there is at least 5 executions of the same fingerprint/database/planHash combination.
func (idxRec *IndexRecCache) ShouldGenerateIndexRecommendation(
	fingerprint string, planHash uint64, database string,
) bool {
	if !sqlstats.SampleIndexRecommendation.Get(&idxRec.st.SV) || !statementCanHaveRecommendation(fingerprint) {
		return false
	}

	idxKey := indexRecKey{
		stmtNoConstants: fingerprint,
		database:        database,
		planHash:        planHash,
	}
	recInfo, found := idxRec.getIndexRecommendation(idxKey)
	// If the statement is being executed for the first time,
	// initialize it.
	if !found {
		idxRec.setIndexRecommendations(idxKey, timeutil.Now().Add(-duration.SecsPerDay*1e6), []string{}, 1)
		recInfo, found = idxRec.getIndexRecommendation(idxKey)
		// If couldn't initialized, don't generate recommendations.
		if !found {
			return false
		}
	}

	timeSinceLastGenerated := timeutil.Now().Sub(recInfo.lastGeneratedTs)
	return recInfo.executionCount >= 5 && timeSinceLastGenerated.Hours() >= 1
}

// UpdateIndexRecommendations updates the values for index recommendations.
// If reset is true, a new recommendation was generated, so reset the execution counter and
// lastGeneratedTs, otherwise just increment the executionCount.
func (idxRec *IndexRecCache) UpdateIndexRecommendations(
	fingerprint string, planHash uint64, database string, recommendations []string, reset bool,
) []string {
	idxKey := indexRecKey{
		stmtNoConstants: fingerprint,
		database:        database,
		planHash:        planHash,
	}

	if reset {
		idxRec.setIndexRecommendations(idxKey, timeutil.Now(), recommendations, 0)
		return recommendations
	}

	recInfo, found := idxRec.getIndexRecommendation(idxKey)
	// If is the first time, we want the lastGeneratedTs to be in the past, in case we reach
	// the execution count, we should generate new recommendations.
	generatedTime := timeutil.Now().Add(-duration.SecsPerDay * 1e6)
	count := int64(1)
	if found {
		generatedTime = recInfo.lastGeneratedTs
		recommendations = recInfo.recommendations
		count = recInfo.executionCount + 1
	}
	idxRec.setIndexRecommendations(idxKey, generatedTime, recommendations, count)

	return recInfo.recommendations
}

func (idxRec *IndexRecCache) getIndexRecommendation(key indexRecKey) (indexRecInfo, bool) {
	idxRec.mu.RLock()
	defer idxRec.mu.RUnlock()

	recInfo, found := idxRec.mu.idxRecommendations[key]

	return recInfo, found
}

func (idxRec *IndexRecCache) setIndexRecommendations(
	key indexRecKey, time time.Time, recommendations []string, execCount int64,
) {
	idxRec.mu.RLock()
	defer idxRec.mu.RUnlock()

	_, found := idxRec.getIndexRecommendation(key)

	// If it was not found, check if a new entry can be created, without
	// passing the limit of unique index recommendations from the cache.
	if !found {
		limit := idxRec.uniqueIndexRecInfoLimit.Get(&idxRec.st.SV)
		incrementedCount :=
			atomic.AddInt64(idxRec.atomic.uniqueIndexRecInfo, int64(1))

		// If we have exceeded limit of unique index recommendations try to delete older data.
		if incrementedCount > limit {
			deleted := idxRec.ClearOlderIndexRecommendationsCache()
			// Abort if no entries were deleted.
			if deleted == 0 {
				atomic.AddInt64(idxRec.atomic.uniqueIndexRecInfo, -int64(1))
				return
			}
		}
	}

	idxRec.mu.idxRecommendations[key] = indexRecInfo{
		lastGeneratedTs: time,
		recommendations: recommendations,
		executionCount:  execCount,
	}
}

// ClearOlderIndexRecommendationsCache clear entries that was last updated
// more than a day ago. Returns the total deleted entries.
func (idxRec *IndexRecCache) ClearOlderIndexRecommendationsCache() int {
	idxRec.mu.RLock()
	defer idxRec.mu.RUnlock()

	deleted := 0
	for key, value := range idxRec.mu.idxRecommendations {
		if timeutil.Now().Sub(value.lastGeneratedTs).Hours() >= 24 {
			delete(idxRec.mu.idxRecommendations, key)
			deleted++
		}
	}
	atomic.AddInt64(idxRec.atomic.uniqueIndexRecInfo, int64(-deleted))
	return deleted
}
