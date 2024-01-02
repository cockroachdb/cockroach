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
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/indexrec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type indexRecKey struct {
	stmtNoConstants string
	database        string
	planHash        uint64
}

type indexRecInfo struct {
	lastGeneratedTs time.Time
	recommendations []indexrec.Rec
	executionCount  int64
}

// IndexRecCache stores the map of index recommendations keys (fingerprint, database, planHash) and
// information (lastGeneratedTs, recommendations, executionCount).
type IndexRecCache struct {
	st *cluster.Settings

	// minExecCount is the minimum value for execution count that a statement
	// must have before a recommendation is generated.
	minExecCount int64

	mu struct {
		syncutil.RWMutex

		// idxRecommendations stores index recommendations per indexRecKey.
		idxRecommendations map[indexRecKey]indexRecInfo

		// lastCleanupTs has the last time we cleaned up the cache.
		lastCleanupTs time.Time
	}
}

const timeBetweenCleanups = 5 * time.Minute
const timeThresholdForDeletion = 24 * time.Hour

// NewIndexRecommendationsCache creates a new map to be used as a cache for index recommendations.
func NewIndexRecommendationsCache(setting *cluster.Settings) *IndexRecCache {
	idxRecCache := &IndexRecCache{
		st:           setting,
		minExecCount: 5,
	}
	idxRecCache.mu.idxRecommendations = make(map[indexRecKey]indexRecInfo)
	idxRecCache.mu.lastCleanupTs = time.Time{}
	return idxRecCache
}

// ShouldGenerateIndexRecommendation implements IdxRecommendations interface.
// It returns true if there was no generation in the past hour
// and there is at least 5 executions of the same fingerprint/database/planHash combination.
func (idxRec *IndexRecCache) ShouldGenerateIndexRecommendation(
	fingerprint string,
	planHash uint64,
	database string,
	stmtType tree.StatementType,
	isInternal bool,
) bool {
	if !idxRec.statementCanHaveRecommendation(stmtType, isInternal) {
		return false
	}

	idxKey := indexRecKey{
		stmtNoConstants: fingerprint,
		database:        database,
		planHash:        planHash,
	}
	recInfo, found := idxRec.getOrCreateIndexRecommendation(idxKey)
	// If we couldn't find or create, don't generate recommendations.
	if !found {
		return false
	}

	timeSinceLastGenerated := timeutil.Since(recInfo.lastGeneratedTs)
	return recInfo.executionCount >= idxRec.minExecCount && timeSinceLastGenerated.Hours() >= 1
}

// UpdateIndexRecommendations  implements IdxRecommendations interface.
// It updates the values for index recommendations.
// If reset is true, a new recommendation was generated, so reset the execution counter and
// lastGeneratedTs, otherwise just increment the executionCount.
func (idxRec *IndexRecCache) UpdateIndexRecommendations(
	fingerprint string,
	planHash uint64,
	database string,
	stmtType tree.StatementType,
	isInternal bool,
	recommendations []indexrec.Rec,
	reset bool,
) []indexrec.Rec {
	if !idxRec.statementCanHaveRecommendation(stmtType, isInternal) {
		return recommendations
	}

	idxKey := indexRecKey{
		stmtNoConstants: fingerprint,
		database:        database,
		planHash:        planHash,
	}

	if reset {
		idxRec.setIndexRecommendations(idxKey, timeutil.Now(), recommendations, 0)
		return recommendations
	}

	recInfo, found := idxRec.getOrCreateIndexRecommendation(idxKey)
	if !found {
		return recommendations
	}

	if recInfo.executionCount < idxRec.minExecCount {
		idxRec.setIndexRecommendations(
			idxKey,
			recInfo.lastGeneratedTs,
			recInfo.recommendations,
			recInfo.executionCount+1,
		)
	}

	return recInfo.recommendations
}

// statementCanHaveRecommendation returns true if that type of statement can have recommendations
// generated for it. We only want to recommend if the statement is DML, recommendations are enabled and
// is not internal.
func (idxRec *IndexRecCache) statementCanHaveRecommendation(
	stmtType tree.StatementType, isInternal bool,
) bool {
	if !sqlstats.SampleIndexRecommendation.Get(&idxRec.st.SV) || stmtType != tree.TypeDML || isInternal {
		return false
	}

	return true
}

func (idxRec *IndexRecCache) getIndexRecommendationAndInfo(
	key indexRecKey,
) (indexRecInfo, bool, int, time.Time) {
	idxRec.mu.RLock()
	defer idxRec.mu.RUnlock()
	recInfo, found := idxRec.mu.idxRecommendations[key]
	return recInfo, found, len(idxRec.mu.idxRecommendations), idxRec.mu.lastCleanupTs
}

func (idxRec *IndexRecCache) getOrCreateIndexRecommendation(key indexRecKey) (indexRecInfo, bool) {
	recInfo, found, cacheSize, lastCleanupTs := idxRec.getIndexRecommendationAndInfo(key)
	if found {
		return recInfo, true
	}

	// Get the cluster setting value of the limit on number of unique index
	// recommendations info we can store in memory.
	limit := sqlstats.MaxMemReportedSampleIndexRecommendations.Get(&idxRec.st.SV)

	// Check if the limit was reached and if we can do cleanup (in case it was reached).
	if int64(cacheSize) >= limit && timeutil.Since(lastCleanupTs) < timeBetweenCleanups {
		return indexRecInfo{}, false
	}

	idxRec.mu.Lock()
	defer idxRec.mu.Unlock()
	// Confirm no entry was created in another thread between the check above
	// and the new creation.
	recInfo, found = idxRec.mu.idxRecommendations[key]
	if found {
		return recInfo, true
	}

	// If it was not found, check if a new entry can be created, without
	// passing the limit of unique index recommendations from the cache.
	// Calculate the size again, because it could have been updated by another thread.
	if int64(len(idxRec.mu.idxRecommendations)) >= limit {
		timeNow := timeutil.Now()
		// Check if has been at least 5min since last cleanup, to avoid
		// lock contention when we reached the limit.
		if timeNow.Sub(idxRec.mu.lastCleanupTs) < timeBetweenCleanups {
			return indexRecInfo{}, false
		}

		// Clear entries that were last updated more than a day ago.
		for idxKey, value := range idxRec.mu.idxRecommendations {
			if timeNow.Sub(value.lastGeneratedTs) >= timeThresholdForDeletion {
				delete(idxRec.mu.idxRecommendations, idxKey)
			}
		}
		idxRec.mu.lastCleanupTs = timeNow

		if int64(len(idxRec.mu.idxRecommendations)) >= limit {
			return indexRecInfo{}, false
		}
	}

	// For a new entry, we want the lastGeneratedTs to be in the past, in case we reach
	// the execution count, we should generate new recommendations.
	recInfo = indexRecInfo{
		lastGeneratedTs: timeutil.Now().Add(-time.Hour),
		recommendations: nil,
		executionCount:  0,
	}
	idxRec.mu.idxRecommendations[key] = recInfo

	return recInfo, true
}

func (idxRec *IndexRecCache) setIndexRecommendations(
	key indexRecKey, time time.Time, recommendations []indexrec.Rec, execCount int64,
) {
	_, found := idxRec.getOrCreateIndexRecommendation(key)

	if found {
		idxRec.mu.Lock()
		defer idxRec.mu.Unlock()

		idxRec.mu.idxRecommendations[key] = indexRecInfo{
			lastGeneratedTs: time,
			recommendations: recommendations,
			executionCount:  execCount,
		}
	}
}
