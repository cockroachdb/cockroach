// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxusage

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// IndexStatsRow is a wrapper type around
// serverpb.TableIndexStatsResponse_ExtendedCollectedIndexUsageStatistics that
// implements additional methods to support unused index recommendations and
// hold testing knobs.
type IndexStatsRow struct {
	Row              *serverpb.TableIndexStatsResponse_ExtendedCollectedIndexUsageStatistics
	UnusedIndexKnobs *UnusedIndexRecommendationTestingKnobs
}

// defaultUnusedIndexDuration is a week.
const defaultUnusedIndexDuration = 7 * 24 * time.Hour

// DropUnusedIndexDuration registers the index disuse duration at which we
// begin to recommend dropping the index.
var DropUnusedIndexDuration = settings.RegisterDurationSetting(
	settings.TenantReadOnly,
	"sql.index_recommendation.drop_unused_duration",
	"the index disuse duration at which we begin to recommend dropping the index",
	defaultUnusedIndexDuration,
	settings.NonNegativeDuration,
)

const indexExceedUsageDurationReason = "This index has not been used in over 1 week and can be removed for better write performance."
const indexNeverUsedReason = "This index has not been used in over 1 week and can be removed for better write performance."

// UnusedIndexRecommendationTestingKnobs provides hooks and knobs for unit tests.
type UnusedIndexRecommendationTestingKnobs struct {
	// GetCreatedAt allows tests to override the creation time of the index.
	GetCreatedAt func() *time.Time
	// GetLastRead allows tests to override the time the index was last read.
	GetLastRead func() time.Time
	// GetCurrentTime allows tests to override the current time.
	GetCurrentTime func() time.Time
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*UnusedIndexRecommendationTestingKnobs) ModuleTestingKnobs() {}

// GetRecommendationsFromIndexStats gets index recommendations from the given index
// if applicable.
func (i IndexStatsRow) GetRecommendationsFromIndexStats(
	st *cluster.Settings,
) []*roachpb.IndexRecommendation {
	var recommendations []*roachpb.IndexRecommendation
	rec := i.maybeAddUnusedIndexRecommendation(DropUnusedIndexDuration.Get(&st.SV))
	if rec != nil {
		recommendations = append(recommendations, rec)
	}
	return recommendations
}

func (i IndexStatsRow) maybeAddUnusedIndexRecommendation(
	unusedIndexDuration time.Duration,
) *roachpb.IndexRecommendation {
	var rec *roachpb.IndexRecommendation

	if i.UnusedIndexKnobs == nil {
		rec = i.recommendDropUnusedIndex(timeutil.Now(), i.Row.CreatedAt, i.Row.Statistics.Stats.LastRead, unusedIndexDuration)
	} else {
		rec = i.recommendDropUnusedIndex(i.UnusedIndexKnobs.GetCurrentTime(), i.UnusedIndexKnobs.GetCreatedAt(), i.UnusedIndexKnobs.GetLastRead(), unusedIndexDuration)
	}
	return rec
}

// recommendDropUnusedIndex checks whether the last usage of an index
// qualifies the index as unused, if so returns an index recommendation.
func (i IndexStatsRow) recommendDropUnusedIndex(
	currentTime time.Time,
	createdAt *time.Time,
	lastRead time.Time,
	unusedIndexDuration time.Duration,
) *roachpb.IndexRecommendation {
	// Index has never been read.
	if lastRead.Equal(time.Time{}) {
		// If instead we have the time at which the index was created.
		if createdAt != nil {
			// Check if the amount of time since the index has been created exceeds
			// the unused index duration.
			if currentTime.Sub(*createdAt) >= unusedIndexDuration {
				return &roachpb.IndexRecommendation{
					TableID: i.Row.Statistics.Key.TableID,
					IndexID: i.Row.Statistics.Key.IndexID,
					Type:    roachpb.IndexRecommendation_DROP_UNUSED,
					Reason:  indexExceedUsageDurationReason,
				}
			}
			// If it does not exceed the index duration, don't recommend dropping
			// the index.
			return nil
		}
		// We do not have the creation time and index has never been read. Recommend
		// dropping with a "never used" reason.
		return &roachpb.IndexRecommendation{
			TableID: i.Row.Statistics.Key.TableID,
			IndexID: i.Row.Statistics.Key.IndexID,
			Type:    roachpb.IndexRecommendation_DROP_UNUSED,
			Reason:  indexNeverUsedReason,
		}
	}
	// Last usage of the index exceeds the unused index duration.
	if currentTime.Sub(lastRead) >= unusedIndexDuration {
		return &roachpb.IndexRecommendation{
			TableID: i.Row.Statistics.Key.TableID,
			IndexID: i.Row.Statistics.Key.IndexID,
			Type:    roachpb.IndexRecommendation_DROP_UNUSED,
			Reason:  indexExceedUsageDurationReason,
		}
	}
	return nil
}
