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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"time"
)

type IndexStatsRow serverpb.TableIndexStatsResponse_ExtendedCollectedIndexUsageStatistics

// defaultUnusedIndexDuration is a week.
const defaultUnusedIndexDuration = 7 * 24 * time.Hour

var dropUnusedIndexDuration = settings.RegisterDurationSetting(
	settings.TenantReadOnly,
	"sql.index_recommendation.drop_unused_duration",
	"the index disuse duration at which we begin to recommend dropping the index.",
	defaultUnusedIndexDuration,
	settings.NonNegativeDuration,
)

const indexExceedUsageDurationReason = "This index has not been used in over 1 week and can be removed for better write performance."
const indexNeverUsedReason = "This index has not been used in over 1 week and can be removed for better write performance."

func (i IndexStatsRow) AddRecommendationsForIndex(st *cluster.Settings) {
	i.maybeAddUnusedIndexRecommendation(dropUnusedIndexDuration.Get(&st.SV))
}

func (i IndexStatsRow) maybeAddUnusedIndexRecommendation(unusedIndexDuration time.Duration) {
	shouldDrop, rec := recommendDropUnusedIndex(timeutil.Now(), i.CreatedAt, i.Statistics.Stats.LastRead, unusedIndexDuration)
	if shouldDrop {
		i.Statistics.Recommendations = append(i.Statistics.Recommendations, rec)
	}
}

// recommendDropUnusedIndex checks whether the last usage of an index
// qualifies the index as unused, if so returns an index recommendation.
func recommendDropUnusedIndex(currentTime time.Time, createdAt *time.Time,
	lastRead time.Time, unusedIndexDuration time.Duration,
) (bool, roachpb.ExistingIndexRecommendation) {
	// Index has never been read.
	if lastRead.Equal(time.Time{}) {
		// If instead we have the time at which the index was created.
		if createdAt != nil {
			// Check if the amount of time since the index has been created exceeds
			// the unused index duration.
			if currentTime.Sub(*createdAt) >= unusedIndexDuration {
				return true, roachpb.ExistingIndexRecommendation{
					Type:   roachpb.ExistingIndexRecommendation_DROP_UNUSED,
					Reason: indexExceedUsageDurationReason,
				}
			} else {
				// If it does not exceed the index duration, don't recommend dropping
				// the index.
				return false, roachpb.ExistingIndexRecommendation{}
			}
		}
		// We do not have the creation time and index has never been read. Recommend
		// dropping with a "never used" reason.
		return true, roachpb.ExistingIndexRecommendation{
			Type:   roachpb.ExistingIndexRecommendation_DROP_UNUSED,
			Reason: indexNeverUsedReason,
		}
	}
	// Last usage of the index exceeds the unused index duration.
	if currentTime.Sub(lastRead) >= unusedIndexDuration {
		return true, roachpb.ExistingIndexRecommendation{
			Type:   roachpb.ExistingIndexRecommendation_DROP_UNUSED,
			Reason: indexExceedUsageDurationReason,
		}
	}
	return false, roachpb.ExistingIndexRecommendation{}
}
