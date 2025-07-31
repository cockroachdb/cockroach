// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package idxusage

import (
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// IndexStatsRow is a wrapper type around
// serverpb.TableIndexStatsResponse_ExtendedCollectedIndexUsageStatistics that
// implements additional methods to support unused index recommendations and
// hold testing knobs.
type IndexStatsRow struct {
	TableID          roachpb.TableID
	IndexID          roachpb.IndexID
	CreatedAt        *time.Time
	LastRead         time.Time
	IndexType        string
	IsUnique         bool
	UnusedIndexKnobs *UnusedIndexRecommendationTestingKnobs
}

// defaultUnusedIndexDuration is a week.
const defaultUnusedIndexDuration = 7 * 24 * time.Hour

// DropUnusedIndexDuration registers the index unuse duration at which we
// begin to recommend dropping the index.
var DropUnusedIndexDuration = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.index_recommendation.drop_unused_duration",
	"the index unused duration at which we begin to recommend dropping the index",
	defaultUnusedIndexDuration,
	settings.NonNegativeDuration,
	settings.WithPublic,
)

const indexExceedUsageDurationReasonPlaceholder = "This index has not been used in over %sand can be removed for better write performance."
const indexNeverUsedReason = "This index has not been used and can be removed for better write performance."

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
	dbName string, st *cluster.Settings,
) []*serverpb.IndexRecommendation {
	var recommendations = []*serverpb.IndexRecommendation{}
	// Omit fetching index recommendations for the 'system' database.
	if dbName == catconstants.SystemDatabaseName {
		return recommendations
	}
	rec := i.maybeAddUnusedIndexRecommendation(DropUnusedIndexDuration.Get(&st.SV))
	if rec != nil {
		recommendations = append(recommendations, rec)
	}
	return recommendations
}

func (i IndexStatsRow) maybeAddUnusedIndexRecommendation(
	unusedIndexDuration time.Duration,
) *serverpb.IndexRecommendation {
	if i.IsUnique {
		return nil
	}

	var rec *serverpb.IndexRecommendation

	if i.UnusedIndexKnobs == nil {
		rec = i.recommendDropUnusedIndex(timeutil.Now(), i.CreatedAt,
			i.LastRead, unusedIndexDuration)
	} else {
		rec = i.recommendDropUnusedIndex(i.UnusedIndexKnobs.GetCurrentTime(),
			i.UnusedIndexKnobs.GetCreatedAt(), i.UnusedIndexKnobs.GetLastRead(), unusedIndexDuration)
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
) *serverpb.IndexRecommendation {
	lastActive := lastRead
	if lastActive.Equal(time.Time{}) && createdAt != nil {
		lastActive = *createdAt
	}
	// If we do not have the creation time and index has never been read. Recommend
	// dropping with a "never used" reason.
	if lastActive.Equal(time.Time{}) {
		return &serverpb.IndexRecommendation{
			TableID: i.TableID,
			IndexID: i.IndexID,
			Type:    serverpb.IndexRecommendation_DROP_UNUSED,
			Reason:  indexNeverUsedReason,
		}
	}
	// Last usage of the index exceeds the unused index duration.
	if currentTime.Sub(lastActive) >= unusedIndexDuration {
		return &serverpb.IndexRecommendation{
			TableID: i.TableID,
			IndexID: i.IndexID,
			Type:    serverpb.IndexRecommendation_DROP_UNUSED,
			Reason:  fmt.Sprintf(indexExceedUsageDurationReasonPlaceholder, formatDuration(unusedIndexDuration)),
		}
	}
	return nil
}

func formatDuration(d time.Duration) string {
	const numHoursInDay = 24
	const numMinutesInHour = 60
	const numSecondsInMinute = 60

	days := int64(d.Hours()) / (numHoursInDay)
	hours := int64(math.Floor(d.Hours())) % numHoursInDay
	minutes := int64(math.Floor(d.Minutes())) % numMinutesInHour
	seconds := int64(math.Floor(d.Seconds())) % numSecondsInMinute

	var daysSubstring string
	var hoursSubstring string
	var minutesSubstring string
	var secondsSubstring string

	if days > 0 {
		daysSubstring = fmt.Sprintf("%d days, ", days)
	}
	if hours > 0 {
		hoursSubstring = fmt.Sprintf("%d hours, ", hours)
	}
	if minutes > 0 {
		minutesSubstring = fmt.Sprintf("%d minutes, ", minutes)
	}
	if seconds > 0 {
		secondsSubstring = fmt.Sprintf("%d seconds, ", seconds)
	}

	return fmt.Sprintf("%s%s%s%s", daysSubstring, hoursSubstring, minutesSubstring, secondsSubstring)
}
