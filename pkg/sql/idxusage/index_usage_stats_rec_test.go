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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestGetRecommendationsFromIndexStats(t *testing.T) {

	var currentTime = timeutil.Now()
	var anHourBefore = currentTime.Add(-time.Hour)
	var aMinuteBefore = currentTime.Add(-time.Minute)

	/*
		Test cases:
			- system db name
			- index type primary
			- drop unused rec
			- drop never used rec
			- recently used index
	*/

	testData := []struct {
		idxStats            IndexStatsRow
		dbName              string
		unusedIndexDuration time.Duration
		expectedReturn      []*serverpb.IndexRecommendation
	}{
		// Database name "system", expect no index recommendation.
		{
			idxStats: IndexStatsRow{
				TableID:          1,
				IndexID:          2,
				LastRead:         anHourBefore,
				IndexType:        "secondary",
				CreatedAt:        nil,
				UnusedIndexKnobs: nil,
			},
			dbName:              "system",
			unusedIndexDuration: time.Hour,
			expectedReturn:      []*serverpb.IndexRecommendation{},
		},
		// Index is primary index, expect no index recommendation.
		{
			idxStats: IndexStatsRow{
				TableID:          1,
				IndexID:          2,
				LastRead:         anHourBefore,
				CreatedAt:        nil,
				UnusedIndexKnobs: nil,
				IndexType:        "primary",
			},
			dbName:              "testdb",
			unusedIndexDuration: time.Hour,
			expectedReturn:      []*serverpb.IndexRecommendation{},
		},
		// Index exceeds the unused index duration, expect index recommendation.
		{
			idxStats: IndexStatsRow{
				TableID:          1,
				IndexID:          2,
				LastRead:         anHourBefore,
				IndexType:        "secondary",
				CreatedAt:        nil,
				UnusedIndexKnobs: nil,
			},
			dbName:              "testdb",
			unusedIndexDuration: time.Hour,
			expectedReturn: []*serverpb.IndexRecommendation{
				{
					TableID: 1,
					IndexID: 2,
					Type:    serverpb.IndexRecommendation_DROP_UNUSED,
					Reason:  "This index has not been used in over 1 hours, and can be removed for better write performance.",
				},
			},
		},
		// Index has never been used and has no creation time, expect never used index recommendation.
		{
			idxStats: IndexStatsRow{
				TableID:  1,
				IndexID:  3,
				LastRead: time.Time{},

				IndexType:        "secondary",
				CreatedAt:        nil,
				UnusedIndexKnobs: nil,
			},
			dbName:              "testdb",
			unusedIndexDuration: defaultUnusedIndexDuration,
			expectedReturn: []*serverpb.IndexRecommendation{
				{
					TableID: 1,
					IndexID: 3,
					Type:    serverpb.IndexRecommendation_DROP_UNUSED,
					Reason:  indexNeverUsedReason,
				},
			},
		},
		// Index has been used recently, expect no index recommendations.
		{
			idxStats: IndexStatsRow{
				TableID:          1,
				IndexID:          4,
				LastRead:         aMinuteBefore,
				IndexType:        "secondary",
				CreatedAt:        nil,
				UnusedIndexKnobs: nil,
			},
			dbName:              "testdb",
			unusedIndexDuration: defaultUnusedIndexDuration,
			expectedReturn:      []*serverpb.IndexRecommendation{},
		},
	}

	st := cluster.MakeTestingClusterSettings()
	for _, tc := range testData {
		DropUnusedIndexDuration.Override(context.Background(), &st.SV, tc.unusedIndexDuration)
		actualReturn := tc.idxStats.GetRecommendationsFromIndexStats(tc.dbName, st)
		require.Equal(t, tc.expectedReturn, actualReturn)
	}
}

func TestRecommendDropUnusedIndex(t *testing.T) {

	var currentTime = timeutil.Now()
	var anHourBefore = currentTime.Add(-time.Hour)
	var aMinuteBefore = currentTime.Add(-time.Minute)

	type expectedReturn struct {
		recommendation *serverpb.IndexRecommendation
	}

	indexStatsRow := IndexStatsRow{
		TableID: 1,
		IndexID: 1,
	}

	testData := []struct {
		currentTime         time.Time
		createdAt           *time.Time
		lastRead            time.Time
		unusedIndexDuration time.Duration
		expectedReturn      expectedReturn
	}{
		{
			currentTime:         currentTime,
			createdAt:           nil,
			lastRead:            anHourBefore,
			unusedIndexDuration: time.Hour,
			expectedReturn: expectedReturn{
				recommendation: &serverpb.IndexRecommendation{
					TableID: 1,
					IndexID: 1,
					Type:    serverpb.IndexRecommendation_DROP_UNUSED,
					Reason:  fmt.Sprintf(indexExceedUsageDurationReasonPlaceholder, formatDuration(time.Hour)),
				},
			},
		},
		{
			currentTime:         currentTime,
			createdAt:           nil,
			lastRead:            aMinuteBefore,
			unusedIndexDuration: time.Hour,
			expectedReturn: expectedReturn{
				recommendation: nil,
			},
		},
		{
			currentTime:         currentTime,
			createdAt:           nil,
			lastRead:            time.Time{},
			unusedIndexDuration: time.Hour,
			expectedReturn: expectedReturn{
				recommendation: &serverpb.IndexRecommendation{
					TableID: 1,
					IndexID: 1,
					Type:    serverpb.IndexRecommendation_DROP_UNUSED,
					Reason:  indexNeverUsedReason,
				},
			},
		},
		{
			currentTime:         currentTime,
			createdAt:           &anHourBefore,
			lastRead:            time.Time{},
			unusedIndexDuration: time.Hour,
			expectedReturn: expectedReturn{
				recommendation: &serverpb.IndexRecommendation{
					TableID: 1,
					IndexID: 1,
					Type:    serverpb.IndexRecommendation_DROP_UNUSED,
					Reason:  fmt.Sprintf(indexExceedUsageDurationReasonPlaceholder, formatDuration(time.Hour)),
				},
			},
		},
		{
			currentTime:         currentTime,
			createdAt:           &aMinuteBefore,
			lastRead:            time.Time{},
			unusedIndexDuration: time.Hour,
			expectedReturn: expectedReturn{
				recommendation: nil,
			},
		},
	}

	for _, tc := range testData {
		actualRecommendation := indexStatsRow.recommendDropUnusedIndex(tc.currentTime, tc.createdAt, tc.lastRead, tc.unusedIndexDuration)
		require.Equal(t, tc.expectedReturn.recommendation, actualRecommendation)
	}
}
