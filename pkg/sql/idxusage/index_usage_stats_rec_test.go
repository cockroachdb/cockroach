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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestRecommendDropUnusedIndex(t *testing.T) {

	var currentTime = timeutil.Now()
	var anHourBefore = currentTime.Add(-time.Hour)
	var aMinuteBefore = currentTime.Add(-time.Minute)

	type expectedReturn struct {
		recommendation *roachpb.IndexRecommendation
	}

	stubIndexStatsRow := &serverpb.TableIndexStatsResponse_ExtendedCollectedIndexUsageStatistics{
		Statistics: &roachpb.CollectedIndexUsageStatistics{
			Key: roachpb.IndexUsageKey{
				TableID: 1,
				IndexID: 1,
			},
		},
	}

	indexStatsRow := IndexStatsRow{
		Row: stubIndexStatsRow,
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
				recommendation: &roachpb.IndexRecommendation{
					TableID: 1,
					IndexID: 1,
					Type:    roachpb.IndexRecommendation_DROP_UNUSED,
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
				recommendation: &roachpb.IndexRecommendation{
					TableID: 1,
					IndexID: 1,
					Type:    roachpb.IndexRecommendation_DROP_UNUSED,
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
				recommendation: &roachpb.IndexRecommendation{
					TableID: 1,
					IndexID: 1,
					Type:    roachpb.IndexRecommendation_DROP_UNUSED,
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
