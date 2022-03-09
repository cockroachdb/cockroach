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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestRecommendDropUnusedIndex(t *testing.T) {

	var timeStub = timeutil.Now()
	var createdAnHourAgo = timeStub.Add(-time.Hour)
	var createdAMinuteAgo = timeStub.Add(-time.Minute)

	type expectedReturn struct {
		shouldDrop     bool
		recommendation roachpb.ExistingIndexRecommendation
	}

	testData := []struct {
		currentTime         time.Time
		createdAt           *time.Time
		lastRead            time.Time
		unusedIndexDuration time.Duration
		expectedReturn      expectedReturn
	}{
		{
			currentTime:         timeStub,
			createdAt:           nil,
			lastRead:            timeStub.Add(-time.Hour),
			unusedIndexDuration: time.Hour,
			expectedReturn: expectedReturn{
				shouldDrop: true,
				recommendation: roachpb.ExistingIndexRecommendation{
					Type:   roachpb.ExistingIndexRecommendation_DROP_UNUSED,
					Reason: indexExceedUsageDurationReason,
				},
			},
		},
		{
			currentTime:         timeStub,
			createdAt:           nil,
			lastRead:            time.Time{},
			unusedIndexDuration: time.Hour,
			expectedReturn: expectedReturn{
				shouldDrop: true,
				recommendation: roachpb.ExistingIndexRecommendation{
					Type:   roachpb.ExistingIndexRecommendation_DROP_UNUSED,
					Reason: indexNeverUsedReason,
				},
			},
		},
		{
			currentTime:         timeStub,
			createdAt:           &createdAnHourAgo,
			lastRead:            time.Time{},
			unusedIndexDuration: time.Hour,
			expectedReturn: expectedReturn{
				shouldDrop: true,
				recommendation: roachpb.ExistingIndexRecommendation{
					Type:   roachpb.ExistingIndexRecommendation_DROP_UNUSED,
					Reason: indexExceedUsageDurationReason,
				},
			},
		},
		{
			currentTime:         timeStub,
			createdAt:           &createdAMinuteAgo,
			lastRead:            time.Time{},
			unusedIndexDuration: time.Hour,
			expectedReturn: expectedReturn{
				shouldDrop:     false,
				recommendation: roachpb.ExistingIndexRecommendation{},
			},
		},
	}

	for _, tc := range testData {
		actualShouldDrop, actualRecommendation := recommendDropUnusedIndex(tc.currentTime, tc.createdAt, tc.lastRead, tc.unusedIndexDuration)
		require.Equal(t, tc.expectedReturn.shouldDrop, actualShouldDrop)
		require.Equal(t, tc.expectedReturn.recommendation, actualRecommendation)
	}
}
