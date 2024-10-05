// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestingDistIndexBackfill exposes the index backfill functionality for
// testing.
func (sc *SchemaChanger) TestingDistIndexBackfill(
	ctx context.Context,
	version descpb.DescriptorVersion,
	targetSpans []roachpb.Span,
	addedIndexes []descpb.IndexID,
	filter backfill.MutationFilter,
) error {
	s := &multiStageFractionScaler{initial: 0.0, stages: backfillStageFractions}
	err := sc.distIndexBackfill(ctx, version, targetSpans, addedIndexes, true, filter, s)
	return err
}

// SetJob sets the job.
func (sc *SchemaChanger) SetJob(job *jobs.Job) {
	sc.job = job
}

func TestCalculateSplitAtShards(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		testName    string
		maxSplit    int64
		bucketCount int32
		expected    []int64
	}{
		{
			testName:    "buckets_less_than_max_split",
			maxSplit:    8,
			bucketCount: 0,
			expected:    []int64{},
		},
		{
			testName:    "buckets_less_than_max_split",
			maxSplit:    8,
			bucketCount: 5,
			expected:    []int64{0, 1, 2, 3, 4},
		},
		{
			testName:    "buckets_equal_to_max_split",
			maxSplit:    8,
			bucketCount: 8,
			expected:    []int64{0, 1, 2, 3, 4, 5, 6, 7},
		},
		{
			testName:    "buckets_greater_than_max_split_1",
			maxSplit:    8,
			bucketCount: 30,
			expected:    []int64{0, 3, 7, 11, 15, 18, 22, 26},
		},
		{
			testName:    "buckets_greater_than_max_split_2",
			maxSplit:    8,
			bucketCount: 1000,
			expected:    []int64{0, 125, 250, 375, 500, 625, 750, 875},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			shards := calculateSplitAtShards(tc.maxSplit, tc.bucketCount)
			require.Equal(t, tc.expected, shards)
		})
	}
}

// TestNotFirstInLine tests that if a schema change's mutation is not first in
// line, the error message clearly state what the blocking schema change job ID
// is.
func TestNotFirstInLine(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// A helper to extract blocking schema change job IDs from the error message
	// which is of the form "schema change is blocked by x other schema change
	// job(s) [yyy zzz]".
	extractSortedBlockingJobIDsFromNotFirstInLineErr := func(errMsg string) []string {
		p := regexp.MustCompile(`\[(.*?)\]`)
		m := p.FindStringSubmatch(errMsg)
		require.NotNilf(t, m, "did not find any blocking job IDs")
		blockingJobIDs := strings.Fields(m[1])
		sort.Slice(blockingJobIDs, func(i, j int) bool {
			return blockingJobIDs[i] <= blockingJobIDs[j]
		})
		return blockingJobIDs
	}

	ctx := context.Background()
	desc := descpb.TableDescriptor{
		Name: "t",
		ID:   104,
		Mutations: []descpb.DescriptorMutation{
			{
				Descriptor_: &descpb.DescriptorMutation_Index{},
				MutationID:  1,
			},
			{
				Descriptor_: &descpb.DescriptorMutation_Index{},
				MutationID:  1,
			},
			{
				Descriptor_: &descpb.DescriptorMutation_Column{},
				MutationID:  2,
			},
			{
				Descriptor_: &descpb.DescriptorMutation_Column{},
				MutationID:  3,
			},
		},
		MutationJobs: []descpb.TableDescriptor_MutationJob{
			{
				JobID:      11111,
				MutationID: 1,
			},
			{
				JobID:      22222,
				MutationID: 2,
			},
			{
				JobID:      33333,
				MutationID: 3,
			},
		},
	}
	mut := tabledesc.NewBuilder(&desc).BuildExistingMutableTable()
	{
		sc := SchemaChanger{
			descID:     104,
			mutationID: 1,
		}
		err := sc.notFirstInLine(ctx, mut)
		require.NoError(t, err)
	}
	{
		sc := SchemaChanger{
			descID:     104,
			mutationID: 2,
		}
		err := sc.notFirstInLine(ctx, mut)
		require.True(t, errors.Is(err, errSchemaChangeNotFirstInLine))
		require.Equal(t, []string{"11111"}, extractSortedBlockingJobIDsFromNotFirstInLineErr(err.Error()))
	}
	{
		sc := SchemaChanger{
			descID:     104,
			mutationID: 3,
		}
		err := sc.notFirstInLine(ctx, mut)
		require.True(t, errors.Is(err, errSchemaChangeNotFirstInLine))
		require.Equal(t, []string{"11111", "22222"}, extractSortedBlockingJobIDsFromNotFirstInLineErr(err.Error()))
	}
}
