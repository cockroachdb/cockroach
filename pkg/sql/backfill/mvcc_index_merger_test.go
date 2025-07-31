// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backfill

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReduceBatchSizeWhenAutoRetryLimitExhausted(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		kvErrs                 []error
		expectedFinalErr       error
		batch                  keyBatch
		expectedFinalBatchSize int
	}{
		{
			name:                   "no errors",
			kvErrs:                 []error{nil},
			expectedFinalErr:       nil,
			batch:                  keyBatch{sourceKeys: make([]roachpb.Key, 1000)},
			expectedFinalBatchSize: 1000,
		},
		{
			name:                   "retry once",
			kvErrs:                 []error{errors.Mark(errors.New("retry"), kv.ErrAutoRetryLimitExhausted), nil},
			expectedFinalErr:       nil,
			batch:                  keyBatch{sourceKeys: make([]roachpb.Key, 1000)},
			expectedFinalBatchSize: 500,
		},
		{
			name:                   "retry max times",
			kvErrs:                 []error{errors.Mark(errors.New("retry"), kv.ErrAutoRetryLimitExhausted)},
			expectedFinalErr:       errors.Mark(errors.New("retry"), kv.ErrAutoRetryLimitExhausted),
			batch:                  keyBatch{sourceKeys: make([]roachpb.Key, 1000)},
			expectedFinalBatchSize: 1,
		},
		{
			name: "don't retry if not a auto retry limit exhausted error",
			kvErrs: []error{
				errors.Mark(errors.New("retry"), kv.ErrAutoRetryLimitExhausted),
				errors.Mark(errors.New("retry"), kv.ErrAutoRetryLimitExhausted),
				errors.New("not a retry"),
			},
			expectedFinalErr:       errors.New("not a retry"),
			batch:                  keyBatch{sourceKeys: make([]roachpb.Key, 1000)},
			expectedFinalBatchSize: 250,
		},
	} {
		ctx := context.Background()
		t.Run(tc.name, func(t *testing.T) {
			i := 0
			batch := tc.batch
			err := retryWithReducedBatchWhenAutoRetryLimitExceeded(ctx, &batch, func(context.Context, []roachpb.Key) error {
				// Return the next error in the list, or the last one if we run out.
				if i >= len(tc.kvErrs) {
					i = len(tc.kvErrs) - 1
				}
				err := tc.kvErrs[i]
				i++
				return err
			})
			if tc.expectedFinalErr == nil {
				require.NoError(t, err)
				require.Equal(t, batch.processedKeys, len(batch.sourceKeys))
			} else {
				require.True(t, kv.IsAutoRetryLimitExhaustedError(tc.expectedFinalErr) == kv.IsAutoRetryLimitExhaustedError(err))
				require.Equal(t, batch.processedKeys, 0)
			}
			require.Equal(t, tc.expectedFinalBatchSize, batch.batchSize)
		})
	}
}
