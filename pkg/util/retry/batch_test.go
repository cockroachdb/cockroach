// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package retry

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestBatch(t *testing.T) {
	const totalBatchSize = 1000
	retryMark := errors.New("retry limit exhausted")
	isRetryableError := func(err error) bool {
		return errors.Is(err, retryMark)
	}
	for _, tc := range []struct {
		name               string
		kvErrs             []error
		expectedFinalErr   error
		expectedRetryCount int64
	}{
		{
			name:               "no errors",
			kvErrs:             []error{nil},
			expectedFinalErr:   nil,
			expectedRetryCount: 0,
		},
		{
			name:               "retry once",
			kvErrs:             []error{errors.Mark(errors.New("retry"), retryMark), nil},
			expectedFinalErr:   nil,
			expectedRetryCount: 1,
		},
		{
			name:               "retry max times",
			kvErrs:             []error{errors.Mark(errors.New("retry"), retryMark)},
			expectedFinalErr:   errors.Mark(errors.New("retry"), retryMark),
			expectedRetryCount: 9,
		},
		{
			name: "dont retry if not an auto retry limit exhausted error",
			kvErrs: []error{
				errors.Mark(errors.New("retry"), retryMark),
				errors.Mark(errors.New("retry"), retryMark),
				errors.New("not a retry"),
			},
			expectedFinalErr:   errors.New("not a retry"),
			expectedRetryCount: 2,
		},
	} {
		ctx := context.Background()
		t.Run(tc.name, func(t *testing.T) {
			i := 0
			retryCount := int64(0)
			batch := Batch{
				Do: func(context.Context, int, int) error {
					// Return the next error in the list, or the last one if we run out.
					if i >= len(tc.kvErrs) {
						i = len(tc.kvErrs) - 1
					}
					err := tc.kvErrs[i]
					i++
					return err
				},
				OnRetry: func(error, int) error {
					retryCount++
					return nil
				},
				IsRetryableError: isRetryableError,
			}
			err := batch.Execute(ctx, totalBatchSize)
			if tc.expectedFinalErr == nil {
				require.NoError(t, err)
				require.Equal(t, batch.processed, totalBatchSize)
			} else {
				require.True(t, isRetryableError(tc.expectedFinalErr) == isRetryableError(err))
				require.Equal(t, batch.processed, 0)
			}
			require.Equal(t, tc.expectedRetryCount, retryCount)
		})
	}
}
