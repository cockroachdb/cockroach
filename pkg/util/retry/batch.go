// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package retry

import (
	"context"

	"github.com/cockroachdb/errors"
)

// Batch manages a batched operation, allowing retries for failures.
// It processes items in chunks, adjusting the batch size dynamically
// based on errors encountered. Users must provide a `Do` function
// to define the batch operation and can optionally define retry behavior
// using `IsRetriableError` and `OnRetry`.
type Batch struct {
	// Private fields. These are initialized by Start() to manage the batch process.
	// batchSize is the computed size of the next batch.
	batchSize int64
	// totalToProcess is the total number of items to process across all batches.
	totalToProcess int64
	// processed is the count of items successfully processed so far.
	processed int64

	// Do executes the batch operation. It receives the number of items already
	// processed in previous successful batches and the calculated size of the next batch.
	// If the entire batch is processed successfully, it returns nil.
	Do func(ctx context.Context, processed, batchSize int64) error

	// IsRetriableError determines whether an error is retriable. If it returns false,
	// the error is returned to the caller, and no retry is attempted.
	IsRetriableError func(error) bool

	// OnRetry is an optional function for handling retries (e.g., logging errors).
	// It receives the last encountered error and the computed batch size for the
	// next retry. It must return nil if you want to retry with a smaller batch.
	OnRetry func(err error, batchSize int64) error
}

// Start begins processing the batch operation with the given batch size.
// It runs the `Do` function in a loop, adjusting the batch size as needed
// based on errors. If an error occurs, it checks if it's retriable.
// If so, the batch size is reduced, and `OnRetry` (if provided) is called
// before retrying. If a non-retriable error is encountered or the batch
// reaches the minimum size without success, the function returns the error.
func (b *Batch) Start(ctx context.Context, batchSize int64) error {
	const minBatchSize = 1

	if batchSize <= 0 {
		return errors.AssertionFailedf("batch size must be a positive number: %d", batchSize)
	}

	b.batchSize = batchSize
	b.totalToProcess = batchSize

	for b.processed = 0; b.processed < b.totalToProcess; {
		err := b.Do(ctx, b.processed, b.batchSize)
		if err == nil {
			b.processed += b.batchSize
			b.batchSize = min(b.batchSize, b.totalToProcess-b.processed)
			continue
		}

		// Stop retrying if the batch is already at the minimum size.
		if b.batchSize == minBatchSize {
			return err
		}

		// Handle retriable errors by reducing the batch size and retrying.
		if b.IsRetriableError(err) {
			b.batchSize = max(b.batchSize/2, minBatchSize)
			if b.OnRetry != nil {
				if retryErr := b.OnRetry(err, b.batchSize); retryErr != nil {
					return retryErr
				}
			}
			continue
		}

		// Return non-retriable errors immediately.
		return err
	}

	return nil
}
