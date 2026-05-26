// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// RunWithTimeout runs a function with a timeout, the same way you'd do with
// context.WithTimeout. It improves the opaque error messages returned by
// WithTimeout by augmenting them with the op string that is passed in.
func RunWithTimeout(
	ctx context.Context,
	op redact.RedactableString,
	timeout time.Duration,
	fn func(ctx context.Context) error,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout) // nolint:context
	defer cancel()
	start := Now()
	err := fn(ctx)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = &TimeoutError{
			operation: op,
			timeout:   timeout,
			took:      Since(start),
			cause:     err,
		}
	}
	return err
}
