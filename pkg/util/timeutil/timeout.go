// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
)

// RunWithTimeout runs a function with a timeout, the same way you'd do with
// context.WithTimeout. It improves the opaque error messages returned by
// WithTimeout by augmenting them with the op string that is passed in.
func RunWithTimeout(
	ctx context.Context, op string, timeout time.Duration, fn func(ctx context.Context) error,
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
