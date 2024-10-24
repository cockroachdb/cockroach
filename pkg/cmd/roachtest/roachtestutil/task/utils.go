// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package task

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// IsContextCanceled returns a boolean indicating whether the context
// passed is canceled.
func IsContextCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// WaitForChannel waits for the given channel `ch` to close; returns
// when that happens. If the channel does not close within 5 minutes,
// the function logs a message and returns.
//
// The main use-case for this function is waiting for user-provided
// hooks to return after the context passed to them is canceled. We
// want to allow some time for them to finish, but we also don't want
// to block indefinitely if a function inadvertently ignores context
// cancellation.
func WaitForChannel(ch chan error, desc string, l *logger.Logger) {
	maxWait := 5 * time.Minute
	select {
	case <-ch:
		// return
	case <-time.After(maxWait):
		l.Printf("waited for %s for %s to finish, giving up", maxWait, desc)
	}
}
