// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package besteffort

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Warning executes a best-effort operation that logs a warning on failure.
//
// Best-effort operations are tasks that should be attempted but are not critical
// to system correctness. In production builds, failures are logged as warnings.
// In test builds, failures panic by default to catch regressions.
//
// Example usage:
//
//	// Update cached statistics - helpful but not required for correctness
//	besteffort.Warning(ctx, "update-stats-cache", func(ctx context.Context) error {
//		return cache.UpdateStatistics(ctx)
//	})
func Warning(ctx context.Context, name string, do func(ctx context.Context) error) {
	if shouldSkip(name) {
		log.Dev.Infof(ctx, "skipping best effort operation '%s'", name)
		return
	}
	err := do(ctx)
	if err != nil {
		if !isAllowedFailure(name) {
			panic(err)
		}
		log.Dev.Warningf(ctx, "best effort operation '%s' failed: %+v", name, err)
	}
}

// Error executes a best-effort operation that logs an error on failure.
//
// Best-effort operations are tasks that should be attempted but are not critical
// to system correctness. In production builds, failures are logged as errors.
// In test builds, failures panic by default to catch regressions.
//
// Example usage:
//
//	// Clean up old files - important but not critical for correctness
//	besteffort.Error(ctx, "cleanup-temp-files", func(ctx context.Context) error {
//		return os.RemoveAll(tempDir)
//	})
func Error(ctx context.Context, name string, do func(ctx context.Context) error) {
	if shouldSkip(name) {
		log.Dev.Infof(ctx, "skipping best effort operation '%s'", name)
		return
	}
	err := do(ctx)
	if err != nil {
		if !isAllowedFailure(name) {
			panic(err)
		}
		log.Dev.Errorf(ctx, "best effort operation '%s' failed: %+v", name, err)
	}
}
