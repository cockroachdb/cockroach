// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package besteffort

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type config struct {
	syncutil.Mutex
	allowedFailures map[string]struct{}
	cantSkip        map[string]struct{}
}

var cfg = &config{
	allowedFailures: make(map[string]struct{}),
	cantSkip:        make(map[string]struct{}),
}

func isAllowedFailure(name string) bool {
	if !buildutil.CrdbTestBuild {
		return true
	}
	cfg.Lock()
	defer cfg.Unlock()
	_, ok := cfg.allowedFailures[name]
	return ok
}

// TestAllowFailure marks the besteffort operation with the given name as
// allowed to fail without panicking in test builds.
//
// By default, besteffort operations panic on failure in test builds to catch
// regressions. Use TestAllowFailure when testing error handling or when the
// operation is expected to fail in the test scenario.
//
// Returns a cleanup function that should be deferred to restore the default
// panic behavior. This has no effect in production builds.
//
// Example usage:
//
//	func TestCacheUpdateFailure(t *testing.T) {
//		defer besteffort.TestAllowFailure("update-stats-cache")()
//		defer besteffort.TestForbidSkip("update-stats-cache")()
//
//		// Simulate a failure condition
//		cache.Close()
//
//		// This would normally panic in tests, but is allowed to fail
//		besteffort.Warning(ctx, "update-stats-cache", func(ctx context.Context) error {
//			return cache.UpdateStatistics(ctx)
//		})
//	}
func TestAllowFailure(name string) (cleanup func()) {
	if !buildutil.CrdbTestBuild {
		return func() {}
	}
	cfg.Lock()
	defer cfg.Unlock()
	cfg.allowedFailures[name] = struct{}{}
	return func() {
		cfg.Lock()
		defer cfg.Unlock()
		delete(cfg.allowedFailures, name)
	}
}

func shouldSkip(name string) bool {
	if !buildutil.CrdbTestBuild {
		return false
	}
	if rand.Intn(2) == 0 {
		return false
	}
	cfg.Lock()
	defer cfg.Unlock()
	_, ok := cfg.cantSkip[name]
	return !ok
}

// TestForbidSkip marks the besteffort operation with the given name as not to
// be skipped in test builds.
//
// In test builds, besteffort operations are randomly skipped 50% of the time to
// ensure the system remains correct when these operations don't run. Use
// TestForbidSkip when testing the operation's behavior or side effects.
//
// Returns a cleanup function that should be deferred to restore the default
// random skipping behavior. This has no effect in production builds.
//
// Example usage:
//
//	func TestStatsUpdateSuccess(t *testing.T) {
//		defer besteffort.TestForbidSkip("update-stats-cache")()
//
//		// This ensures the operation actually runs and isn't skipped
//		besteffort.Warning(ctx, "update-stats-cache", func(ctx context.Context) error {
//			return cache.UpdateStatistics(ctx)
//		})
//
//		// Verify the cache was updated
//		require.Equal(t, expectedStats, cache.GetStats())
//	}
func TestForbidSkip(name string) (cleanup func()) {
	if !buildutil.CrdbTestBuild {
		return func() {}
	}
	cfg.Lock()
	defer cfg.Unlock()
	cfg.cantSkip[name] = struct{}{}
	return func() {
		cfg.Lock()
		defer cfg.Unlock()
		delete(cfg.cantSkip, name)
	}
}
