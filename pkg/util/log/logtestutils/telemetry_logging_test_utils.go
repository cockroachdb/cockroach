// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logtestutils

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// StubTime is a helper struct to stub the current time.
type StubTime struct {
	syncutil.RWMutex
	t time.Time
}

// SetTime sets the current time.
func (s *StubTime) SetTime(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.t = t
}

// TimeNow returns the current stubbed time.
func (s *StubTime) TimeNow() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.t
}

// StubQueryStats is a helper struct to stub query level stats.
type StubQueryStats struct {
	syncutil.RWMutex
	stats execstats.QueryLevelStats
}

// SetQueryLevelStats sets the stubbed query level stats.
func (s *StubQueryStats) SetQueryLevelStats(stats execstats.QueryLevelStats) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.stats = stats
}

// QueryLevelStats returns the current stubbed query level stats.
func (s *StubQueryStats) QueryLevelStats() execstats.QueryLevelStats {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.stats
}

// StubTracingStatus is a helper struct to stub whether a query is being
// traced.
type StubTracingStatus struct {
	syncutil.RWMutex
	isTracing bool
}

// SetTracingStatus sets the stubbed status for tracing (true/false).
func (s *StubTracingStatus) SetTracingStatus(t bool) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.isTracing = t
}

// TracingStatus returns the stubbed status for tracing.
func (s *StubTracingStatus) TracingStatus() bool {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.isTracing
}
