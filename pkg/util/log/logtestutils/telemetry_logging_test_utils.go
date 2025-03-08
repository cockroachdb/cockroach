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
	syncutil.Mutex
	t time.Time
}

// SetTime sets the current time.
func (s *StubTime) SetTime(t time.Time) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.t = t
}

// TimeNow returns the current stubbed time.
func (s *StubTime) TimeNow() time.Time {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.t
}

// StubQueryStats is a helper struct to stub query level stats.
type StubQueryStats struct {
	syncutil.Mutex
	stats execstats.QueryLevelStats
}

// SetQueryLevelStats sets the stubbed query level stats.
func (s *StubQueryStats) SetQueryLevelStats(stats execstats.QueryLevelStats) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.stats = stats
}

// QueryLevelStats returns the current stubbed query level stats.
func (s *StubQueryStats) QueryLevelStats() execstats.QueryLevelStats {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.stats
}

// StubTracingStatus is a helper struct to stub whether a query is being
// traced.
type StubTracingStatus struct {
	syncutil.Mutex
	isTracing bool
}

// SetTracingStatus sets the stubbed status for tracing (true/false).
func (s *StubTracingStatus) SetTracingStatus(t bool) {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	s.isTracing = t
}

// TracingStatus returns the stubbed status for tracing.
func (s *StubTracingStatus) TracingStatus() bool {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()
	return s.isTracing
}
