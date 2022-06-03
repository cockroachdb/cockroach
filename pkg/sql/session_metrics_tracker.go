// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// SessionMetricsTrackerTestingKnobs is used to control the behavior of
// the SessionMetricsTracker. If it is non-nil, both of the override
// values will be used, even if they are zero.
type SessionMetricsTrackerTestingKnobs struct {
	MinUpdateInterval, LongRunningThreshold time.Duration
}

// ModuleTestingKnobs makes SessionMetricsTrackerTestingKnobs into a
// base.ModuleTestingKnobs.
func (*SessionMetricsTrackerTestingKnobs) ModuleTestingKnobs() {}

// sessionMetricsTracker tracks, in a stateful way, some aggregate information
// over all sessions. It exists in part to facilitate functional gauges which
// want relatively fresh data, but not so much that we should refresh this data
// more frequently than some pre-defined defaultMinUpdateInterval.
type sessionMetricsTracker struct {
	registry *SessionRegistry
	// Below fields are only protected by the mutex to enable testing.
	minUpdateInterval    time.Duration
	longRunningThreshold time.Duration
	mu                   struct {
		syncutil.Mutex

		lastUpdate     time.Time
		user, internal sessionMetricsData
	}
}

func newSessionMetricsTracker(
	registry *SessionRegistry, knobs *SessionMetricsTrackerTestingKnobs,
) *sessionMetricsTracker {
	const defaultMinUpdateInterval = time.Second
	smt := &sessionMetricsTracker{registry: registry}
	if knobs != nil {
		smt.minUpdateInterval = knobs.MinUpdateInterval
		smt.longRunningThreshold = knobs.LongRunningThreshold
	} else {
		smt.minUpdateInterval = defaultMinUpdateInterval
		smt.longRunningThreshold = base.SlowRequestThreshold
	}
	return smt
}

type longRunningMetrics struct {
	longest time.Duration
	count   int64
}

type sessionMetricsData = struct {
	stmt, txn longRunningMetrics
}

func (s *sessionMetricsTracker) read(internal bool) sessionMetricsData {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeUpdateLocked()
	if internal {
		return s.mu.internal
	}
	return s.mu.user
}

func (s *sessionMetricsTracker) maybeUpdateLocked() {
	now := timeutil.Now()
	if s.mu.lastUpdate.Add(s.minUpdateInterval).After(now) {
		return
	}
	s.mu.lastUpdate = now
	s.mu.user, s.mu.internal = makeSessionMetricsData(
		s.registry, now, s.longRunningThreshold,
	)
}

func makeSessionMetricsData(
	registry *SessionRegistry, now time.Time, longRunningThreshold time.Duration,
) (user, internal sessionMetricsData) {
	updateLongRunning := func(lr *longRunningMetrics, dur time.Duration) {
		if dur > lr.longest {
			lr.longest = dur
		}
		if dur > longRunningThreshold {
			lr.count++
		}
	}
	update := func(smd *sessionMetricsData, s serverpb.Session) {
		if s.ActiveTxn != nil {
			updateLongRunning(&smd.txn, now.Sub(s.ActiveTxn.Start))
		}
		for i := range s.ActiveQueries {
			updateLongRunning(&smd.stmt, now.Sub(s.ActiveQueries[i].Start))
		}
	}
	registry.Lock()
	defer registry.Unlock()
	for _, session := range registry.sessions {
		s := session.serialize()
		if strings.HasPrefix(s.ApplicationName, catconstants.InternalAppNamePrefix) {
			update(&internal, s)
		} else {
			update(&user, s)
		}
	}
	return user, internal
}
