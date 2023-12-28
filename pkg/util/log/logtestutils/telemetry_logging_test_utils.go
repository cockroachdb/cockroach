// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logtestutils

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
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

type SampledQueryLogSpy struct {
	testState          *testing.T
	stripRedactMarkers bool

	mu struct {
		syncutil.RWMutex
		logs   []eventpb.SampledQuery
		filter func(entry eventpb.SampledQuery) bool
	}
}

func NewSampledQueryLogSpy(
	testState *testing.T, stripRedactMarkers bool, filter func(entry eventpb.SampledQuery) bool,
) *SampledQueryLogSpy {
	s := &SampledQueryLogSpy{
		testState:          testState,
		stripRedactMarkers: stripRedactMarkers,
	}
	s.mu.filter = filter
	return s
}

func (s *SampledQueryLogSpy) Intercept(entry []byte) {
	var logEntry logpb.Entry

	if err := json.Unmarshal(entry, &logEntry); err != nil {
		s.testState.Fatal(err)
	}

	if logEntry.Channel != logpb.Channel_TELEMETRY || !strings.Contains(logEntry.Message, "sampled_query") {
		return
	}

	var statementLog eventpb.SampledQuery
	if err := json.Unmarshal([]byte(logEntry.Message[logEntry.StructuredStart:logEntry.StructuredEnd]),
		&statementLog); err != nil {
		s.testState.Fatal(err)
	}

	if s.mu.filter != nil && !s.mu.filter(statementLog) {
		return
	}

	if s.stripRedactMarkers {
		statementLog.Statement = redact.RedactableString(statementLog.Statement.StripMarkers())
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.logs = append(s.mu.logs, statementLog)
}

func (s *SampledQueryLogSpy) GetStatementLogs() []eventpb.SampledQuery {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stmtLogs := make([]eventpb.SampledQuery, len(s.mu.logs))
	copy(stmtLogs, s.mu.logs)
	return stmtLogs
}

func (s *SampledQueryLogSpy) ClearCollectedLogs() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.logs = make([]eventpb.SampledQuery, 0)
}
