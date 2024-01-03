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
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
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

// These fields should be constant in each test run.
// Note that they won't appear in the output if the field is omitted.
var includeByDefault = map[string]struct{}{
	"EventType":               {},
	"User":                    {},
	"Statement":               {},
	"Tag":                     {},
	"ApplicationName":         {},
	"PlaceholderValues":       {},
	"NumRows":                 {},
	"SQLSTATE":                {},
	"ErrorText":               {},
	"NumRetries":              {},
	"FullTableScan":           {},
	"FullIndexScan":           {},
	"StmtPosInTxn":            {},
	"SkippedQueries":          {},
	"Distribution":            {},
	"PlanGist":                {},
	"Database":                {},
	"StatementFingerprintID":  {},
	"MaxFullScanRowsEstimate": {},
	"TotalScanRowsEstimate":   {},
	"OutputRowsEstimate":      {},
	"RowsRead":                {},
	"RowsWritten":             {},
	"InnerJoinCount":          {},
	"LeftOuterJoinCount":      {},
	"FullOuterJoinCount":      {},
	"SemiJoinCount":           {},
	"AntiJoinCount":           {},
	"IntersectAllJoinCount":   {},
	"ExceptAllJoinCount":      {},
	"HashJoinCount":           {},
	"CrossJoinCount":          {},
	"IndexJoinCount":          {},
	"LookupJoinCount":         {},
	"MergeJoinCount":          {},
	"InvertedJoinCount":       {},
	"ApplyJoinCount":          {},
	"ZigZagJoinCount":         {},
	"KVRowsRead":              {},
	"IndexRecommendations":    {},
	"ScanCount":               {},
	"Indexes":                 {},
}

// printJSONMap prints a map as a JSON string. In the future we can
// add more filtering logic here to include additional fields but the default
// fields are sufficient for testing for now.
func printJSONMap(data map[string]interface{}) string {
	filteredJson := make(map[string]interface{})
	for k, v := range data {
		if _, found := includeByDefault[k]; found {
			filteredJson[k] = v
		}
	}

	jsonStr, err := json.Marshal(filteredJson)
	if err != nil {
		return "Error:" + err.Error()
	}

	return string(jsonStr)
}

type TelemetryLogSpy struct {
	testState *testing.T

	mu struct {
		syncutil.RWMutex
		logs    []string
		filters []func(entry logpb.Entry) bool
		format  func(entry logpb.Entry) string
	}
}

func NewSampledQueryLogScrubVolatileFields(testState *testing.T) *TelemetryLogSpy {
	s := &TelemetryLogSpy{
		testState: testState,
	}
	s.mu.filters = append(s.mu.filters, func(entry logpb.Entry) bool {
		return strings.Contains(entry.Message, "sampled_query")
	})
	s.mu.format = func(entry logpb.Entry) string {
		var jsonMap map[string]interface{}
		if err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &jsonMap); err != nil {
			s.testState.Fatal(err)
		}
		return printJSONMap(jsonMap)
	}

	return s
}

func (s *TelemetryLogSpy) AddFilter(f func(entry logpb.Entry) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.filters = append(s.mu.filters, f)
}

func (s *TelemetryLogSpy) GetLastNLogs(n int) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return strings.Join(s.mu.logs[len(s.mu.logs)-n:], "\n")
}

func (s *TelemetryLogSpy) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.mu.logs)
}

func (s *TelemetryLogSpy) Intercept(entry []byte) {
	var logEntry logpb.Entry

	if err := json.Unmarshal(entry, &logEntry); err != nil {
		s.testState.Fatal(err)
	}

	if logEntry.Channel != logpb.Channel_TELEMETRY {
		return
	}

	if s.mu.filters != nil {
		for _, filter := range s.mu.filters {
			if !filter(logEntry) {
				return
			}
		}
	}

	formattedLogStr := s.mu.format(logEntry)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.logs = append(s.mu.logs, formattedLogStr)
}
