// Copyright 2024 The Cockroach Authors.
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
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// These fields should be constant in each test run.
// Note that they won't appear in the output if the field is omitted.
// TODO(xinhaoz): We may need to split this structure if we encounter
// field name collisions between events types.
var includeByDefault = map[string]struct{}{
	// Common event fields.
	"EventType":         {},
	"User":              {},
	"Statement":         {},
	"Tag":               {},
	"ApplicationName":   {},
	"PlaceholderValues": {},
	"NumRows":           {},
	"SQLSTATE":          {},
	"ErrorText":         {},
	"NumRetries":        {},
	"FullTableScan":     {},
	"FullIndexScan":     {},
	"StmtPosInTxn":      {},

	// Telemetry fields.
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

	// Transaction fields.
	"Committed":                {},
	"Implicit":                 {},
	"LastAutoRetryReason":      {},
	"SkippedTransactions":      {},
	"TransactionFingerprintID": {},
	"StatementFingerprintIDs":  {},
}

// printJSONMap prints a map as a JSON string. In the future we can
// add more filtering logic here to include additional fields but the default
// fields are sufficient for testing for now.
func printJSONMap(data map[string]interface{}) (string, error) {
	filteredJson := make(map[string]interface{})
	for k, v := range data {
		if _, found := includeByDefault[k]; found {
			filteredJson[k] = v
		}
	}

	outData, err := json.MarshalIndent(filteredJson, "", "\t")
	return string(outData), err
}

func FormatEntryAsJSON(entry logpb.Entry) (string, error) {
	var jsonMap map[string]interface{}
	if err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &jsonMap); err != nil {
		return "", err
	}

	return printJSONMap(jsonMap)
}

type StructuredLogSpy[T any] struct {
	testState *testing.T

	// map of channels to pointer
	channels map[logpb.Channel]interface{}

	// eventTypes is a list of event types that the spy is interested in.
	// If the list is empty, the spy will intercept all events in the channel.
	// The event type is derived from the proto message name converted to snake case.
	// e.g. "sampled_query" for eventpb.SampledQuery.
	eventTypes  []string
	eventTypeRe []*regexp.Regexp

	mu struct {
		syncutil.RWMutex
		logs    map[logpb.Channel][]T
		filters []func(entry logpb.Entry, formattedEntry T) bool
		format  func(entry logpb.Entry) (T, error)
	}
}

func NewStructuredLogSpy[T any](
	testState *testing.T,
	channels []logpb.Channel,
	eventTypes []string,
	format func(entry logpb.Entry) (T, error),
	filters ...func(entry logpb.Entry, formattedEntry T) bool,
) *StructuredLogSpy[T] {
	s := &StructuredLogSpy[T]{
		testState: testState,
		channels:  make(map[logpb.Channel]interface{}, len(channels)),
	}

	for _, ch := range channels {
		s.channels[ch] = struct{}{}
	}
	s.mu.logs = make(map[logpb.Channel][]T, len(s.channels))
	s.mu.format = format
	s.mu.filters = append(s.mu.filters, filters...)
	s.eventTypes = eventTypes
	for _, eventType := range eventTypes {
		s.eventTypeRe = append(s.eventTypeRe, regexp.MustCompile(fmt.Sprintf(`"EventType":"%s"`, eventType)))
	}

	return s

}

func (s *StructuredLogSpy[T]) AddFilters(f ...func(entry logpb.Entry, formattedEntry T) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.filters = append(s.mu.filters, f...)
}

func (s *StructuredLogSpy[T]) ClearFilters() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.filters = nil
}

// Count returns the number of logs that have been intercepted in
// the given channels. If no channels are provided, all logs are counted.
func (s *StructuredLogSpy[T]) Count(channels ...logpb.Channel) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sum := 0
	chs := channels
	if len(channels) == 0 {
		chs = s.Channels()
	}
	for _, ch := range chs {
		sum += len(s.mu.logs[ch])
	}
	return sum
}

// GetLastNLogs returns the last N logs that have been intercepted in
// the given channel. If n is greater than the number of logs intercepted
// all logs are returned.
func (s *StructuredLogSpy[T]) GetLastNLogs(ch logpb.Channel, n int) []T {
	if n < 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if n > len(s.mu.logs[ch]) {
		n = len(s.mu.logs[ch])
	}
	logs := make([]T, 0, n)
	logs = append(logs, s.mu.logs[ch][len(s.mu.logs[ch])-n:]...)
	return logs
}

// GetLogs returns all logs that have been intercepted in the given channel.
func (s *StructuredLogSpy[T]) GetLogs(ch logpb.Channel) []T {
	s.mu.RLock()
	defer s.mu.RUnlock()
	logs := make([]T, 0, len(s.mu.logs[ch]))
	logs = append(logs, s.mu.logs[ch]...)
	return logs
}

func (s *StructuredLogSpy[T]) Intercept(entry []byte) {
	var logEntry logpb.Entry

	if err := json.Unmarshal(entry, &logEntry); err != nil {
		s.testState.Fatal(err)
	}

	if _, ok := s.channels[logEntry.Channel]; !ok {
		return
	}

	if len(s.eventTypes) > 0 {
		found := func() bool {
			for _, re := range s.eventTypeRe {
				if re.MatchString(logEntry.Message) {
					return true
				}
			}
			return false
		}()
		if !found {
			return
		}
	}

	formattedLog, err := s.mu.format(logEntry)
	if err != nil {
		s.testState.Fatal(err)
	}

	if s.mu.filters != nil {
		for _, filter := range s.mu.filters {
			if !filter(logEntry, formattedLog) {
				return
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.logs[logEntry.Channel] = append(s.mu.logs[logEntry.Channel], formattedLog)
}

func (s *StructuredLogSpy[T]) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.logs = make(map[logpb.Channel][]T, len(s.channels))
}

func (s *StructuredLogSpy[T]) Channels() []logpb.Channel {
	channels := make([]logpb.Channel, 0, len(s.channels))
	for ch := range s.channels {
		channels = append(channels, ch)
	}
	return channels
}
