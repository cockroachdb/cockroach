// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logtestutils

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
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

// FormatEntryAsJSON formats a structured log entry as a JSON string.
// It only includes fields that are included by in includeByDefault.
func FormatEntryAsJSON(entry logpb.Entry) (string, error) {
	var jsonMap map[string]interface{}
	// A decoder with .UseNumber() is used to ensure that precision on int64 and uint64 isn't lost. This happens
	// due to the json package using type float64 when unmarshalling numbers into an interface{} type.
	decoder := json.NewDecoder(strings.NewReader(entry.Message[entry.StructuredStart:entry.StructuredEnd]))
	decoder.UseNumber()
	if err := decoder.Decode(&jsonMap); err != nil {
		return "", err
	}
	return printJSONMap(jsonMap)
}

// StructuredLogSpy is a test utility that intercepts structured log entries
// and stores them in memory. It can be used to verify the contents of log
// entries in tests.
// The spy can be configured to intercept logs from specific channels and
// specific event types. If no event types are specified, the spy will
// intercept all logs in the given channels.
type StructuredLogSpy[T any] struct {
	testState *testing.T

	// map of channels to pointer
	channels map[logpb.Channel]struct{}

	// eventTypes is a list of event types that the spy is interested in.
	// If the list is empty, the spy will intercept all events in the channel.
	// The event type is derived from the proto message name converted to snake case.
	// e.g. "sampled_query" for eventpb.SampledQuery.
	eventTypes []string

	// eventTypeRe is a list of regular expressions created from eventTypes
	// that match the event type in the log message.
	eventTypeRe []*regexp.Regexp

	mu struct {
		syncutil.RWMutex

		logs map[logpb.Channel][]T

		// filters is a list of functions that are applied to the formatted log entry
		// to determine if the log should be intercepted.
		filters []func(entry logpb.Entry, formattedEntry T) bool

		// Function to transform log entries into the desired format.
		format func(entry logpb.Entry) (T, error)

		// lastReadIdx is a map of channel to int, representing the last read log
		// line read when calling GetUnreadLogs.
		lastReadIdx map[logpb.Channel]int
	}
}

// NewStructuredLogSpy creates a new StructuredLogSpy.
//   - @param testState: the testing.T instance to use for fatal errors.
//   - @param channels: the channels to intercept logs from.
//   - @param eventTypes: the event types to intercept. If empty, all events are intercepted.
//     Event type names are derived from the proto message name converted to snake case,
//     e.g. eventpb.SampledQuery => "sampled_query"
//   - @param format: function to convert the logpb.Entry to the desired format.
//   - @param filters: list of boolean functions that are applied to the formatted log entry
//     to determine if the log should be intercepted.
func NewStructuredLogSpy[T any](
	testState *testing.T,
	channels []logpb.Channel,
	eventTypes []string,
	format func(entry logpb.Entry) (T, error),
	filters ...func(entry logpb.Entry, formattedEntry T) bool,
) *StructuredLogSpy[T] {
	s := &StructuredLogSpy[T]{
		testState: testState,
		channels:  make(map[logpb.Channel]struct{}, len(channels)),
	}

	for _, ch := range channels {
		s.channels[ch] = struct{}{}
	}
	s.mu.lastReadIdx = make(map[logpb.Channel]int, len(channels))
	s.mu.logs = make(map[logpb.Channel][]T, len(s.channels))
	s.mu.format = format
	s.mu.filters = append(s.mu.filters, filters...)
	s.eventTypes = eventTypes
	for _, eventType := range eventTypes {
		s.eventTypeRe = append(s.eventTypeRe, regexp.MustCompile(fmt.Sprintf(`"EventType":"%s"`, eventType)))
	}

	return s

}

// AddFilters adds filters to the spy.
func (s *StructuredLogSpy[T]) AddFilters(f ...func(entry logpb.Entry, formattedEntry T) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.filters = append(s.mu.filters, f...)
}

// ClearFilters removes all filters from the spy.
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

// GetUnreadLogs returns all logs that have been intercepted in the given channel
// since the last time this method was called.
func (s *StructuredLogSpy[T]) GetUnreadLogs(ch logpb.Channel) []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	currentCount := len(s.mu.logs[ch])
	lastReadLogLine := s.mu.lastReadIdx[ch]
	logs := make([]T, 0, currentCount-lastReadLogLine)
	logs = append(logs, s.mu.logs[ch][lastReadLogLine:currentCount]...)
	s.mu.lastReadIdx[ch] = currentCount
	return logs
}

// SetLastNLogsAsUnread will decrement lastReadIdx[ch] by n to consider those
// logs "unread". As a result, they will be included in the next GetUnreadLogs
// call.
func (s *StructuredLogSpy[T]) SetLastNLogsAsUnread(ch logpb.Channel, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.lastReadIdx[ch] -= n

}

// Intercept intercepts a log entry and stores it in memory.
// Logs are formatted and then filtered before being stored.
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

// Reset clears all logs that have been intercepted thus far.
func (s *StructuredLogSpy[T]) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.logs = make(map[logpb.Channel][]T, len(s.channels))
	s.mu.lastReadIdx = make(map[logpb.Channel]int, len(s.channels))
}

// Channels returns a list of the channels that the spy is
// intercepting logs from.
func (s *StructuredLogSpy[T]) Channels() []logpb.Channel {
	channels := make([]logpb.Channel, 0, len(s.channels))
	for ch := range s.channels {
		channels = append(channels, ch)
	}
	return channels
}
