// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logtestutils

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// LogSpy is a simple test utility which intercepts and stores
// log entries for testing purposes. It is not be used outside
// of tests.
type LogSpy struct {
	testState *testing.T

	filters []func(entry logpb.Entry) bool

	logs []logpb.Entry

	mu struct {
		syncutil.Mutex
	}
}

func NewLogSpy(t *testing.T, filters ...func(entry logpb.Entry) bool) *LogSpy {
	return &LogSpy{
		testState: t,
		filters:   filters,
		logs:      []logpb.Entry{},
	}
}

// Intercept satisfies the log.Interceptor interface.
// It parses the log entry, checks if it passes the filters if
// any exist and if it does, stores it in the logs slice.
func (s *LogSpy) Intercept(body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var entry logpb.Entry
	err := json.Unmarshal(body, &entry)

	if err != nil {
		s.testState.Fatal(err)
	}

	for _, filter := range s.filters {
		if !filter(entry) {
			return
		}
	}

	s.logs = append(s.logs, entry)
}

// The read functions below discard the logs which are consumed.
func (s *LogSpy) ReadAll() []logpb.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.readLocked(math.MaxUint32)
}

func (s *LogSpy) Read(n int) []logpb.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.readLocked(n)
}

func (s *LogSpy) readLocked(n int) []logpb.Entry {
	if n > len(s.logs) {
		n = len(s.logs)
	}

	entries := s.logs[:n]
	s.logs = s.logs[n:]

	return entries
}

// Has checks whether the spy has any log which passes the passed
// in filters.
func (s *LogSpy) Has(filters ...func(logpb.Entry) bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

logLoop:
	for _, entry := range s.logs {
		for _, f := range filters {
			if !f(entry) {
				continue logLoop
			}
		}
		// only gets here if all filters matched this log
		return true
	}
	return false
}

// Matches returns a filter that matches log entries which contain
// the input string.
func MatchesF(s string) func(entry logpb.Entry) bool {
	return func(entry logpb.Entry) bool {
		return strings.Contains(entry.Message, s)
	}
}
