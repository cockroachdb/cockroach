// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logtestutils

import (
	"context"
	"encoding/json"
	"math"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// LogSpy is a simple test utility which intercepts and stores
// log entries for testing purposes. It is not be used outside
// of tests.
type LogSpy struct {
	testState *testing.T

	filters []func(entry logpb.Entry) bool

	mu struct {
		syncutil.Mutex

		logs []logpb.Entry
	}
}

// NewLogSpy takes a test state and list of filters and returns
// a new LogSpy.
func NewLogSpy(t *testing.T, filters ...func(entry logpb.Entry) bool) *LogSpy {
	return &LogSpy{
		testState: t,
		filters:   filters,
		mu: struct {
			syncutil.Mutex
			logs []logpb.Entry
		}{
			logs: []logpb.Entry{},
		},
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

	s.mu.logs = append(s.mu.logs, entry)
}

// ReadAll consumes all logs contained within the spy and returns
// them as a list. Once the logs are consumed they cannot be read again.
func (s *LogSpy) ReadAll() []logpb.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.readLocked(math.MaxUint32)
}

// ReadAll consumes the specified number of logs contained within
// the spy and returns them as a list. Once the logs are consumed
// they cannot be read again.
func (s *LogSpy) Read(n int) []logpb.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.readLocked(n)
}

func (s *LogSpy) readLocked(n int) []logpb.Entry {
	if n > len(s.mu.logs) {
		n = len(s.mu.logs)
	}

	entries := s.mu.logs[:n]
	s.mu.logs = s.mu.logs[n:]

	return entries
}

// Has checks whether the spy has any log which passes the passed
// in filters.
func (s *LogSpy) Has(filters ...func(logpb.Entry) bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

logLoop:
	for _, entry := range s.mu.logs {
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

// MatchesF returns a filter that matches log entries which contain
// the input string.
func MatchesF(s string) func(entry logpb.Entry) bool {
	return func(entry logpb.Entry) bool {
		exists, err := regexp.MatchString(s, entry.Message)
		if err != nil {
			log.Errorf(context.Background(), "failed to match regex %s: %s", s, err)
		}
		return exists
	}
}
