// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/petermattis/goid"
)

type loggerStack struct {
	loggers []*Logger
}

type logRedirect struct {
	mu struct {
		syncutil.Mutex
		logMap map[int64]*loggerStack
	}
	fallbackLogger  *Logger
	configured      bool
	cancelIntercept func()
}

var logRedirectInst = &logRedirect{}

var (
	goroutineRegex = regexp.MustCompile(`goroutine (\d+) .*:`)
	createdByRegex = regexp.MustCompile(`created by .* in goroutine (\d+)`)
)

// InitCRDBLogConfig sets up an interceptor for the CockroachDB log in order to
// redirect logs to the appropriate test log or the fallback log. This is
// necessary as the CockroachDB log is used in some test utilities shared
// between roachtest and CockroachDB. Generally, CockroachDB logs should not be
// used explicitly in roachtest.
func InitCRDBLogConfig(fallbackLogger *Logger) {
	logRedirectInst.mu.Lock()
	defer logRedirectInst.mu.Unlock()
	if logRedirectInst.configured {
		panic("internal error: CRDB log interceptor already configured")
	}
	if fallbackLogger == nil {
		panic("internal error: fallback logger is nil")
	}
	logRedirectInst.mu.logMap = make(map[int64]*loggerStack)
	logConf := logconfig.DefaultStderrConfig()
	logConf.Sinks.Stderr.Filter = logpb.Severity_FATAL
	// Disable logging to a file. File sinks write the application arguments to
	// the log by default (see: log_entry.go), and it is best to avoid logging
	// the roachtest arguments as it may contain sensitive information.
	if err := logConf.Validate(nil); err != nil {
		panic(fmt.Errorf("internal error: could not validate CRDB log config: %w", err))
	}
	if _, err := log.ApplyConfig(logConf, nil /* fileSinkMetricsForDir */, nil /* fatalOnLogStall */); err != nil {
		panic(fmt.Errorf("internal error: could not apply CRDB log config: %w", err))
	}
	logRedirectInst.cancelIntercept = log.InterceptWith(context.Background(), logRedirectInst)
	logRedirectInst.fallbackLogger = fallbackLogger
	logRedirectInst.configured = true
}

// TestingCRDBLogConfig is meant to be used in tests to reset the CRDB log, it's
// interceptor, and the redirect log config. This is necessary to avoid leaking
// state between tests.
func TestingCRDBLogConfig(fallbackLogger *Logger) {
	logRedirectInst.mu.Lock()
	defer logRedirectInst.mu.Unlock()
	if logRedirectInst.cancelIntercept != nil {
		logRedirectInst.cancelIntercept()
	}
	log.TestingResetActive()
	logRedirectInst = &logRedirect{}
	InitCRDBLogConfig(fallbackLogger)
}

// Intercept intercepts CockroachDB log entries and redirects it to the
// appropriate roachtest test logger or stderr. If no logger is found, the log
// entry is written to the fallback logger's stderr.
func (i *logRedirect) Intercept(logData []byte) {
	var entry logpb.Entry
	if err := json.Unmarshal(logData, &entry); err != nil {
		i.fallbackLogger.Errorf("failed to unmarshal intercepted log entry: %v", err)
	}
	l, err := i.resolveLogger(entry.Goroutine)
	if err != nil {
		i.fallbackLogger.Errorf("failed to resolve logger for goroutine %d: %v", entry.Goroutine, err)
		return
	}
	if l != nil && !l.Closed() {
		if entry.Severity == logpb.Severity_ERROR || entry.Severity == logpb.Severity_FATAL {
			i.writeLog(l.Stderr, entry)
			return
		}
		i.writeLog(l.Stdout, entry)
		return
	}
	// If no logger is found, write to the fallback logger's stderr.
	i.writeLog(i.fallbackLogger.Stderr, entry)
}

func (i *logRedirect) resolveLogger(gID int64) (*Logger, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if stack, ok := i.mu.logMap[gID]; ok {
		return stack.top(), nil
	}

	// Attempt to find the log bridge from an ancestor goroutine. If an ancestor
	// goroutine is found, cache the log bridge for the goroutine. The penalty
	// is only paid on the first call for an unknown goroutine.
	graph, err := goroutineGraph()
	if err != nil {
		return nil, err
	}
	for k := range graph {
		if int64(k) == gID {
			ancestorID, l := i.traverseAncestors(graph, k)
			if l != nil {
				// Cache the log bridge for the goroutine.
				i.mu.logMap[gID] = i.mu.logMap[ancestorID]
				return l, nil
			}
			break
		}
	}
	return nil, nil
}

func (i *logRedirect) traverseAncestors(graph map[int]int, goID int) (int64, *Logger) {
	ancestor, ok := graph[goID]
	if !ok {
		return 0, nil
	}
	ancestorID := int64(ancestor)
	if stack, ok := i.mu.logMap[ancestorID]; ok {
		return ancestorID, stack.top()
	}
	return i.traverseAncestors(graph, ancestor)
}

func (i *logRedirect) writeLog(dst io.Writer, entry logpb.Entry) {
	if err := log.FormatLegacyEntry(entry, dst); err != nil {
		i.fallbackLogger.Errorf("could not format and write CRDB log entry: %v", err)
	}
}

func (s *loggerStack) top() *Logger {
	return s.loggers[len(s.loggers)-1]
}

// BridgeCRDBLog bridges the CockroachDB log to the provided test logger. It
// uses the current goroutine ID to determine which logger to bridge to. If a
// call is made from a child goroutine an attempt will be made to find the
// ancestor goroutine and bridge to that logger. If no ancestor goroutine is
// found, usually from an orphaned goroutine, the log entry will be written to
// the default fallback logger.
// The returned function should be called, in order, to remove the bridge.
// Multiple calls in the same goroutine will bridge to the last logger, until
// the returned function is called, which pops the last logger from the stack
// for that goroutine.
func BridgeCRDBLog(l *Logger) func() {
	goID := goid.Get()

	logRedirectInst.mu.Lock()
	defer logRedirectInst.mu.Unlock()
	if !logRedirectInst.configured {
		panic(fmt.Errorf("internal error: CRDB log interceptor not configured"))
	}

	stack := logRedirectInst.mu.logMap[goID]
	if stack == nil {
		stack = &loggerStack{
			loggers: []*Logger{},
		}
		logRedirectInst.mu.logMap[goID] = stack
	}
	stack.loggers = append(stack.loggers, l)

	return func() {
		logRedirectInst.mu.Lock()
		defer logRedirectInst.mu.Unlock()
		stack.loggers = stack.loggers[:len(stack.loggers)-1]
		if len(stack.loggers) == 0 {
			delete(logRedirectInst.mu.logMap, goID)
		}
	}
}

// goroutineGraph is a simplistic goroutine stack parser that returns a graph of
// goroutines and their parent-child relationships. It is used to find the
// ancestor goroutine of a child goroutine.
func goroutineGraph() (map[int]int, error) {
	stacks := string(allstacks.Get())
	graph := make(map[int]int)
	parseID := func(s string) (int, error) {
		id, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("failed to parse goroutine ID: %w", err)
		}
		return id, nil
	}
	var err error
	var createdByID, curGoroutine = -1, -1
	lines := strings.Split(stacks, "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			curGoroutine = -1
			continue
		}
		if matches := goroutineRegex.FindStringSubmatch(line); matches != nil {
			if curGoroutine, err = parseID(matches[1]); err != nil {
				return nil, err
			}
			continue
		}
		if matches := createdByRegex.FindStringSubmatch(line); matches != nil && curGoroutine != -1 {
			if createdByID, err = parseID(matches[1]); err != nil {
				return nil, err
			}
			graph[curGoroutine] = createdByID
		}
	}
	return graph, nil
}
