// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logger

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

type mockLogger struct {
	logger *Logger
	writer *mockWriter
}

type mockWriter struct {
	lines []string
}

func (w *mockWriter) Write(p []byte) (n int, err error) {
	w.lines = append(w.lines, string(p))
	return len(p), nil
}

func newMockLogger(t *testing.T) *mockLogger {
	writer := &mockWriter{}
	logConf := Config{Stdout: writer, Stderr: writer}
	l, err := logConf.NewLogger("" /* path */)
	require.NoError(t, err)
	return &mockLogger{logger: l, writer: writer}
}

func requireLine(t *testing.T, l *mockLogger, line string) {
	t.Helper()
	found := false

	for _, logLine := range l.writer.lines {
		if strings.Contains(logLine, line) {
			found = true
			break
		}
	}
	require.True(t, found, "expected line not found: %s", line)
}

func TestLogRedirect(t *testing.T) {
	TestingCRDBLogConfig(newMockLogger(t).logger)
	ctx := context.Background()
	l := newMockLogger(t)
	defer BridgeCRDBLog(l.logger)()
	log.Infof(ctx, "[simple test]")
	requireLine(t, l, "[simple test]")
}

func TestLogNestedGoroutines(t *testing.T) {
	TestingCRDBLogConfig(newMockLogger(t).logger)
	ctx := context.Background()
	l := newMockLogger(t)
	defer BridgeCRDBLog(l.logger)()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		wgN := sync.WaitGroup{}
		wgN.Add(1)
		go func() {
			defer wgN.Done()
			log.Infof(ctx, "[simple nested]")
		}()
		wgN.Wait()
	}()

	wg.Wait()
	requireLine(t, l, "[simple nested]")
}

func TestLogRedirectScope(t *testing.T) {
	TestingCRDBLogConfig(newMockLogger(t).logger)
	ctx := context.Background()
	l1, l2, l2stack, l3 := newMockLogger(t), newMockLogger(t), newMockLogger(t), newMockLogger(t)

	defer BridgeCRDBLog(l1.logger)()
	wg1 := sync.WaitGroup{}
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		log.Infof(ctx, "[level 1 log]")
		wg2 := sync.WaitGroup{}
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			defer BridgeCRDBLog(l2.logger)()
			log.Infof(ctx, "[level 2 log]")
			defer BridgeCRDBLog(l2stack.logger)()
			log.Infof(ctx, "[level 2 log stack]")
			wg3 := sync.WaitGroup{}
			wg3.Add(1)
			go func() {
				defer wg3.Done()
				endScope := BridgeCRDBLog(l3.logger)
				log.Infof(ctx, "[level 3 log]")
				endScope()
				log.Infof(ctx, "[level 2 log stack 2]")
			}()
			wg3.Wait()
		}()
		wg2.Wait()
	}()
	wg1.Wait()
	requireLine(t, l1, "[level 1 log]")
	requireLine(t, l2, "[level 2 log]")
	requireLine(t, l2stack, "[level 2 log stack]")
	requireLine(t, l2stack, "[level 2 log stack 2]")
	requireLine(t, l3, "[level 3 log]")
}

func TestFallbackLog(t *testing.T) {
	fallbackLog := newMockLogger(t)
	TestingCRDBLogConfig(fallbackLog.logger)
	ctx := context.Background()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Infof(ctx, "[fallback]")
	}()
	wg.Wait()
	requireLine(t, fallbackLog, "[fallback]")
}
