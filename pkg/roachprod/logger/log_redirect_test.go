// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logger

import (
	"context"
	"strings"
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
	l := newMockLogger(t)
	TestingCRDBLogConfig(l.logger)
	ctx := context.Background()

	log.Infof(ctx, "[simple test]")
	requireLine(t, l, "[simple test]")
	require.Equal(t, 1, len(l.writer.lines))
}
