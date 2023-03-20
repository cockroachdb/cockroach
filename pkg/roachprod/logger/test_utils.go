// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logger

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockLogger struct {
	Logger *Logger
	writer *mockWriter
}

type mockWriter struct {
	lines []string
}

func (w *mockWriter) Write(p []byte) (n int, err error) {
	w.lines = append(w.lines, string(p))
	return len(p), nil
}

// Creates a logger whose entire output (stdout/stderr included) is redirected to mockWriter.
func NewMockLogger(t *testing.T) *mockLogger {
	writer := &mockWriter{}
	logConf := Config{Stdout: writer, Stderr: writer}
	l, err := logConf.NewLogger("")
	require.NoError(t, err)
	return &mockLogger{Logger: l, writer: writer}
}

func RequireLine(t *testing.T, l *mockLogger, line string) {
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

func RequireEqual(t *testing.T, l *mockLogger, expectedLines []string) {
	require.Equal(t, expectedLines, l.writer.lines)
}
