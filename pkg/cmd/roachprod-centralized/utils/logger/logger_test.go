// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logger

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSink records entries written through the LogSink interface.
type mockSink struct {
	entries  []LogEntry
	closed   bool
	writeErr error // when non-nil, WriteEntry returns this error
}

func (s *mockSink) WriteEntry(entry LogEntry) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	s.entries = append(s.entries, entry)
	return nil
}

func (s *mockSink) Close() error {
	s.closed = true
	return nil
}

func TestLineWriter_BuffersByNewline(t *testing.T) {
	sink := &mockSink{}
	l := NewLogger("info").WithSink(sink)
	w := l.NewLineWriter(slog.LevelInfo)

	n, err := w.Write([]byte("line1\nline2\nline3\n"))
	require.NoError(t, err)
	assert.Equal(t, 18, n)

	require.Len(t, sink.entries, 3)
	assert.Equal(t, "line1", sink.entries[0].Message)
	assert.Equal(t, "line2", sink.entries[1].Message)
	assert.Equal(t, "line3", sink.entries[2].Message)
}

func TestLineWriter_PartialLines(t *testing.T) {
	sink := &mockSink{}
	l := NewLogger("info").WithSink(sink)
	w := l.NewLineWriter(slog.LevelInfo)

	// Write partial line — should not be forwarded yet.
	_, err := w.Write([]byte("partial"))
	require.NoError(t, err)
	assert.Empty(t, sink.entries)

	// Complete the line.
	_, err = w.Write([]byte(" line\n"))
	require.NoError(t, err)
	require.Len(t, sink.entries, 1)
	assert.Equal(t, "partial line", sink.entries[0].Message)
}

func TestLineWriter_CloseFlushesPartial(t *testing.T) {
	sink := &mockSink{}
	l := NewLogger("info").WithSink(sink)
	w := l.NewLineWriter(slog.LevelInfo)

	_, err := w.Write([]byte("no newline"))
	require.NoError(t, err)
	assert.Empty(t, sink.entries)

	require.NoError(t, w.Close())
	require.Len(t, sink.entries, 1)
	assert.Equal(t, "no newline", sink.entries[0].Message)
}

func TestLineWriter_CloseEmptyBuffer(t *testing.T) {
	sink := &mockSink{}
	l := NewLogger("info").WithSink(sink)
	w := l.NewLineWriter(slog.LevelInfo)

	require.NoError(t, w.Close())
	assert.Empty(t, sink.entries)
}

func TestLineWriter_SinkEntryFields(t *testing.T) {
	sink := &mockSink{}
	l := NewLogger("info").WithSink(sink)
	w := l.NewLineWriter(slog.LevelInfo, slog.String("stream", "stdout"))

	_, err := w.Write([]byte("hello\n"))
	require.NoError(t, err)

	require.Len(t, sink.entries, 1)
	entry := sink.entries[0]
	assert.Equal(t, "hello", entry.Message)
	assert.Equal(t, "INFO", entry.Level)
	assert.Equal(t, "stdout", entry.Attrs["stream"])
	assert.False(t, entry.Timestamp.IsZero())
}

func TestLineWriter_EmptyLinesSkipped(t *testing.T) {
	sink := &mockSink{}
	l := NewLogger("info").WithSink(sink)
	w := l.NewLineWriter(slog.LevelInfo)

	_, err := w.Write([]byte("\n\nline\n\n"))
	require.NoError(t, err)
	require.Len(t, sink.entries, 1)
	assert.Equal(t, "line", sink.entries[0].Message)
}

func TestLineWriter_SinkErrorDoesNotFailWrite(t *testing.T) {
	sink := &mockSink{writeErr: fmt.Errorf("gcs unavailable")}
	l := NewLogger("info").WithSink(sink)
	w := l.NewLineWriter(slog.LevelInfo)

	// Write should succeed even though the sink returns an error.
	n, err := w.Write([]byte("hello\n"))
	require.NoError(t, err)
	assert.Equal(t, 6, n)
}

func TestLineWriter_NilSink(t *testing.T) {
	// Logger without sink — should work without panicking.
	l := NewLogger("info")
	w := l.NewLineWriter(slog.LevelInfo)

	n, err := w.Write([]byte("hello\n"))
	require.NoError(t, err)
	assert.Equal(t, 6, n)
	require.NoError(t, w.Close())
}

func TestLineWriter_MultipleWrites(t *testing.T) {
	sink := &mockSink{}
	l := NewLogger("info").WithSink(sink)
	w := l.NewLineWriter(slog.LevelInfo)

	// Simulate streaming output arriving in small chunks.
	chunks := []string{"hel", "lo ", "wor", "ld\ngood", "bye\n"}
	for _, chunk := range chunks {
		_, err := w.Write([]byte(chunk))
		require.NoError(t, err)
	}

	require.Len(t, sink.entries, 2)
	assert.Equal(t, "hello world", sink.entries[0].Message)
	assert.Equal(t, "goodbye", sink.entries[1].Message)
}
