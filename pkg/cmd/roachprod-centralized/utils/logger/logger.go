// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logger

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"time"
)

var (
	// DefaultLogger is the default logger used by the application
	DefaultLogger = &Logger{
		Logger: slog.Default(),
	}
)

// Logger is simply a wrapper around slog.Logger that implements
// the io.Writer interface. This allows to use slog and its attributes
// for the code that relies on the CockroachDB logger.
type Logger struct {
	*slog.Logger
	LogLevel slog.Level
	sink     LogSink // nil unless explicitly set via WithSink
}

// NewLogger creates a new Logger instance with the default logger
func NewLogger(logLevel string) *Logger {

	slogLevel := slog.LevelInfo
	switch strings.ToLower(logLevel) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "info":
		slogLevel = slog.LevelInfo
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	}
	return &Logger{
		LogLevel: slogLevel,
		Logger: slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
				Level: slogLevel,
			}),
		),
	}
}

func (l *Logger) With(attrs ...slog.Attr) *Logger {
	return &Logger{
		LogLevel: l.LogLevel,
		Logger:   slog.New(l.Logger.Handler().WithAttrs(attrs)),
		sink:     l.sink,
	}
}

// WithSink returns a shallow copy of the logger with the given LogSink attached.
// LineWriters created from this logger will forward entries to the sink.
func (l *Logger) WithSink(s LogSink) *Logger {
	return &Logger{
		LogLevel: l.LogLevel,
		Logger:   l.Logger,
		sink:     s,
	}
}

// Write implements the io.Writer interface.
// It writes the data to the slog.Logger with appropriate log level.
// GIN debug messages (starting with "[GIN-debug]") are logged at Debug level,
// while other messages are logged at Info level.
// It also removes the trailing newline character from the input data.
func (l *Logger) Write(p []byte) (n int, err error) {
	length := len(p)
	message := strings.TrimSuffix(string(p), "\n")

	// Log GIN debug messages at debug level
	logLevel := slog.LevelInfo
	if strings.HasPrefix(message, "[GIN-debug]") {
		logLevel = slog.LevelDebug
	}

	l.Logger.Log(
		context.Background(),
		logLevel,
		message,
	)
	return length, nil
}

// LineWriter is an io.WriteCloser that buffers writes by newline and logs
// each complete line as a structured log entry. Call Close to flush any
// remaining partial line. This is useful for streaming subprocess output
// to the logger line-by-line in real time.
type LineWriter struct {
	logger *Logger
	level  slog.Level
	attrs  []slog.Attr
	buf    []byte
	sink   LogSink // inherited from logger, may be nil
}

// NewLineWriter returns an io.WriteCloser that buffers subprocess output
// by newline and logs each complete line at the given level with the
// provided attributes.
func (l *Logger) NewLineWriter(level slog.Level, attrs ...slog.Attr) *LineWriter {
	return &LineWriter{logger: l, level: level, attrs: attrs, sink: l.sink}
}

// Write buffers incoming data and logs each complete line.
func (w *LineWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		idx := bytes.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}
		line := string(w.buf[:idx])
		w.buf = w.buf[idx+1:]
		if line != "" {
			w.logger.LogAttrs(context.Background(), w.level, line, w.attrs...)
			w.writeToSink(line)
		}
	}
	return len(p), nil
}

// Close flushes any remaining buffered content (incomplete last line).
// The sink is NOT closed here — the task executor is responsible for
// closing the sink, since it is shared across multiple LineWriters
// (stdout + stderr).
func (w *LineWriter) Close() error {
	if len(w.buf) > 0 {
		line := string(w.buf)
		w.logger.LogAttrs(context.Background(), w.level, line, w.attrs...)
		w.writeToSink(line)
		w.buf = nil
	}
	return nil
}

// writeToSink forwards a log line to the sink if one is configured.
// Best-effort: logs a warning but does not fail the write.
func (w *LineWriter) writeToSink(line string) {
	if w.sink == nil {
		return
	}
	entry := LogEntry{
		Timestamp: time.Now().UTC(),
		Level:     w.level.String(),
		Message:   line,
	}
	if len(w.attrs) > 0 {
		entry.Attrs = make(map[string]string, len(w.attrs))
		for _, a := range w.attrs {
			entry.Attrs[a.Key] = a.Value.String()
		}
	}
	if sinkErr := w.sink.WriteEntry(entry); sinkErr != nil {
		w.logger.Warn("log sink write failed", slog.Any("error", sinkErr))
	}
}
