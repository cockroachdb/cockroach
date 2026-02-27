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
//
// When a LogSink is attached via WithSink, every log call (Info, Warn,
// Error, …) is also forwarded to the sink as a structured LogEntry.
// This is implemented by wrapping the underlying slog.Handler with a
// sinkHandler, so all paths through slog converge to the sink.
type Logger struct {
	*slog.Logger
	LogLevel slog.Level
	sink     LogSink // nil unless explicitly set via WithSink
}

// sinkHandler wraps an slog.Handler and mirrors every log record to a
// LogSink as a structured LogEntry. This ensures that standard logger
// calls (Info, Error, …) reach the sink, not only LineWriter output.
type sinkHandler struct {
	inner slog.Handler
	sink  LogSink
	attrs []slog.Attr // handler-level attrs accumulated via WithAttrs
}

var _ slog.Handler = (*sinkHandler)(nil)

func (h *sinkHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *sinkHandler) Handle(ctx context.Context, r slog.Record) error {
	// Forward to the wrapped handler (stdout JSON, etc.).
	if err := h.inner.Handle(ctx, r); err != nil {
		return err
	}

	// Mirror to sink (best-effort, errors silently ignored to avoid
	// recursive logging).
	entry := LogEntry{
		Timestamp: r.Time.UTC(),
		Level:     r.Level.String(),
		Message:   r.Message,
	}
	totalAttrs := len(h.attrs) + r.NumAttrs()
	if totalAttrs > 0 {
		entry.Attrs = make(map[string]string, totalAttrs)
		for _, a := range h.attrs {
			entry.Attrs[a.Key] = a.Value.String()
		}
		r.Attrs(func(a slog.Attr) bool {
			entry.Attrs[a.Key] = a.Value.String()
			return true
		})
	}
	_ = h.sink.WriteEntry(entry)
	return nil
}

func (h *sinkHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	combined := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(combined, h.attrs)
	copy(combined[len(h.attrs):], attrs)
	return &sinkHandler{
		inner: h.inner.WithAttrs(attrs),
		sink:  h.sink,
		attrs: combined,
	}
}

func (h *sinkHandler) WithGroup(name string) slog.Handler {
	return &sinkHandler{
		inner: h.inner.WithGroup(name),
		sink:  h.sink,
		attrs: h.attrs,
	}
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

// WithSink returns a copy of the logger with the given LogSink attached.
// All subsequent log calls (Info, Warn, Error, …) and LineWriter output
// will be mirrored to the sink as structured LogEntry records.
func (l *Logger) WithSink(s LogSink) *Logger {
	handler := &sinkHandler{
		inner: l.Logger.Handler(),
		sink:  s,
	}
	return &Logger{
		LogLevel: l.LogLevel,
		Logger:   slog.New(handler),
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
//
// Sink forwarding is handled transparently by the sinkHandler installed
// via WithSink; LineWriter does not interact with the sink directly.
type LineWriter struct {
	logger *Logger
	level  slog.Level
	attrs  []slog.Attr
	buf    []byte
}

// NewLineWriter returns an io.WriteCloser that buffers subprocess output
// by newline and logs each complete line at the given level with the
// provided attributes.
func (l *Logger) NewLineWriter(level slog.Level, attrs ...slog.Attr) *LineWriter {
	return &LineWriter{logger: l, level: level, attrs: attrs}
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
		w.buf = nil
	}
	return nil
}
