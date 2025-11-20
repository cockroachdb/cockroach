// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logger

import (
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
type Logger struct {
	*slog.Logger
	LogLevel slog.Level
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
