// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provide logging infrastructure for the Task Execution Framework (TEF).
// This file defines the Logger implementation using Go's structured logging (slog) package.
// The Logger wraps slog.Logger and implements io.Writer for interoperability with legacy code.
//
// This implementation follows the pattern from pkg/cmd/roachprod-centralized/utils/logger/logger.go,
// providing structured logging with attributes for tracing execution across tasks, plans, and workflows.
package planners

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// Logger wraps slog.Logger and implements io.Writer for interoperability with legacy code.
// It provides structured logging with attributes, enabling easy tracking of execution context
// across tasks, plans, and workflows.
type Logger struct {
	*slog.Logger
	LogLevel slog.Level
}

// NewLogger creates a new Logger instance with JSON output to stdout.
// The logLevel parameter accepts: "debug", "info", "warn", "error" (case-insensitive).
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

// With creates a child logger with the specified attributes.
// Attributes are inherited by all log calls on the child logger, enabling
// contextual logging (e.g., workflow_id, task_name, plan_variant).
func (l *Logger) With(attrs ...slog.Attr) *Logger {
	return &Logger{
		LogLevel: l.LogLevel,
		Logger:   slog.New(l.Logger.Handler().WithAttrs(attrs)),
	}
}

// Write implements the io.Writer interface for interoperability with legacy code
// that expects an io.Writer (e.g., CockroachDB logger configs).
// Messages are logged at Info level by default, with trailing newlines removed.
func (l *Logger) Write(p []byte) (n int, err error) {
	length := len(p)
	message := strings.TrimSuffix(string(p), "\n")
	l.Logger.Log(context.Background(), slog.LevelInfo, message)
	return length, nil
}

// contextKey is a private type for context keys to avoid collisions.
type contextKey string

// loggerKey is the context key for the TEF logger.
const loggerKey contextKey = "tef_logger"

// ContextWithLogger returns a new context with the logger attached.
func ContextWithLogger(ctx context.Context, logger *Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// LoggerFromContext retrieves the logger from the context.
// If no logger is found, it returns a default logger instance with info level.
func LoggerFromContext(ctx context.Context) *Logger {
	if logger, ok := ctx.Value(loggerKey).(*Logger); ok {
		return logger
	}
	// Return a default logger if none is found in context
	return NewLogger("info")
}
