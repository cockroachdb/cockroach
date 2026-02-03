// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provide logging infrastructure for the Task Execution Framework (TEF).
// This file defines the Logger interface and its default implementation, which wraps
// CockroachDB's logging infrastructure to provide consistent logging throughout TEF.
package planners

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Logger provides logging capabilities for TEF CLI operations.
// It wraps the CockroachDB logging infrastructure and provides a consistent
// interface for logging throughout the TEF codebase.
type Logger interface {
	// Infof logs an informational message
	Infof(ctx context.Context, format string, args ...interface{})

	// Warningf logs a warning message
	Warningf(ctx context.Context, format string, args ...interface{})

	// Errorf logs an error message
	Errorf(ctx context.Context, format string, args ...interface{})
}

// defaultLogger implements Logger using CockroachDB's log.Dev channel.
// By default, this logs to stdout/stderr.
type defaultLogger struct{}

// NewLogger creates a new Logger instance that logs to stdout/stderr.
func NewLogger() Logger {
	return &defaultLogger{}
}

// Infof logs an informational message to stdout via log.Dev channel.
func (l *defaultLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	log.Dev.Infof(ctx, format, args...)
}

// Warningf logs a warning message to stderr via log.Dev channel.
func (l *defaultLogger) Warningf(ctx context.Context, format string, args ...interface{}) {
	log.Dev.Warningf(ctx, format, args...)
}

// Errorf logs an error message to stderr via log.Dev channel.
func (l *defaultLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	log.Dev.Errorf(ctx, format, args...)
}

// contextKey is a private type for context keys to avoid collisions.
type contextKey string

// loggerKey is the context key for the TEF logger.
const loggerKey contextKey = "tef_logger"

// ContextWithLogger returns a new context with the logger attached.
func ContextWithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// LoggerFromContext retrieves the logger from the context.
// If no logger is found, it returns a default logger instance.
func LoggerFromContext(ctx context.Context) Logger {
	if logger, ok := ctx.Value(loggerKey).(Logger); ok {
		return logger
	}
	// Return a default logger if none is found in context
	return NewLogger()
}
