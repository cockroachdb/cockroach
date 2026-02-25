// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logger

import "time"

// LogEntry is a structured log line for external storage (GCS, memory, etc.).
type LogEntry struct {
	Timestamp time.Time         `json:"ts"`
	Level     string            `json:"level"`
	Message   string            `json:"msg"`
	Attrs     map[string]string `json:"attrs,omitempty"`
}

// LogSink receives structured log entries for external persistence.
// A single LineWriter owns its sink; no concurrent Write calls occur.
type LogSink interface {
	// WriteEntry sends a single log entry to the sink.
	WriteEntry(entry LogEntry) error
	// Close flushes buffered data and releases resources.
	Close() error
}
