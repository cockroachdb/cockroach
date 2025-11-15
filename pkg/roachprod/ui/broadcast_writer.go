// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ui

import (
	"io"
	"regexp"
)

// ansiRegex matches ANSI escape sequences
var ansiRegex = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]|\r`)

// BroadcastWriter is an io.Writer that writes to both an underlying writer
// and broadcasts to WebSocket clients.
type BroadcastWriter struct {
	underlying  io.Writer
	broadcaster LogBroadcaster
}

// NewBroadcastWriter creates a new BroadcastWriter.
func NewBroadcastWriter(underlying io.Writer, broadcaster LogBroadcaster) *BroadcastWriter {
	return &BroadcastWriter{
		underlying:  underlying,
		broadcaster: broadcaster,
	}
}

// Write writes data to both the underlying writer and broadcasts to WebSocket clients.
func (bw *BroadcastWriter) Write(p []byte) (n int, err error) {
	// Write to underlying writer first (with ANSI codes for terminal)
	n, err = bw.underlying.Write(p)

	// Broadcast to WebSocket clients (strip ANSI codes since browsers don't handle them)
	if bw.broadcaster != nil && len(p) > 0 {
		// Strip ANSI escape sequences and carriage returns
		cleanText := ansiRegex.ReplaceAllString(string(p), "")
		if len(cleanText) > 0 {
			bw.broadcaster.Broadcast(cleanText)
		}
	}

	return n, err
}
