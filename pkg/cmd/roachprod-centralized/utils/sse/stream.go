// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sse

import (
	"bufio"
	"io"
	"log/slog"

	"github.com/gin-gonic/gin"
)

// Stream reads newline-delimited data from reader and streams each line
// as an SSE event. Sends "event: done" when the reader returns io.EOF,
// or "event: error" if an error occurs.
//
// Gin SSE mechanics:
//   - Sets Content-Type: text/event-stream and related headers
//   - c.Stream() runs a callback in a loop; returning false exits
//   - c.SSEvent(event, data) writes formatted "event: <event>\ndata: <data>\n\n"
//   - Gin flushes the response writer after each c.Stream() iteration
//
// The reader is expected to block when no data is available (e.g., io.Pipe).
// When the reader returns io.EOF, the stream sends a done event and closes.
// Client disconnects are detected via c.Request.Context().
//
// When l is non-nil, stream errors are logged server-side and a generic
// message is sent to the client to avoid leaking internal details.
func Stream(c *gin.Context, reader io.ReadCloser, l *slog.Logger) {
	defer reader.Close()

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Transfer-Encoding", "chunked")

	scanner := bufio.NewScanner(reader)
	// Increase the default 64 KiB token limit to handle long log lines
	// (e.g., verbose task output) without triggering "token too long" errors.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	c.Stream(func(w io.Writer) bool {
		// scanner.Scan() blocks until a line is available or the reader closes.
		// When the context is cancelled, the pipe writer closes with an error,
		// which causes Scan() to return false.
		if scanner.Scan() {
			c.SSEvent("log", scanner.Text())
			return true
		}
		// Reader closed. Check if it was a clean EOF or an error.
		if err := scanner.Err(); err != nil {
			if l != nil {
				l.Warn("SSE stream error", slog.Any("error", err))
			}
			c.SSEvent("error", "log stream error")
		} else {
			c.SSEvent("done", "{}")
		}
		return false
	})
}
