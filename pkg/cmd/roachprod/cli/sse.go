// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client"
	"github.com/cockroachdb/errors"
)

// maxConsecutiveReconnects is the maximum number of reconnection attempts
// with zero log entries before giving up. Each successful reconnect that
// returns at least one entry resets the counter.
const maxConsecutiveReconnects = 10

// reconnectDelay is the pause between SSE reconnection attempts.
const reconnectDelay = 1 * time.Second

// logEntry is the JSONL structure written by the task logger's LogSink.
type logEntry struct {
	Timestamp string            `json:"ts"`
	Level     string            `json:"level"`
	Message   string            `json:"msg"`
	Attrs     map[string]string `json:"attrs,omitempty"`
}

// streamSSELogs connects to the task logs SSE endpoint and prints log
// messages to stdout until the stream ends or ctx is cancelled.
//
// If the SSE connection drops before the server sends a "done" event
// (e.g. due to a proxy request-duration limit), the client reconnects
// from the last received offset and resumes streaming. This makes log
// tailing resilient to intermediate proxies with strict timeouts.
func streamSSELogs(ctx context.Context, c *client.Client, taskID string, offset int) error {
	currentOffset := offset
	emptyReconnects := 0

	for {
		resp, err := c.StreamTaskLogs(ctx, taskID, currentOffset)
		if err != nil {
			return errors.Wrap(err, "open log stream")
		}

		entriesRead, completed, streamErr := processSSEStream(ctx, resp.Body)
		resp.Body.Close()
		currentOffset += entriesRead

		if streamErr != nil {
			return streamErr
		}
		if completed {
			return nil
		}

		// Stream ended without a done event — connection was dropped.
		// Track consecutive empty reconnects to avoid infinite loops when
		// the stream repeatedly fails without delivering data.
		if entriesRead > 0 {
			emptyReconnects = 0
		} else {
			emptyReconnects++
			if emptyReconnects >= maxConsecutiveReconnects {
				return errors.New("log stream: too many reconnects without new data")
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectDelay):
		}
	}
}

// processSSEStream reads SSE events from reader, prints log entries,
// and returns the number of log entries processed and whether the stream
// completed normally (via a "done" event).
//
// Return semantics:
//   - completed=true, err=nil: server sent "done" event, stream finished
//   - completed=false, err=nil: reader hit EOF without "done" (connection drop)
//   - err != nil: server sent "error" event or scanner failed
func processSSEStream(
	ctx context.Context, reader io.Reader,
) (entriesRead int, completed bool, err error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var currentEvent string
	var dataLines []string

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return entriesRead, false, ctx.Err()
		default:
		}

		line := scanner.Text()
		if line == "" {
			// Empty line = end of SSE event frame.
			if currentEvent != "" {
				data := strings.Join(dataLines, "\n")
				switch currentEvent {
				case "log":
					printLogEntry(data)
					entriesRead++
				case "done":
					return entriesRead, true, nil
				case "error":
					return entriesRead, false, errors.Newf("log stream error: %s", data)
				}
			}
			currentEvent = ""
			dataLines = nil
			continue
		}
		field, value := parseSSEField(line)
		switch field {
		case "event":
			currentEvent = value
		case "data":
			dataLines = append(dataLines, value)
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return entriesRead, false, errors.Wrap(scanErr, "read SSE stream")
	}
	return entriesRead, false, nil
}

// parseSSEField splits an SSE line into field name and value. Per the SSE
// spec, the format is "field:value" where a single leading space after the
// colon is optional and stripped if present.
func parseSSEField(line string) (string, string) {
	idx := strings.IndexByte(line, ':')
	if idx < 0 {
		return "", ""
	}
	field := line[:idx]
	value := line[idx+1:]
	// Strip optional single leading space per SSE spec.
	value = strings.TrimPrefix(value, " ")
	return field, value
}

// printLogEntry parses a JSONL log line and prints the message with
// any structured attributes appended as key=value pairs.
// Falls back to printing raw data if JSON parsing fails.
func printLogEntry(data string) {
	var entry logEntry
	if err := json.Unmarshal([]byte(data), &entry); err != nil {
		fmt.Println(data)
		return
	}
	ts := entry.Timestamp
	if parsed, parseErr := time.Parse(time.RFC3339Nano, ts); parseErr == nil {
		ts = parsed.Format("15:04:05")
	}
	if len(entry.Attrs) > 0 {
		fmt.Printf("[%s] %s: %s %s\n",
			ts, strings.ToUpper(entry.Level), entry.Message,
			formatAttrs(entry.Attrs),
		)
	} else {
		fmt.Printf("[%s] %s: %s\n", ts, strings.ToUpper(entry.Level), entry.Message)
	}
}

// formatAttrs formats a map of log attributes as a parenthesized,
// comma-separated list of key=value pairs, sorted for deterministic output.
func formatAttrs(attrs map[string]string) string {
	// Deterministic order: collect keys and sort.
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+attrs[k])
	}
	return "(" + strings.Join(parts, ", ") + ")"
}
