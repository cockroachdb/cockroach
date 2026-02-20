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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client"
	"github.com/cockroachdb/errors"
)

// logEntry is the JSONL structure written by the task logger's LogSink.
type logEntry struct {
	Timestamp string `json:"ts"`
	Level     string `json:"level"`
	Message   string `json:"msg"`
}

// streamSSELogs connects to the task logs SSE endpoint and prints log
// messages to stdout until the stream ends or ctx is cancelled.
func streamSSELogs(ctx context.Context, c *client.Client, taskID string, offset int) error {
	resp, err := c.StreamTaskLogs(ctx, taskID, offset)
	if err != nil {
		return errors.Wrap(err, "open log stream")
	}
	defer resp.Body.Close()
	return processSSEStream(ctx, resp.Body)
}

// processSSEStream reads SSE events from reader and prints log entries.
func processSSEStream(ctx context.Context, reader io.Reader) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var currentEvent string
	var dataLines []string

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
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
				case "done":
					return nil
				case "error":
					return errors.Newf("log stream error: %s", data)
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
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "read SSE stream")
	}
	return nil
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

// printLogEntry parses a JSONL log line and prints the message.
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
	fmt.Printf("[%s] %s: %s\n", ts, strings.ToUpper(entry.Level), entry.Message)
}
