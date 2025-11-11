// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ui

import (
	"bytes"
	"io"
	"regexp"
	"strings"
)

// ansiRegex matches ANSI escape sequences
var ansiRegex = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

// BroadcastWriter is an io.Writer that writes to both an underlying writer
// and broadcasts to WebSocket clients.
type BroadcastWriter struct {
	underlying  io.Writer
	broadcaster LogBroadcaster
	buffer      bytes.Buffer
	lines       []string
}

// NewBroadcastWriter creates a new BroadcastWriter.
func NewBroadcastWriter(underlying io.Writer, broadcaster LogBroadcaster) *BroadcastWriter {
	return &BroadcastWriter{
		underlying:  underlying,
		broadcaster: broadcaster,
		lines:       make([]string, 0),
	}
}

// Write writes data to both the underlying writer and broadcasts to WebSocket clients.
func (bw *BroadcastWriter) Write(p []byte) (n int, err error) {
	// Write to underlying writer first (with ANSI codes for terminal)
	n, err = bw.underlying.Write(p)

	// Process output for WebSocket broadcast
	if bw.broadcaster != nil && len(p) > 0 {
		bw.processOutput(p)
	}

	return n, err
}

// processOutput handles ANSI escape sequences and manages line state for clean WebSocket output
func (bw *BroadcastWriter) processOutput(p []byte) {
	text := string(p)

	// Process character by character to handle ANSI codes
	i := 0
	for i < len(text) {
		if text[i] == '\x1b' && i+1 < len(text) && text[i+1] == '[' {
			// Found ANSI escape sequence - skip it
			j := i + 2
			for j < len(text) && !((text[j] >= 'A' && text[j] <= 'Z') || (text[j] >= 'a' && text[j] <= 'z')) {
				j++
			}
			if j < len(text) {
				i = j + 1
				continue
			}
		} else if text[i] == '\r' {
			// Carriage return - skip it
			i++
			continue
		} else if text[i] == '\n' {
			// Newline - process buffered content
			line := strings.TrimSpace(bw.buffer.String())
			if line != "" {
				bw.processLine(line)
			}
			bw.buffer.Reset()
			i++
		} else {
			// Regular character
			bw.buffer.WriteByte(text[i])
			i++
		}
	}
}

// processLine handles a complete line of output
func (bw *BroadcastWriter) processLine(line string) {
	// Check if this is a spinner line (node status)
	if strings.Contains(line, ": copying") || strings.Contains(line, ": done") {
		bw.updateSpinnerLine(line)
	} else {
		// Regular line - broadcast immediately
		bw.broadcaster.Broadcast(line + "\n")
	}
}

// updateSpinnerLine updates the spinner state for a node and broadcasts state updates
func (bw *BroadcastWriter) updateSpinnerLine(line string) {
	// Parse node number from line like "   1: copying |" or "   1: done"
	parts := strings.SplitN(strings.TrimSpace(line), ":", 2)
	if len(parts) != 2 {
		return
	}

	nodeNum := strings.TrimSpace(parts[0])
	status := strings.TrimSpace(parts[1])

	// Determine state: "copying" or "done"
	state := "copying"
	if strings.HasPrefix(status, "done") {
		state = "done"
	}

	// Update or append this node's line
	found := false
	for i, existing := range bw.lines {
		if strings.HasPrefix(strings.TrimSpace(existing), nodeNum+":") {
			oldState := "copying"
			if strings.Contains(existing, "done") {
				oldState = "done"
			}

			bw.lines[i] = nodeNum + ": " + state

			// Only broadcast if state changed
			if oldState != state {
				bw.broadcastSpinnerState()
			}
			found = true
			break
		}
	}

	if !found {
		bw.lines = append(bw.lines, nodeNum+": "+state)
		bw.broadcastSpinnerState()
	}
}

// broadcastSpinnerState sends current spinner state as JSON to the frontend
func (bw *BroadcastWriter) broadcastSpinnerState() {
	if len(bw.lines) == 0 {
		return
	}

	// Build state map
	states := make(map[string]string)
	allDone := true

	for _, line := range bw.lines {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			nodeNum := strings.TrimSpace(parts[0])
			state := strings.TrimSpace(parts[1])
			states[nodeNum] = state
			if state != "done" {
				allDone = false
			}
		}
	}

	// Send state update as special marker the frontend can detect
	var sb strings.Builder
	sb.WriteString("__SPINNER_STATE__")
	for node, state := range states {
		sb.WriteString(node)
		sb.WriteString("=")
		sb.WriteString(state)
		sb.WriteString(";")
	}
	sb.WriteString("\n")

	bw.broadcaster.Broadcast(sb.String())

	// Clear state when all done
	if allDone {
		bw.lines = make([]string, 0)
	}
}
