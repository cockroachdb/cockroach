// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package recording

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

// Recording can be used to play back a set of operations (defined only by a
// "command" and an "expected output"). It provides a handy way to mock out the
// components being recorded.
type Recording struct {
	// scanner is where we're replaying the recording from. op is the
	// scratch space used to parse out the current operation being read.
	scanner *scanner
	op      Operation
}

// WithReplayFrom is used to configure a Recording to play back from the given
// reader. The provided name is used only for diagnostic purposes, it's
// typically the name of the file being read.
func WithReplayFrom(r io.Reader, name string) *Recording {
	re := &Recording{}
	re.scanner = newScanner(r, name)
	return re
}

// Next is used to step through the next operation found in the recording, if
// any.
func (r *Recording) Next(f func(Operation) error) (found bool, err error) {
	parsed, err := r.parseOperation()
	if err != nil {
		return false, err
	}

	if !parsed {
		return false, nil
	}

	if err := f(r.op); err != nil {
		return false, fmt.Errorf("%s: %w", r.scanner.pos(), err)
	}
	return true, nil
}

// parseOperation parses out the next Operation from the internal scanner. See
// type-level comment on Operation to understand the grammar we're parsing
// against.
func (r *Recording) parseOperation() (parsed bool, err error) {
	for r.scanner.Scan() {
		r.op = Operation{}
		line := r.scanner.Text()

		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			// Skip comment lines.
			continue
		}

		// Support wrapping command directive lines using "\".
		for strings.HasSuffix(line, `\`) && r.scanner.Scan() {
			nextLine := r.scanner.Text()
			line = strings.TrimSuffix(line, `\`)
			line = strings.TrimSpace(line)
			line = fmt.Sprintf("%s %s", line, strings.TrimSpace(nextLine))
		}

		command, err := r.parseCommand(line)
		if err != nil {
			return false, err
		}
		if command == "" {
			// Nothing to do here.
			continue
		}
		r.op.Command = command

		if err := r.parseSeparator(); err != nil {
			return false, err
		}

		if err := r.parseOutput(); err != nil {
			return false, err
		}

		return true, nil
	}
	return false, nil
}

// parseCommand parses a <command> line and returns it if parsed correctly. See
// type-level comment on Operation to understand the grammar we're parsing
// against.
func (r *Recording) parseCommand(line string) (cmd string, err error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", nil
	}

	origLine := line
	cmd = strings.TrimSpace(line)
	if cmd == "" {
		column := len(origLine) - len(line) + 1
		return "", fmt.Errorf("%s: cannot parse command at col %d: %s", r.scanner.pos(), column, origLine)
	}
	return cmd, nil
}

// parseSeparator parses a separator ('----'), erroring out if it's not parsed
// correctly. See type-level comment on Operation to understand the grammar
// we're parsing against.
func (r *Recording) parseSeparator() error {
	if !r.scanner.Scan() {
		return fmt.Errorf("%s: expected to find separator after command", r.scanner.pos())
	}
	line := r.scanner.Text()
	if line != "----" {
		return fmt.Errorf("%s: expected to find separator after command, found %q instead", r.scanner.pos(), line)
	}
	return nil
}

// parseOutput parses an <output>. See type-level comment on Operation to
// understand the grammar we're parsing against.
func (r *Recording) parseOutput() error {
	var buf bytes.Buffer
	var line string

	var allowBlankLines bool
	if r.scanner.Scan() {
		line = r.scanner.Text()
		if line == "----" {
			allowBlankLines = true
		}
	}

	if !allowBlankLines {
		// Terminate on first blank line.
		for {
			if strings.TrimSpace(line) == "" {
				break
			}

			if _, err := fmt.Fprintln(&buf, line); err != nil {
				return err
			}

			if !r.scanner.Scan() {
				break
			}

			line = r.scanner.Text()
		}
		r.op.Output = buf.String()
		return nil
	}

	// Look for two successive lines of "----" before terminating.
	for r.scanner.Scan() {
		line = r.scanner.Text()
		if line != "----" {
			// We just picked up a regular line that's part of the command
			// output.
			if _, err := fmt.Fprintln(&buf, line); err != nil {
				return err
			}

			continue
		}

		// We picked up a separator. We could either be part of the
		// command output, or it was actually intended by the user as a
		// separator. Let's check to see if we can parse a second one.
		if err := r.parseSeparator(); err == nil {
			// We just saw the second separator, the output portion is done.
			// Read the following blank line.
			if r.scanner.Scan() && r.scanner.Text() != "" {
				return fmt.Errorf("%s: non-blank line after end of double ---- separator section", r.scanner.pos())
			}
			break
		}

		// The separator we saw was part of the command output.
		// Let's collect both lines (the first separator, and the
		// new one).
		if _, err := fmt.Fprintln(&buf, line); err != nil {
			return err
		}

		line2 := r.scanner.Text()
		if _, err := fmt.Fprintln(&buf, line2); err != nil {
			return err
		}
	}

	r.op.Output = buf.String()
	return nil
}
