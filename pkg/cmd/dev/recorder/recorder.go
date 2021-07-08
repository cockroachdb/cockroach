// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package recorder

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Recorder can be used to record a set of operations (defined only by a
// "command" and an "expected output"). These recordings can then be played
// back, which provides a handy way to mock out the components being recorded.
type Recorder struct {
	// writer is set if we're in recording mode, and is where operations are
	// recorded.
	writer io.Writer

	// scanner and op are set if we're in replay mode. It's where we're
	// replaying the recording from. op is the scratch space used to
	// parse out the current operation being read.
	scanner *scanner
	op      Operation
}

// New constructs a Recorder, using the specified configuration option (one of
// WithReplayFrom or WithRecordingTo).
func New(opt func(r *Recorder)) *Recorder {
	r := &Recorder{}
	opt(r)
	return r
}

// WithReplayFrom is used to configure a Recorder to play back from the given
// reader. The provided name is used only for diagnostic purposes, it's
// typically the name of the file being read.
func WithReplayFrom(r io.Reader, name string) func(*Recorder) {
	return func(re *Recorder) {
		re.scanner = newScanner(r, name)
	}
}

// WithRecordingTo is used to configure a Recorder to record into the given
// writer.
func WithRecordingTo(w io.Writer) func(*Recorder) {
	return func(r *Recorder) {
		r.writer = w
	}
}

// Recording returns whether or not the recorder is configured to record (as
// opposed to replay from a recording).
func (r *Recorder) Recording() bool {
	return r.writer != nil
}

// Record is used to record the given operation.
func (r *Recorder) Record(o Operation) error {
	if !r.Recording() {
		return errors.New("misconfigured recorder; not set to record")
	}

	_, err := r.writer.Write([]byte(o.String()))
	return err
}

// Next is used to step through the next operation found in the recording, if
// any.
func (r *Recorder) Next(f func(Operation) error) (found bool, err error) {
	if r.Recording() {
		return false, errors.New("misconfigured recorder; set to record, not replay")
	}

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
func (r *Recorder) parseOperation() (parsed bool, err error) {
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
func (r *Recorder) parseCommand(line string) (cmd string, err error) {
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
func (r *Recorder) parseSeparator() error {
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
func (r *Recorder) parseOutput() error {
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
