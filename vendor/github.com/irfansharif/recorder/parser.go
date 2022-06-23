// Copyright 2021 Irfan Sharif.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package recorder

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

// parseOperation parses out the next operation from the internal scanner. See
// top-level comment on Recorder to understand the grammar we're parsing
// against.
func (r *Recorder) parseOperation() (parsed bool, err error) {
	for r.scanner.Scan() {
		r.op = operation{}
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
		r.op.command = command

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
// top-level comment on Recorder to understand the grammar we're parsing
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
		return "", errors.New(fmt.Sprintf("%s: cannot parse command at col %d: %s", r.scanner.pos(), column, origLine))
	}
	return cmd, nil
}

// parseSeparator parses a separator ('----'), erroring out if it's not parsed
// correctly. See top-level comment on Recorder to understand the grammar we're
// parsing against.
func (r *Recorder) parseSeparator() error {
	if !r.scanner.Scan() {
		return errors.New(fmt.Sprintf("%s: expected to find separator after command", r.scanner.pos()))
	}
	line := r.scanner.Text()
	if line != "----" {
		return errors.New(fmt.Sprintf("%s: expected to find separator after command, found %q instead", r.scanner.pos(), line))
	}
	return nil
}

// parseOutput parses an <output>. See top-level comment on Recorder to
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
		r.op.output = buf.String()
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
				return errors.New(fmt.Sprintf("%s: non-blank line after end of double ---- separator section", r.scanner.pos()))
			}
			r.op.output = buf.String()
			return nil
		}

		// The separator we saw was part of the command output.
		// Let's collect both lines (the first separator, and the
		// new one), and continue.
		if _, err := fmt.Fprintln(&buf, line); err != nil {
			return err
		}

		line2 := r.scanner.Text()
		if _, err := fmt.Fprintln(&buf, line2); err != nil {
			return err
		}
	}

	// We reached the end of the file before finding the closing separator.
	return errors.New(fmt.Sprintf("%s: missing closing double ---- separators", r.scanner.pos()))
}

// TODO(irfansharif): We could introduce a `# keep` directive to pin recordings
// on re-write. It raises a few questions around how new recordings get merged
// with existing ones, but it could be useful. It would allow test authors to
// trim down auto-generated mocks by hand for readability, and ensure that
// re-writes don't simply undo the work.
