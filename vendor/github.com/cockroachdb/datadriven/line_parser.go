// Copyright 2019 The Cockroach Authors.
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

package datadriven

import (
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
)

// ParseLine parses a datadriven directive line and returns the parsed command
// and CmdArgs.
//
// An input directive line is a command optionally followed by a list of
// arguments. Arguments may or may not have values and are specified with one of
// the forms:
//  - <argname>                            # No values.
//  - <argname>=<value>                    # Single value.
//  - <argname>=(<value1>, <value2>, ...)  # Multiple values.
//
// Note that in the last case, we allow the values to contain parens; the
// parsing will take nesting into account. For example:
//   cmd exprs=(a + (b + c), d + f)
// is valid and produces the expected values for the argument.
//
func ParseLine(line string) (cmd string, cmdArgs []CmdArg, err error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", nil, nil
	}
	origLine := line

	defer func() {
		if r := recover(); r != nil {
			if r == (parseError{}) {
				column := len(origLine) - len(line) + 1
				cmd = ""
				cmdArgs = nil
				err = errors.Errorf("cannot parse directive at column %d: %s", column, origLine)
				// Note: to debug an unexpected parsing error, this is a good place to
				// add a debug.PrintStack().
			} else {
				panic(r)
			}
		}
	}()

	// until removes the prefix up to one of the given characters from line and
	// returns the prefix.
	until := func(chars string) string {
		idx := strings.IndexAny(line, chars)
		if idx == -1 {
			idx = len(line)
		}
		res := line[:idx]
		line = line[idx:]
		return res
	}

	cmd = until(" ")
	if cmd == "" {
		panic(parseError{})
	}
	line = strings.TrimSpace(line)

	for line != "" {
		var arg CmdArg
		arg.Key = until(" =")
		if arg.Key == "" {
			panic(parseError{})
		}
		if line != "" && line[0] == '=' {
			// Skip the '='.
			line = line[1:]

			if line == "" || line[0] == ' ' {
				// Empty value.
				arg.Vals = []string{""}
			} else if line[0] != '(' {
				// Single value.
				val := until(" ")
				arg.Vals = []string{val}
			} else {
				// Skip the '('.
				pos := 1
				nestLevel := 1
				lastValStart := pos
				// Run through the characters for the values, being mindful of nested
				// parens. When we find a top-level comma, we "cut" a value and append
				// it to the array.
				for nestLevel > 0 {
					if pos == len(line) {
						// The string ended before we found the final ')'.
						panic(parseError{})
					}
					r, runeSize := utf8.DecodeRuneInString(line[pos:])
					pos += runeSize
					switch r {
					case ',':
						if nestLevel == 1 {
							// Found a top-level comma.
							arg.Vals = append(arg.Vals, line[lastValStart:pos-1])
							// Skip any spaces after the comma.
							for pos < len(line) && line[pos] == ' ' {
								pos++
							}
							lastValStart = pos
						}
					case '(':
						nestLevel++
					case ')':
						nestLevel--
					}
				}
				arg.Vals = append(arg.Vals, line[lastValStart:pos-1])
				line = strings.TrimSpace(line[pos:])
			}
		}
		cmdArgs = append(cmdArgs, arg)
		line = strings.TrimSpace(line)
	}
	return cmd, cmdArgs, nil
}

type parseError struct{}
