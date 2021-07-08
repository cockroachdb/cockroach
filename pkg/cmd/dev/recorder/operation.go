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

import "strings"

// Operation represents the base unit of what can be recorded. It consists of a
// command and an expected output.
//
// The printed form of the command is defined by the following grammar:
//
//   # comment
//   <command> \
//   <command wraps over onto the next line>
//   ----
//   <output>
//
// By default <output> cannot contain blank lines. This alternative syntax
// allows the use of blank lines.
//
//   <command>
//   ----
//   ----
//   <output>
//
//   <more output>
//   ----
//   ----
type Operation struct {
	Command string // <command>
	Output  string // <output>
}

// String returns a printable form for the given Operation. See type-level
// comment to understand the grammar we're constructing against.
func (o *Operation) String() string {
	var sb strings.Builder
	sb.WriteString(o.Command)
	sb.WriteString("\n")

	sb.WriteString("----\n")

	multiline := strings.ContainsAny(strings.TrimRight(o.Output, "\n"), "\n")
	if multiline {
		sb.WriteString("----\n")
	}

	sb.WriteString(o.Output)
	if o.Output != "" && !strings.HasSuffix(o.Output, "\n") {
		sb.WriteString("\n")
	}

	if multiline {
		sb.WriteString("----\n")
		sb.WriteString("----\n")
	}

	sb.WriteString("\n")
	return sb.String()
}
