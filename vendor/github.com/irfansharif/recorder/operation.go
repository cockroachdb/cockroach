// Copyright 2021 Irfan Sharif.
// Copyright 2018 The Cockroach Authors.
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
//
// Portions of this code was derived from cockroachdb/datadriven.

package recorder

import (
	"strings"
)

// operation represents the base unit of what can be recorded. It consists of a
// command and the corresponding output.
type operation struct {
	command string // <command>
	output  string // <output>
}

// String returns a printable form for the given operation, respecting the
// pre-defined grammar (see the comment on Recorder for the grammar we're
// constructing against).
func (o *operation) String() string {
	var sb strings.Builder
	sb.WriteString(o.command)
	sb.WriteString("\n")

	sb.WriteString("----")
	sb.WriteString("\n")

	var emptyLine bool
	if o.output != "" {
		lines := strings.Split(strings.TrimRight(o.output, "\n"), "\n")
		for _, line := range lines {
			if line == "" {
				emptyLine = true
				break
			}
		}
	}
	if emptyLine {
		sb.WriteString("----")
		sb.WriteString("\n")
	}

	sb.WriteString(o.output)
	if o.output != "" && !strings.HasSuffix(o.output, "\n") {
		// If the output is not \n terminated, add it.
		sb.WriteString("\n")
	}

	if emptyLine {
		sb.WriteString("----")
		sb.WriteString("\n")
		sb.WriteString("----")
		sb.WriteString("\n")
	}

	sb.WriteString("\n")
	return sb.String()
}
