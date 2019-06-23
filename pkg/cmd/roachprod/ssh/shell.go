// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ssh

import (
	"fmt"
	"regexp"
	"strings"
)

const shellMetachars = "|&;()<> \t\n$\\`"

// Escape1 TODO(peter): document
func Escape1(arg string) string {
	if strings.ContainsAny(arg, shellMetachars) {
		// Argument contains shell metacharacters. Double quote the
		// argument, and backslash-escape any characters that still have
		// meaning inside of double quotes.
		e := regexp.MustCompile("([$`\"\\\\])").ReplaceAllString(arg, `\$1`)
		return fmt.Sprintf(`"%s"`, e)
	}
	return arg
}

// Escape TODO(peter): document
func Escape(args []string) string {
	escaped := make([]string, len(args))
	for i := range args {
		escaped[i] = Escape1(args[i])
	}
	return strings.Join(escaped, " ")
}
