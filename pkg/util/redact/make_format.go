// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package redact

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

func reproducePrintf(w io.Writer, s fmt.State, verb rune, arg interface{}) {
	justV, revFmt := MakeFormat(s, verb)
	if justV {
		// Common case, avoids generating then parsing the format again.
		fmt.Fprint(w, arg)
	} else {
		fmt.Fprintf(w, revFmt, arg)
	}
}

// MakeFormat reproduces the format currently active in fmt.State and
// verb. This is provided because Go's standard fmt.State does not
// make the original format string available to us.
//
// If the return value justV is true, then the current state
// was found to be %v exactly; in that case the caller
// can avoid a full-blown Printf call and use just Print instead
// to take a shortcut.
func MakeFormat(s fmt.State, verb rune) (justV bool, format string) {
	plus, minus, hash, sp, z := s.Flag('+'), s.Flag('-'), s.Flag('#'), s.Flag(' '), s.Flag('0')
	w, wp := s.Width()
	p, pp := s.Precision()

	if !plus && !minus && !hash && !sp && !z && !wp && !pp {
		switch verb {
		case 'v':
			return true, "%v"
		case 's':
			return false, "%s"
		case 'd':
			return false, "%d"
		}
		// Other cases handled in the slow path below.
	}

	var f strings.Builder
	f.WriteByte('%')
	if plus {
		f.WriteByte('+')
	}
	if minus {
		f.WriteByte('-')
	}
	if hash {
		f.WriteByte('#')
	}
	if sp {
		f.WriteByte(' ')
	}
	if z {
		f.WriteByte('0')
	}
	if wp {
		f.WriteString(strconv.Itoa(w))
	}
	if pp {
		f.WriteByte('.')
		f.WriteString(strconv.Itoa(p))
	}
	f.WriteRune(verb)
	return false, f.String()
}
