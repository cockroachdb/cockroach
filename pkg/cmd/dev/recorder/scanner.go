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
	"bufio"
	"fmt"
	"io"
)

// scanner is a convenience wrapper around a bufio.Scanner that keeps track of
// the currently read line number (and an associated name for the reader -
// typically a file name).
type scanner struct {
	*bufio.Scanner
	line int
	name string
}

func newScanner(r io.Reader, name string) *scanner {
	bufioScanner := bufio.NewScanner(r)
	// We use a large max-token-size to account for lines in the output that far
	// exceed the default bufio scanner token size.
	bufioScanner.Buffer(make([]byte, 100), 10*bufio.MaxScanTokenSize)
	return &scanner{
		Scanner: bufioScanner,
		name:    name,
	}
}

func (s *scanner) Scan() bool {
	ok := s.Scanner.Scan()
	if ok {
		s.line++
	}
	return ok
}

// pos is a file:line prefix for the input file, suitable for inclusion in logs
// and error messages.
func (s *scanner) pos() string {
	return fmt.Sprintf("%s:%d", s.name, s.line)
}
