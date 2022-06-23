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
	"bufio"
	"fmt"
	"io"
)

// scanner is a convenience wrapper around a bufio.Scanner that keeps track of
// the last read line number. It's also configured with an associated name for
// the reader (typically a file name) to generate positional error messages.
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
