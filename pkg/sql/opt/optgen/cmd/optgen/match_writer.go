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

package main

import (
	"fmt"
	"io"
	"strings"
)

// matchWriter keeps track of the indentation level so that callers can nest
// and unnest code in whatever pattern they choose.
type matchWriter struct {
	writer  io.Writer
	nesting int
}

func (w *matchWriter) nest(format string, args ...interface{}) {
	w.writeIndent(format, args...)
	w.nesting++
}

func (w *matchWriter) write(format string, args ...interface{}) {
	fmt.Fprintf(w.writer, format, args...)
}

func (w *matchWriter) writeIndent(format string, args ...interface{}) {
	fmt.Fprintf(w.writer, strings.Repeat("  ", w.nesting))
	fmt.Fprintf(w.writer, format, args...)
}

func (w *matchWriter) unnest(n int, suffix string) {
	for ; n > 0; n-- {
		w.nesting--
		fmt.Fprintf(w.writer, strings.Repeat("  ", w.nesting))
		fmt.Fprintf(w.writer, suffix)
	}
}
