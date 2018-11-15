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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package ui

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

// Writer TODO(peter): document
type Writer struct {
	buf       bytes.Buffer
	lineCount int
}

// Flush TODO(peter): document
func (w *Writer) Flush(out io.Writer) error {
	if len(w.buf.Bytes()) == 0 {
		return nil
	}
	w.clearLines(out)

	for _, b := range w.buf.Bytes() {
		if b == '\n' {
			w.lineCount++
		}
	}
	_, err := out.Write(w.buf.Bytes())
	w.buf.Reset()
	return err
}

func (w *Writer) Write(b []byte) (n int, err error) {
	return w.buf.Write(b)
}

func (w *Writer) clearLines(out io.Writer) {
	fmt.Fprint(out, strings.Repeat("\033[1A\033[2K\r", w.lineCount))
	w.lineCount = 0
}
