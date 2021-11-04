// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
