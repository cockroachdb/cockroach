// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import (
	"bufio"
	"io"
)

type writer interface {
	Write([]byte) (int, error)
	WriteByte(byte) error
	WriteString(string) (int, error)
	Flush() error
}

type monadicWriter struct {
	writer
	err error
}

func newMonadicWriter(w io.Writer) *monadicWriter {
	if w, ok := w.(writer); ok {
		return &monadicWriter{writer: w}
	}
	return &monadicWriter{writer: bufio.NewWriter(w)}
}

func (w *monadicWriter) Write(p []byte) (n int, err error) {
	if w.err != nil {
		return
	}

	n, err = w.writer.Write(p)
	w.err = err
	return
}

func (w *monadicWriter) WriteByte(b byte) (err error) {
	if w.err != nil {
		return
	}

	err = w.writer.WriteByte(b)
	w.err = err
	return
}

func (w *monadicWriter) WriteString(s string) (n int, err error) {
	if w.err != nil {
		return
	}

	n, err = w.writer.WriteString(s)
	w.err = err
	return
}

func (w *monadicWriter) Flush() (err error) {
	if w.err != nil {
		return
	}

	err = w.writer.Flush()
	w.err = err
	return
}
