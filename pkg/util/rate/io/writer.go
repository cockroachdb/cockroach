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

package io

import (
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/rate"
)

// Writer wraps an io.Writer and provides an associated IO rate.
// The rate will be frozen if the underlying writer returns a
// write error.
type Writer struct {
	ctr *rate.Counter
	w   io.Writer
}

var _ io.Writer = &Writer{}
var _ rate.Rater = &Writer{}

// Composition constructor.
func newWriter(ctr *rate.Counter, w io.Writer) *Writer {
	return &Writer{ctr: ctr, w: w}
}

// NewWriter constructs a new iorate.Writer around an io.Writer,
// sampling the observed IO rate over a one-minute interval.
func NewWriter(w io.Writer) *Writer {
	return newWriter(&rate.Counter{}, w)
}

// Rate returns the rate at which bytes are being written.  The value
// returned here will always decay, since there is no way to know
// when to freeze the value.
func (r *Writer) Rate() rate.Rate {
	return r.ctr.Rate()
}

// Write implements the io.Writer interface.
func (r *Writer) Write(p []byte) (int, error) {
	count, err := r.w.Write(p)
	if err == nil {
		r.ctr.Add(count)
	} else {
		r.ctr.Freeze()
	}
	return count, err
}

// String returns the rates over the configured timescales.
func (r *Writer) String() string {
	return fmt.Sprintf("%.2f bytes / sec", r.Rate())
}

// WriteCloser wraps an io.WriteCloser and provides an associated IO
// rate.  The rate will be frozen if the underlying Writer returns an
// error or Close() is called.
type WriteCloser struct {
	c *closer
	w *Writer
}

var _ io.WriteCloser = &WriteCloser{}
var _ rate.Rater = &WriteCloser{}

// NewWriteCloser constructs a new iorate.WriteCloser around an
// io.WriteCloser, with optional callbacks when Close() is called.
func NewWriteCloser(wc io.WriteCloser, onClose ...func()) *WriteCloser {
	var base rate.Counter
	return &WriteCloser{
		c: newCloser(&base, wc, onClose),
		w: newWriter(&base, wc),
	}
}

// Close implements the io.Closer interface.  Calling this method will
// freeze the Rate associated with the WriteCloser.
func (w *WriteCloser) Close() error {
	return w.c.Close()
}

// Rate returns the rate at which bytes are being written, or the last
// known value when the writer was closed.
func (w *WriteCloser) Rate() rate.Rate {
	return w.w.Rate()
}

// Write implements the io.Writer interface.
func (w *WriteCloser) Write(p []byte) (int, error) {
	return w.w.Write(p)
}
