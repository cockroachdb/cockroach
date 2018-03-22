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
	"io"

	"github.com/cockroachdb/cockroach/pkg/util/rate"
)

// Reader wraps an io.Reader and provides an associated IO rate.
// The rate will be frozen when the underlying reader returns io.EOF.
type Reader struct {
	ctr      *rate.Counter
	delegate io.Reader
}

var _ io.Reader = &Reader{}
var _ rate.Rater = &Reader{}

// Composition constructor.
func newReader(ctr *rate.Counter, r io.Reader) *Reader {
	return &Reader{
		ctr:      ctr,
		delegate: r,
	}
}

// NewReader constructs a new iorate.Reader around an io.Reader,
// sampling the observed IO rate over a one-minute interval.
func NewReader(r io.Reader) *Reader {
	return newReader(&rate.Counter{}, r)
}

// Rate returns the current IO rate in bytes / second if the Read()
// has not yet returned an error (including io.EOF), or the last known
// rate when an error was returned.
func (r *Reader) Rate() rate.Rate {
	return r.ctr.Rate()
}

// Read implements the io.Reader interface.
func (r *Reader) Read(p []byte) (int, error) {
	count, err := r.delegate.Read(p)
	if err == nil {
		r.ctr.Add(count)
	} else {
		r.ctr.Freeze()
	}
	return count, err
}

// ReadCloser wraps an io.ReadCloser and provides an associated IO rate.
// The rate will be frozen when the underlying reader returns an EOF
// or if Close is called.
type ReadCloser struct {
	r *Reader
	c *closer
}

var _ io.ReadCloser = &ReadCloser{}
var _ rate.Rater = &ReadCloser{}

// NewReadCloser wraps an io.ReadCloser to provide an IO rate, with
// optional callbacks when Close() is called.
func NewReadCloser(rc io.ReadCloser, onClose ...func()) *ReadCloser {
	var base rate.Counter
	return &ReadCloser{
		r: newReader(&base, rc),
		c: newCloser(&base, rc, onClose),
	}
}

// Rate returns the current IO rate if the Read()
// has not yet returned an error (including io.EOF), or the last known
// rate when an error was returned or when Close() was called.
func (r *ReadCloser) Rate() rate.Rate {
	return r.r.Rate()
}

// Read implements the io.Reader interface.
func (r *ReadCloser) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

// Close implements the io.Closer interface.  It will freeze the rate
// associated with the ReadCloser.
func (r *ReadCloser) Close() error {
	return r.c.Close()
}

// ReadSeeker wraps an io.ReadSeeker and provides an associated IO rate.
// The rate will be frozen when the reader returns an EOF.
type ReadSeeker struct {
	r *Reader
	s io.Seeker
}

var _ io.ReadSeeker = &ReadSeeker{}
var _ rate.Rater = &ReadSeeker{}

// NewReadSeeker wraps an io.ReadSeeker to provide an IO rate.
func NewReadSeeker(rs io.ReadSeeker) *ReadSeeker {
	return &ReadSeeker{
		r: newReader(&rate.Counter{}, rs),
		s: rs,
	}
}

// Rate returns the current IO rate in bytes / second if the Read()
// has not yet returned an error (including io.EOF), or the last known
// rate when an error was returned.
func (r *ReadSeeker) Rate() rate.Rate {
	return r.r.Rate()
}

// Read implements the io.Reader interface.
func (r *ReadSeeker) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

// Seek implements the io.Seeker interface.
func (r *ReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.s.Seek(offset, whence)
}
