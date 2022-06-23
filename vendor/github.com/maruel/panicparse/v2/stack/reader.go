// Copyright 2020 Marc-Antoine Ruel. All rights reserved.
// Use of this source code is governed under the Apache License, Version 2.0
// that can be found in the LICENSE file.

package stack

import (
	"bytes"
	"errors"
	"io"
)

var (
	errBufferFull = errors.New("buffer full")
)

type reader struct {
	buf  [16 * 1024]byte
	rd   io.Reader
	r, w int
	err  error
}

// fill reads a new chunk into the buffer.
func (r *reader) fill() {
	// Slide existing data to beginning.
	if r.r > 0 {
		copy(r.buf[:], r.buf[r.r:r.w])
		r.w -= r.r
		r.r = 0
	}
	if r.w >= len(r.buf) {
		panic("tried to fill full buffer")
	}
	// Read new data: try a limited number of times.
	for i := 100; i > 0; i-- {
		n, err := r.rd.Read(r.buf[r.w:])
		if n < 0 {
			panic("reader returned negative count from Read")
		}
		r.w += n
		if err != nil {
			r.err = err
			return
		}
		if n > 0 {
			return
		}
	}
	r.err = io.ErrNoProgress
}

func (r *reader) buffered() []byte {
	return r.buf[r.r:r.w]
}

func (r *reader) readSlice() ([]byte, error) {
	for s := 0; ; r.fill() {
		if i := bytes.IndexByte(r.buf[r.r+s:r.w], '\n'); i >= 0 {
			i += s
			line := r.buf[r.r : r.r+i+1]
			r.r += i + 1
			return line, nil
		}
		if r.err != nil {
			line := r.buf[r.r:r.w]
			r.r = r.w
			err := r.err
			r.err = nil
			return line, err
		}
		if r.w-r.r == len(r.buf) {
			r.r = r.w
			return r.buf[:], errBufferFull
		}
		s = r.w - r.r
	}
}

// readLine is our own implementation of ReadBytes().
//
// We try to use readSlice() as much as we can but we need to tolerate if an
// input line is longer than the buffer specified at Reader creation. Not using
// the more complicated slice of slices that Reader.ReadBytes() uses since it
// should not happen often here. Instead bootstrap the memory allocation by
// starting with 4x buffer size, which should get most cases with a single
// allocation.
func (r *reader) readLine() ([]byte, error) {
	var d []byte
	for {
		f, err := r.readSlice()
		if err != errBufferFull {
			if d == nil {
				return f, err
			}
			return append(d, f...), err
		}
		if d == nil {
			d = make([]byte, 0, len(f)*4)
		}
		d = append(d, f...)
	}
}
