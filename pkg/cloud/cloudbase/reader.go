// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudbase

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
)

// ReadCloserCtx groups the Read and Close methods. It's similar to
// io.ReadCloser, except the operations take a ctx.
type ReadCloserCtx interface {
	ReaderCtx
	Close(context.Context) error
}

// ReaderCtx is like io.Reader, but the Read() method takes in a ctx.
type ReaderCtx interface {
	Read(ctx context.Context, p []byte) (n int, err error)
}

// ioReadCloserAdapter turns an io.ReadCloser into a ReadCloserCtx by ignoring
// the ctx passed to all the methods.
type ioReadCloserAdapter struct {
	r io.ReadCloser
}

var _ ReadCloserCtx = ioReadCloserAdapter{}

// ReadCloserAdapter turns an io.ReadCloser into a ReadCloserCtx by ignoring the
// ctx passed to all the methods.
func ReadCloserAdapter(r io.ReadCloser) ReadCloserCtx {
	return ioReadCloserAdapter{r: r}
}

// Read is part of the ReadCloserCtx interface.
func (r ioReadCloserAdapter) Read(_ context.Context, p []byte) (n int, err error) {
	return r.r.Read(p)
}

// Close is part of the ReadCloserCtx interface.
func (r ioReadCloserAdapter) Close(context.Context) error {
	return r.r.Close()
}

// ioReaderAdapter turns an io.Reader into a cloud.ReaderCtx by ignoring
// the ctx passed to Read.
type ioReaderAdapter struct {
	r io.Reader
}

var _ ReaderCtx = ioReaderAdapter{}

// ReaderAdapter turns an io.Reader into a ReaderCtx by ignoring the ctx passed
// to Read.
func ReaderAdapter(r io.Reader) ReaderCtx {
	return ioReaderAdapter{r: r}
}

// Read implements the ReaderCtx interface.
func (r ioReaderAdapter) Read(_ context.Context, p []byte) (n int, err error) {
	return r.r.Read(p)
}

// readerCtxAdapter turn a ReaderCtx into an io.Reader by capturing a context at
// construction time and using it for all the Read calls.
type readerCtxAdapter struct {
	ctx context.Context
	r   ReaderCtx
}

var _ io.Reader = readerCtxAdapter{}

// ReaderCtxAdapter turn a ReaderCtx into an io.Reader by capturing a context at
// construction time and using it for all the Read calls.
func ReaderCtxAdapter(ctx context.Context, r ReaderCtx) io.Reader {
	return readerCtxAdapter{
		ctx: ctx,
		r:   r,
	}
}

// Read implements io.Reader.
func (r readerCtxAdapter) Read(p []byte) (n int, err error) {
	return r.r.Read(r.ctx, p)
}

// ReadAll reads from r until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because ReadAll is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
//
// This code is adapted from the stdlib io.ReadAll, except that:
// - it operates on a ReaderCtx instead of a io.Reader
// - it takes in a ctx
// - it terminates successfully on errors that wrap io.EOF, not just on io.EOF
// itself.
func ReadAll(ctx context.Context, r ReaderCtx) ([]byte, error) {
	b := make([]byte, 0, 512)
	for {
		if len(b) == cap(b) {
			// Add more capacity (let append pick how much).
			b = append(b, 0)[:len(b)]
		}
		n, err := r.Read(ctx, b[len(b):cap(b)])
		b = b[:len(b)+n]
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return b, err
		}
	}
}

// NopCloser returns a ReadCloser with a no-op Close method wrapping
// the provided ReaderCtx.
func NopCloser(r ReaderCtx) ReadCloserCtx {
	return nopCloser{r}
}

type nopCloser struct {
	ReaderCtx
}

// Close is part of the ReadClosedCtx interface.
func (nopCloser) Close(ctx context.Context) error { return nil }
