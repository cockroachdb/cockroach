// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file contains interfaces extending io.Reader with a ctx argument, and
// adapters between io.Reader and our ReaderCtx.

package ioctx

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
)

// ReaderCtx is like io.Reader, but the Read() method takes in a ctx.
//
// NB: ReaderCtxAdapter can be used to turn a ReaderCtx into an io.Reader.
// Conversely, ReaderAdapter can be used to turn an io.Reader into a ReaderCtx.
type ReaderCtx interface {
	Read(ctx context.Context, p []byte) (n int, err error)
}

// ReaderAdapter turns an io.Reader into a ReaderCtx by ignoring the ctx passed
// to Read.
func ReaderAdapter(r io.Reader) ReaderCtx {
	return ioReaderAdapter{r: r}
}

// ioReaderAdapter turns an io.Reader into a cloud.ReaderCtx by ignoring
// the ctx passed to Read.
type ioReaderAdapter struct {
	r io.Reader
}

var _ ReaderCtx = ioReaderAdapter{}

// Read implements the ReaderCtx interface.
func (r ioReaderAdapter) Read(_ context.Context, p []byte) (n int, err error) {
	return r.r.Read(p)
}

// ReaderCtxAdapter turn a ReaderCtx into an io.Reader by capturing a context at
// construction time and using it for all the Read calls.
func ReaderCtxAdapter(ctx context.Context, r ReaderCtx) io.Reader {
	return readerCtxAdapter{
		ctx: ctx,
		r:   r,
	}
}

// readerCtxAdapter turn a ReaderCtx into an io.Reader by capturing a context at
// construction time and using it for all the Read calls.
type readerCtxAdapter struct {
	ctx context.Context
	r   ReaderCtx
}

var _ io.Reader = readerCtxAdapter{}

// Read implements io.Reader.
func (r readerCtxAdapter) Read(p []byte) (n int, err error) {
	return r.r.Read(r.ctx, p)
}

// ReadCloserCtx groups the Read and Close methods. It's similar to
// io.ReadCloser, except the operations take a ctx.
//
// NB: ReadCloserAdapter can be used to turh an io.ReadCloser into a ReadCloserCtx.
type ReadCloserCtx interface {
	ReaderCtx
	Close(context.Context) error
}

var _ ReadCloserCtx = ioReadCloserAdapter{}

// ReadCloserAdapter turns an io.ReadCloser into a ReadCloserCtx by ignoring the
// ctx passed to all the methods.
func ReadCloserAdapter(r io.ReadCloser) ReadCloserCtx {
	return ioReadCloserAdapter{r: r}
}

// ioReadCloserAdapter turns an io.ReadCloser into a ReadCloserCtx by ignoring
// the ctx passed to all the methods.
type ioReadCloserAdapter struct {
	r io.ReadCloser
}

// Read is part of the ReadCloserCtx interface.
func (r ioReadCloserAdapter) Read(_ context.Context, p []byte) (n int, err error) {
	return r.r.Read(p)
}

// Close is part of the ReadCloserCtx interface.
func (r ioReadCloserAdapter) Close(context.Context) error {
	return r.r.Close()
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

// ReaderAtCtx is like io.ReaderAt, but the ReadAt() method takes a context.
type ReaderAtCtx interface {
	ReadAt(ctx context.Context, p []byte, off int64) (n int, err error)
}

// SeekerCtx is like io.Seeker, but the Seek() method takes a context.
type SeekerCtx interface {
	Seek(ctx context.Context, offset int64, whence int) (n int64, err error)
}

// ReaderAtSeekerCloser combines io.Reader, io.ReaderAt, io.Seeker, and io.Closer.
// This is useful for formats like Parquet that require random access to files.
type ReaderAtSeekerCloser interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

// ReaderAtSeekerAdapter adapts a ReadCloserCtx that implements ReaderAtCtx
// and SeekerCtx into a ReaderAtSeekerCloser. The context is captured at construction
// time and used for all operations (Read, ReadAt, Seek, and Close).
//
// Returns an error if the underlying reader does not implement both ReaderAtCtx and
// SeekerCtx, or if a test seek operation fails (indicating the reader claims to support
// seeking but doesn't actually work).
//
// This is useful for integrating context-aware cloud storage readers with libraries
// that require the standard io.ReaderAt and io.Seeker interfaces (e.g., Parquet).
//
// IMPORTANT: The returned adapter's lifetime is tied to the provided context. If the
// context is canceled, all subsequent Read/ReadAt/Seek operations will fail. The caller
// must ensure the context remains valid for the entire duration the adapter is in use.
func ReaderAtSeekerAdapter(
	ctx context.Context, readCloser ReadCloserCtx,
) (ReaderAtSeekerCloser, error) {
	readerAt, okReaderAt := readCloser.(ReaderAtCtx)
	if !okReaderAt {
		return nil, errors.Newf("reader does not implement ioctx.ReaderAtCtx (type %T)", readCloser)
	}
	seeker, okSeeker := readCloser.(SeekerCtx)
	if !okSeeker {
		return nil, errors.Newf("reader does not implement ioctx.SeekerCtx (type %T)", readCloser)
	}

	// Test if seeking actually works by attempting a no-op seek
	// This catches cases where the reader implements the interface but fails at runtime
	if _, err := seeker.Seek(ctx, 0, io.SeekCurrent); err != nil {
		return nil, errors.Wrapf(err, "reader type %T claims to support seeking but test seek failed", readCloser)
	}

	return &readerAtSeekerAdapter{
		ctx:        ctx,
		readCloser: readCloser,
		readerAt:   readerAt,
		seeker:     seeker,
	}, nil
}

type readerAtSeekerAdapter struct {
	ctx        context.Context
	readCloser ReadCloserCtx
	readerAt   ReaderAtCtx
	seeker     SeekerCtx
}

var _ ReaderAtSeekerCloser = &readerAtSeekerAdapter{}

// Read implements io.Reader using the captured context.
func (r *readerAtSeekerAdapter) Read(p []byte) (int, error) {
	return r.readCloser.Read(r.ctx, p)
}

// ReadAt implements io.ReaderAt by delegating to the underlying ReaderAtCtx
// using the captured context.
func (r *readerAtSeekerAdapter) ReadAt(p []byte, off int64) (int, error) {
	return r.readerAt.ReadAt(r.ctx, p, off)
}

// Seek implements io.Seeker by delegating to the underlying SeekerCtx
// using the captured context.
func (r *readerAtSeekerAdapter) Seek(offset int64, whence int) (int64, error) {
	return r.seeker.Seek(r.ctx, offset, whence)
}

// Close implements io.Closer using the captured context.
func (r *readerAtSeekerAdapter) Close() error {
	return r.readCloser.Close(r.ctx)
}
