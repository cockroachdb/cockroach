// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ioctx

import (
	"context"
	"io"

	"github.com/cockroachdb/errors"
)

// OpenerAtFunc is a factory function that opens a reader at a specific offset.
// endHint can be used to optimize the read if the end position is known.
type OpenerAtFunc func(ctx context.Context, offset int64, endHint int64) (ReadCloserCtx, error)

// randomAccessReader wraps any cloud storage reader and adds ReadAt/Seek support
// using a factory function that can open readers at specific offsets.
//
// This provides random access capabilities without requiring the cloud storage
// implementation itself to support seeking. Each ReadAt call opens a new reader
// at the requested offset.
//
// Thread Safety:
//   - ReadAt: Safe for concurrent calls. Each call opens its own reader.
//   - Read/Seek: NOT safe for concurrent calls. Should only be used from a single goroutine.
type randomAccessReader struct {
	ctx    context.Context
	size   int64
	openAt OpenerAtFunc

	// Current sequential reader (for Read operations)
	current ReadCloserCtx
	pos     int64
}

var _ io.ReaderAt = &randomAccessReader{}
var _ io.ReadSeekCloser = &randomAccessReader{}

// NewRandomAccessReader creates a reader that supports ReadAt and Seek
// by using the provided opener function to create readers at specific offsets.
//
// Parameters:
//   - ctx: Context for operations
//   - size: Total size of the file
//   - openAt: Factory function that opens a reader at a given offset
func NewRandomAccessReader(
	ctx context.Context, size int64, openAt OpenerAtFunc,
) *randomAccessReader {
	return &randomAccessReader{
		ctx:    ctx,
		size:   size,
		openAt: openAt,
	}
}

// Read implements io.Reader using sequential access
func (r *randomAccessReader) Read(p []byte) (int, error) {
	if r.current == nil {
		// Open reader at current position
		opened, err := r.openAt(r.ctx, r.pos, r.size)
		if err != nil {
			return 0, err
		}
		r.current = opened
	}

	n, err := r.current.Read(r.ctx, p)
	r.pos += int64(n)
	return n, err
}

// ReadAt implements io.ReaderAt - safe for concurrent calls.
// Each call opens a new reader at the requested offset, so multiple
// goroutines can call ReadAt simultaneously.
//
// Note: This opens a new connection (HTTP request) for every ReadAt call.
// For cloud storage, this means each random access read results in a new
// range request to the storage service.
func (r *randomAccessReader) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}

	// Open a new reader at the requested offset
	reader, err := r.openAt(r.ctx, off, off+int64(len(p)))
	if err != nil {
		return 0, err
	}
	defer reader.Close(r.ctx)

	// Read exactly len(p) bytes or until EOF
	return io.ReadFull(ReaderCtxAdapter(r.ctx, reader), p)
}

// Seek implements io.Seeker
func (r *randomAccessReader) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = r.pos + offset
	case io.SeekEnd:
		newPos = r.size + offset
	default:
		return 0, errors.Newf("invalid whence: %d", whence)
	}

	if newPos < 0 {
		return 0, errors.New("negative position")
	}

	// Close current reader if position changed
	if r.current != nil && newPos != r.pos {
		r.current.Close(r.ctx)
		r.current = nil
	}

	r.pos = newPos
	return newPos, nil
}

// Close closes any open reader
func (r *randomAccessReader) Close() error {
	if r.current != nil {
		return r.current.Close(r.ctx)
	}
	return nil
}
