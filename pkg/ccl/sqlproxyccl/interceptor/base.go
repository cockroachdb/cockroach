// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package interceptor

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// pgHeaderSizeBytes represents the number of bytes of a pgwire message header
// (i.e. one byte for type, and an int for the body size, inclusive of the
// length itself).
const pgHeaderSizeBytes = 5

// defaultBufferSize is the default buffer size for the interceptor. 8K was
// chosen to match Postgres' send and receive buffer sizes.
//
// See: https://github.com/postgres/postgres/blob/249d64999615802752940e017ee5166e726bc7cd/src/backend/libpq/pqcomm.c#L134-L135.
const defaultBufferSize = 1 << 13 // 8K

// ErrProtocolError indicates that the packets are malformed, and are not as
// expected.
var ErrProtocolError = errors.New("protocol error")

// pgInterceptor provides a convenient way to read and forward Postgres
// messages, while minimizing IO reads and memory allocations.
//
// NOTE: Methods on the interceptor are not thread-safe.
type pgInterceptor struct {
	_ util.NoCopy

	src io.Reader

	// buf stores buffered bytes from src. This may contain one or more pgwire
	// messages, and messages may be partially buffered.
	buf []byte

	// readPos and writePos indicate the read and write pointers for bytes in
	// the buffer buf. When readPos == writePos, the buffer is empty. writePos
	// will be advanced whenever bytes are read into the buffer from src,
	// whereas readPos will be advanced whenever bytes are read from the buffer
	// (i.e through ReadMsg or ForwardMsg). When peeking the buffer, none of
	// these pointers are updated.
	//
	// Note that readPos will always be <= writePos. Bytes are read into the
	// buffer from src until writePos reaches len(buf). If the last message
	// was partially buffered, the interceptor will handle that case before
	// resetting readPos and writePos to 0.
	readPos, writePos int
}

// newPgInterceptor creates a new instance of the interceptor with an internal
// buffer of bufSize bytes. If bufSize is smaller than 5 bytes, the interceptor
// will default to an 8K buffer size.
func newPgInterceptor(src io.Reader, bufSize int) *pgInterceptor {
	// The internal buffer must be able to fit the header. If bufSize is smaller
	// than 5 bytes, just default to 8K, or else the interceptor is unusable.
	if bufSize < pgHeaderSizeBytes {
		bufSize = defaultBufferSize
	}
	return &pgInterceptor{
		src: src,
		buf: make([]byte, bufSize),
	}
}

// PeekMsg returns the header of the current pgwire message without advancing
// the interceptor. On return, err == nil if and only if the entire header can
// be read. The returned size corresponds to the entire message size, which
// includes the header type and body length. This will return ErrProtocolError
// if the packets are malformed.
//
// If err != nil, we are safe to reuse the interceptor. In the case of
// ErrProtocolError, the interceptor is still usable, though calls to ReadMsg
// and ForwardMsg will return an error. The bytes are still in the buffer, so
// the only way is to abandon the interceptor.
func (p *pgInterceptor) PeekMsg() (typ byte, size int, err error) {
	if err := p.ensureNextNBytes(pgHeaderSizeBytes); err != nil {
		// Possibly due to a timeout or context cancellation.
		return 0, 0, err
	}

	typ = p.buf[p.readPos]
	size = int(binary.BigEndian.Uint32(p.buf[p.readPos+1:]))

	// Size has to be at least itself based on pgwire's protocol. Theoretically,
	// math.MaxInt32 is valid since the body's length is stored within 4 bytes,
	// but we'll just enforce that for simplicity (because we're adding 1 below).
	if size < 4 || size >= math.MaxInt32 {
		return 0, 0, ErrProtocolError
	}

	// Add 1 to size to account for type. We don't need to add 4 (int length) to
	// it because size is already inclusive of that.
	return typ, size + 1, nil
}

// ReadMsg returns the current pgwire message in bytes. It also advances the
// interceptor to the next message. On return, the msg field is valid if and
// only if err == nil.
//
// The interceptor retains ownership of all the memory returned by ReadMsg; the
// caller is allowed to hold on to this memory *until* the next moment other
// methods on the interceptor are called. The data will only be valid until then
// as well. This may allocate if the message does not fit into the internal
// buffer, so use with care. If we are using this with the intention of sending
// it to another connection, we should use ForwardMsg, which does not allocate.
//
// WARNING: If err != nil, the caller should abandon the interceptor, as we may
// be in a corrupted state. This invokes PeekMsg under the hood to know the
// message length. One optimization that could be done is to invoke PeekMsg
// manually first before calling this to ensure that we do not return errors
// when peeking during ReadMsg.
func (p *pgInterceptor) ReadMsg() (msg []byte, err error) {
	// Peek header of the current message for message size.
	_, size, err := p.PeekMsg()
	if err != nil {
		return nil, err
	}

	// Can the entire message fit into the buffer?
	if size <= len(p.buf) {
		if err := p.ensureNextNBytes(size); err != nil {
			// Possibly due to a timeout or context cancellation.
			return nil, err
		}

		// Return a slice to the internal buffer to avoid an allocation here.
		retBuf := p.buf[p.readPos : p.readPos+size]
		p.readPos += size
		return retBuf, nil
	}

	// Message cannot fit, so we will have to allocate.
	msg = make([]byte, size)

	// Copy bytes which have already been read.
	n := copy(msg, p.buf[p.readPos:p.writePos])
	p.readPos += n

	// Read more bytes.
	if _, err := io.ReadFull(p.src, msg[n:]); err != nil {
		return nil, err
	}

	return msg, nil
}

// ForwardMsg sends the current pgwire message to dst, and advances the
// interceptor to the next message. On return, n == pgwire message size if
// and only if err == nil.
//
// WARNING: If err != nil, the caller should abandon the interceptor or dst, as
// we may be in a corrupted state. This invokes PeekMsg under the hood to know
// the message length. One optimization that could be done is to invoke PeekMsg
// manually first before calling this to ensure that we do not return errors
// when peeking during ForwardMsg.
func (p *pgInterceptor) ForwardMsg(dst io.Writer) (n int, err error) {
	// Retrieve header of the current message for body size.
	_, size, err := p.PeekMsg()
	if err != nil {
		return 0, err
	}

	// Handle overflows as current message may not fit in the current buffer.
	startPos := p.readPos
	endPos := startPos + size
	remainingBytes := 0
	if endPos > p.writePos {
		remainingBytes = endPos - p.writePos
		endPos = p.writePos
	}
	p.readPos = endPos

	// Forward the message to the destination.
	n, err = dst.Write(p.buf[startPos:endPos])
	if err != nil {
		return n, err
	}
	// n shouldn't be larger than the size of the buffer unless the
	// implementation of Write for dst is incorrect. This shouldn't be the case
	// if we're using a TCP connection here.
	if n < endPos-startPos {
		return n, io.ErrShortWrite
	}

	// Message was partially buffered, so copy the remaining.
	if remainingBytes > 0 {
		m, err := io.CopyN(dst, p.src, int64(remainingBytes))
		n += int(m)
		if err != nil {
			return n, err
		}
		// n shouldn't be larger than remainingBytes unless the internal Read
		// and Write calls for either of src or dst are incorrect. This
		// shouldn't be the case if we're using a TCP connection here.
		if int(m) < remainingBytes {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}

// readSize returns the number of bytes read by the interceptor.
func (p *pgInterceptor) readSize() int {
	return p.writePos - p.readPos
}

// writeSize returns the remaining number of bytes that could fit into the
// internal buffer before needing to be re-aligned.
func (p *pgInterceptor) writeSize() int {
	return len(p.buf) - p.writePos
}

// ensureNextNBytes blocks on IO reads until the buffer has at least n bytes.
func (p *pgInterceptor) ensureNextNBytes(n int) error {
	if n < 0 || n > len(p.buf) {
		return errors.AssertionFailedf(
			"invalid number of bytes %d for buffer size %d", n, len(p.buf))
	}

	// Buffer already has n bytes.
	if p.readSize() >= n {
		return nil
	}

	// Not enough empty slots to fit the unread bytes, so re-align bytes.
	minReadCount := n - p.readSize()
	if p.writeSize() < minReadCount {
		p.writePos = copy(p.buf, p.buf[p.readPos:p.writePos])
		p.readPos = 0
	}

	c, err := io.ReadAtLeast(p.src, p.buf[p.writePos:], minReadCount)
	p.writePos += c
	return err
}

var _ io.Writer = &errWriter{}

// errWriter is an io.Writer that fails whenever a Write call is made. This is
// used within ReadMsg for both BackendInterceptor and FrontendInterceptor.
// Since it's just a Read, Write calls should not be made.
type errWriter struct{}

// Write implements the io.Writer interface.
func (w *errWriter) Write(p []byte) (int, error) {
	return 0, errors.AssertionFailedf("unexpected Write call")
}
