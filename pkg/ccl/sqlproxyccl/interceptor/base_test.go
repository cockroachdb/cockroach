// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package interceptor

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"
	"testing/iotest"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

func TestNewPgInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reader, _ := io.Pipe()

	for _, tc := range []struct {
		bufSize           int
		normalizedBufSize int
	}{
		{-1, 8192},
		{pgHeaderSizeBytes - 1, 8192},
		{pgHeaderSizeBytes, pgHeaderSizeBytes},
		{1024, 1024},
	} {
		pgi := newPgInterceptor(reader, tc.bufSize)
		require.NotNil(t, pgi)
		require.Len(t, pgi.buf, tc.normalizedBufSize)
		require.Equal(t, reader, pgi.src)
	}
}

func TestPGInterceptor_PeekMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("read_error", func(t *testing.T) {
		r := iotest.ErrReader(errors.New("read error"))

		pgi := newPgInterceptor(r, 10 /* bufSize */)

		typ, size, err := pgi.PeekMsg()
		require.EqualError(t, err, "read error")
		require.Equal(t, byte(0), typ)
		require.Equal(t, 0, size)
	})

	t.Run("protocol_error/length=0", func(t *testing.T) {
		var data [10]byte

		buf := new(bytes.Buffer)
		_, err := buf.Write(data[:])
		require.NoError(t, err)

		pgi := newPgInterceptor(buf, 10 /* bufSize */)

		typ, size, err := pgi.PeekMsg()
		require.EqualError(t, err, ErrProtocolError.Error())
		require.Equal(t, byte(0), typ)
		require.Equal(t, 0, size)
	})

	t.Run("protocol_error/length=3", func(t *testing.T) {
		var data [5]byte
		binary.BigEndian.PutUint32(data[1:5], uint32(3))

		buf := new(bytes.Buffer)
		_, err := buf.Write(data[:])
		require.NoError(t, err)

		pgi := newPgInterceptor(buf, 10 /* bufSize */)

		typ, size, err := pgi.PeekMsg()
		require.EqualError(t, err, ErrProtocolError.Error())
		require.Equal(t, byte(0), typ)
		require.Equal(t, 0, size)
	})

	t.Run("protocol_error/length=math.MaxInt32", func(t *testing.T) {
		var data [5]byte
		binary.BigEndian.PutUint32(data[1:5], uint32(math.MaxInt32))

		buf := new(bytes.Buffer)
		_, err := buf.Write(data[:])
		require.NoError(t, err)

		pgi := newPgInterceptor(buf, 10 /* bufSize */)

		typ, size, err := pgi.PeekMsg()
		require.EqualError(t, err, ErrProtocolError.Error())
		require.Equal(t, byte(0), typ)
		require.Equal(t, 0, size)
	})

	t.Run("successful/length=4", func(t *testing.T) {
		// Only write 5 bytes (without body)
		var data [5]byte
		data[0] = 'A'
		binary.BigEndian.PutUint32(data[1:5], uint32(4))

		buf := new(bytes.Buffer)
		_, err := buf.Write(data[:])
		require.NoError(t, err)

		pgi := newPgInterceptor(buf, 5 /* bufSize */)

		typ, size, err := pgi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, byte('A'), typ)
		require.Equal(t, 5, size)
		require.Equal(t, 0, buf.Len())
	})

	t.Run("successful", func(t *testing.T) {
		buf := buildSrc(t, 1)

		pgi := newPgInterceptor(buf, 10 /* bufSize */)

		typ, size, err := pgi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, byte(pgwirebase.ClientMsgSimpleQuery), typ)
		require.Equal(t, len(testSelect1Bytes), size)
		require.Equal(t, 4, buf.Len())

		// Invoking Peek should not advance the interceptor.
		typ, size, err = pgi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, byte(pgwirebase.ClientMsgSimpleQuery), typ)
		require.Equal(t, len(testSelect1Bytes), size)
		require.Equal(t, 4, buf.Len())
	})
}

func TestPGInterceptor_ReadMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("read_error/msg_fits", func(t *testing.T) {
		buf := buildSrc(t, 1)

		// Use a LimitReader to allow PeekMsg to read 5 bytes, then update src
		// back to the original version.
		src := &errReadWriter{r: buf, count: 2}
		pgi := newPgInterceptor(io.LimitReader(src, 5), 32 /* bufSize */)

		// Call PeekMsg here to populate internal buffer with header.
		typ, size, err := pgi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, byte(pgwirebase.ClientMsgSimpleQuery), typ)
		require.Equal(t, len(testSelect1Bytes), size)
		require.Equal(t, 9, buf.Len())

		// Update src back to the non LimitReader version.
		pgi.src = src

		// Now call ReadMsg.
		msg, err := pgi.ReadMsg()
		require.EqualError(t, err, io.ErrClosedPipe.Error())
		require.Nil(t, msg)
		require.Equal(t, 9, buf.Len())
	})

	// When we overflow, ReadMsg will allocate.
	t.Run("read_error/msg_overflows", func(t *testing.T) {
		buf := buildSrc(t, 1)

		// testSelect1Bytes has 14 bytes, but only 6 bytes within internal
		// buffer, so overflow.
		src := &errReadWriter{r: buf, count: 2}
		pgi := newPgInterceptor(src, 6 /* bufSize */)

		msg, err := pgi.ReadMsg()
		require.EqualError(t, err, io.ErrClosedPipe.Error())
		require.Nil(t, msg)
		require.Equal(t, 8, buf.Len())
	})

	t.Run("successful/msg_fits", func(t *testing.T) {
		const count = 101 // Inclusive of warm-up run in AllocsPerRun.

		buf := buildSrc(t, count)

		// Set buffer's size to be a multiple of the message so that we'll
		// always hit the case where the message fits.
		pgi := newPgInterceptor(buf, len(testSelect1Bytes)*3)

		c := 0
		n := testing.AllocsPerRun(count-1, func() {
			// We'll ignore checking msg[1:5] here since other tests cover that.
			msg, err := pgi.ReadMsg()
			require.NoError(t, err)
			require.Equal(t, byte(pgwirebase.ClientMsgSimpleQuery), msg[0])

			expectedStr := "SELECT 1\x00"
			if c%2 == 1 {
				expectedStr = "SELECT 2\x00"
			}

			// Using require.Equal here will result in 2 allocs.
			body := msg[5:]
			str := *((*string)(unsafe.Pointer(&body)))
			if str != expectedStr {
				t.Fatalf(`expected %q, got: %q`, expectedStr, str)
			}
			c++
		})
		require.Equal(t, float64(0), n, "should not allocate")
		require.Equal(t, 0, buf.Len())
	})

	// When we overflow, ReadMsg will allocate.
	t.Run("successful/msg_overflows", func(t *testing.T) {
		const count = 101 // Inclusive of warm-up run in AllocsPerRun.

		buf := buildSrc(t, count)

		// Set the buffer to be large enough to fit more bytes than the header,
		// but not the entire message.
		pgi := newPgInterceptor(buf, 7 /* bufSize */)

		c := 0
		n := testing.AllocsPerRun(count-1, func() {
			// We'll ignore checking msg[1:5] here since other tests cover that.
			msg, err := pgi.ReadMsg()
			require.NoError(t, err)
			require.Equal(t, byte(pgwirebase.ClientMsgSimpleQuery), msg[0])

			expectedStr := "SELECT 1\x00"
			if c%2 == 1 {
				expectedStr = "SELECT 2\x00"
			}

			// Using require.Equal here will result in 2 allocs.
			body := msg[5:]
			str := *((*string)(unsafe.Pointer(&body)))
			if str != expectedStr {
				t.Fatalf(`expected %q, got: %q`, expectedStr, str)
			}
			c++
		})
		// Ensure that we only have 1 allocation. We could technically improve
		// this by ensuring that one pool of memory is used to reduce the number
		// of allocations, but ReadMsg is only called during a transfer session,
		// so there's very little benefit to optimizing for that.
		require.Equal(t, float64(1), n)
		require.Equal(t, 0, buf.Len())
	})
}

func TestPGInterceptor_ForwardMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("write_error/fully_buffered", func(t *testing.T) {
		src := buildSrc(t, 1)
		dst := new(bytes.Buffer)
		dstWriter := &errReadWriter{w: dst, count: 1}

		pgi := newPgInterceptor(src, 32 /* bufSize */)

		n, err := pgi.ForwardMsg(dstWriter)
		require.EqualError(t, err, io.ErrClosedPipe.Error())
		require.Equal(t, 0, n)

		// Managed to read everything, but could not write to dst.
		require.Equal(t, 0, src.Len())
		require.Equal(t, 0, dst.Len())
		require.Equal(t, 0, pgi.readSize())
	})

	t.Run("write_error/partially_buffered", func(t *testing.T) {
		src := buildSrc(t, 1)
		dst := new(bytes.Buffer)
		dstWriter := &errReadWriter{w: dst, count: 2}

		// testSelect1Bytes has 14 bytes, but only 6 bytes within internal
		// buffer, so partially buffered.
		pgi := newPgInterceptor(src, 6 /* bufSize */)

		n, err := pgi.ForwardMsg(dstWriter)
		require.EqualError(t, err, io.ErrClosedPipe.Error())
		require.Equal(t, 6, n)

		require.Equal(t, 6, dst.Len())
	})

	t.Run("successful/fully_buffered", func(t *testing.T) {
		const count = 101 // Inclusive of warm-up run in AllocsPerRun.

		src := buildSrc(t, count)
		dst := new(bytes.Buffer)

		// Set buffer's size to be a multiple of the message so that we'll
		// always hit the case where the message fits.
		pgi := newPgInterceptor(src, len(testSelect1Bytes)*3)

		// Forward all the messages, and ensure 0 allocations.
		n := testing.AllocsPerRun(count-1, func() {
			n, err := pgi.ForwardMsg(dst)
			require.NoError(t, err)
			require.Equal(t, 14, n)
		})
		require.Equal(t, float64(0), n, "should not allocate")
		require.Equal(t, 0, src.Len())

		// Validate messages.
		validateDst(t, dst, count)
		require.Equal(t, 0, dst.Len())
	})

	t.Run("successful/partially_buffered", func(t *testing.T) {
		const count = 151 // Inclusive of warm-up run in AllocsPerRun.

		src := buildSrc(t, count)
		dst := new(bytes.Buffer)

		// Set the buffer to be large enough to fit more bytes than the header,
		// but not the entire message.
		pgi := newPgInterceptor(src, 7 /* bufSize */)

		n := testing.AllocsPerRun(count-1, func() {
			n, err := pgi.ForwardMsg(dst)
			require.NoError(t, err)
			require.Equal(t, 14, n)
		})
		// NOTE: This allocation is benign, and is due to the fact that io.CopyN
		// allocates an internal buffer in copyBuffer. This wouldn't happen if
		// a TCP connection is used as the destination since there's a fast-path
		// that prevents that.
		//
		// See: https://cs.opensource.google/go/go/+/refs/tags/go1.17.6:src/io/io.go;l=402-410;drc=refs%2Ftags%2Fgo1.17.6
		require.Equal(t, float64(1), n)
		require.Equal(t, 0, src.Len())

		// Validate messages.
		validateDst(t, dst, count)
		require.Equal(t, 0, dst.Len())
	})
}

func TestPGInterceptor_readSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buf := bytes.NewBufferString("foobarbazz")
	pgi := newPgInterceptor(iotest.OneByteReader(buf), 10 /* bufSize */)

	// No reads to internal buffer.
	require.Equal(t, 0, pgi.readSize())

	// Attempt reads to buffer.
	require.NoError(t, pgi.ensureNextNBytes(3))
	require.Equal(t, 3, pgi.readSize())

	// Read until buffer is full.
	require.NoError(t, pgi.ensureNextNBytes(10))
	require.Equal(t, 10, pgi.readSize())
}

func TestPGInterceptor_writeSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	buf := bytes.NewBufferString("foobarbazz")
	pgi := newPgInterceptor(iotest.OneByteReader(buf), 10 /* bufSize */)

	// No writes to internal buffer.
	require.Equal(t, 10, pgi.writeSize())

	// Attempt writes to buffer.
	require.NoError(t, pgi.ensureNextNBytes(3))
	require.Equal(t, 7, pgi.writeSize())

	// Attempt more writes to buffer until full.
	require.NoError(t, pgi.ensureNextNBytes(10))
	require.Equal(t, 0, pgi.writeSize())
}

func TestPGInterceptor_ensureNextNBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("invalid n", func(t *testing.T) {
		pgi := newPgInterceptor(nil /* src */, 8 /* bufSize */)

		require.EqualError(t, pgi.ensureNextNBytes(-1),
			"invalid number of bytes -1 for buffer size 8")
		require.EqualError(t, pgi.ensureNextNBytes(9),
			"invalid number of bytes 9 for buffer size 8")
	})

	t.Run("buffer already has n bytes", func(t *testing.T) {
		buf := bytes.NewBufferString("foobarbaz")

		pgi := newPgInterceptor(iotest.OneByteReader(buf), 8 /* bufSize */)

		// Read "foo" into buffer".
		require.NoError(t, pgi.ensureNextNBytes(3))

		// These should not read anything since we expect the buffer to
		// have three bytes.
		require.NoError(t, pgi.ensureNextNBytes(3))
		require.Equal(t, 6, buf.Len())
		require.NoError(t, pgi.ensureNextNBytes(0))
		require.Equal(t, 6, buf.Len())
		require.NoError(t, pgi.ensureNextNBytes(1))
		require.Equal(t, 6, buf.Len())

		// Verify that buf actually has "foo".
		require.Equal(t, "foo", string(pgi.buf[pgi.readPos:pgi.writePos]))
	})

	t.Run("bytes are realigned", func(t *testing.T) {
		buf := bytes.NewBufferString("foobarbazcar")

		pgi := newPgInterceptor(iotest.OneByteReader(buf), 9 /* bufSize */)

		// Read "foobarb" into buffer.
		require.NoError(t, pgi.ensureNextNBytes(7))

		// Assume "foobar" is read.
		pgi.readPos += 6

		// Now ensure that we have 6 bytes.
		require.NoError(t, pgi.ensureNextNBytes(6))
		require.Equal(t, 0, buf.Len())

		// Verify that buf has "bazcar".
		require.Equal(t, "bazcar", string(pgi.buf[pgi.readPos:pgi.writePos]))
	})

	t.Run("bytes are read greedily", func(t *testing.T) {
		// This tests that we read as much as we can into the internal buffer
		// if there was a Read call.
		buf := bytes.NewBufferString("foobarbaz")

		pgi := newPgInterceptor(buf, 10 /* bufSize */)

		// Request for only 1 byte.
		require.NoError(t, pgi.ensureNextNBytes(1))

		// Verify that buf has "foobarbaz".
		require.Equal(t, "foobarbaz", string(pgi.buf[pgi.readPos:pgi.writePos]))

		// Should be a no-op.
		_, err := buf.WriteString("car")
		require.NoError(t, err)
		require.NoError(t, pgi.ensureNextNBytes(9))
		require.Equal(t, 3, buf.Len())
		require.Equal(t, "foobarbaz", string(pgi.buf[pgi.readPos:pgi.writePos]))
	})
}

var _ io.Reader = &errReadWriter{}
var _ io.Writer = &errReadWriter{}

// errReadWriter returns io.ErrClosedPipe after count reads or writes in total.
type errReadWriter struct {
	r     io.Reader
	w     io.Writer
	count int
}

// Read implements the io.Reader interface.
func (rw *errReadWriter) Read(p []byte) (int, error) {
	rw.count--
	if rw.count <= 0 {
		return 0, io.ErrClosedPipe
	}
	return rw.r.Read(p)
}

// Write implements the io.Writer interface.
func (rw *errReadWriter) Write(p []byte) (int, error) {
	rw.count--
	if rw.count <= 0 {
		return 0, io.ErrClosedPipe
	}
	return rw.w.Write(p)
}

// testSelect1Bytes represents the bytes for a SELECT 1 query. This will always
// be 14 bytes (5 (header) + 8 (query) + 1 (null terminator)).
var testSelect1Bytes = (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)

// buildSrc generates a buffer with count test queries which alternates between
// SELECT 1 and SELECT 2.
func buildSrc(t *testing.T, count int) *bytes.Buffer {
	t.Helper()

	// Reset bytes back to SELECT 1.
	defer func() {
		testSelect1Bytes[12] = '1'
	}()

	// Generate buffer.
	src := new(bytes.Buffer)
	for i := 0; i < count; i++ {
		// Alternate between SELECT 1 and 2 to ensure correctness.
		if i%2 == 0 {
			testSelect1Bytes[12] = '1'
		} else {
			testSelect1Bytes[12] = '2'
		}
		_, err := src.Write(testSelect1Bytes)
		require.NoError(t, err)
	}
	return src
}

// validateDst ensures that we have the right sequence of test queries in dst.
// There should be count queries that alternate between SELECT 1 and SELECT 2.
// Use buildSrc to generate the sender's buffer.
func validateDst(t *testing.T, dst io.Reader, count int) {
	t.Helper()
	backend := pgproto3.NewBackend(pgproto3.NewChunkReader(dst), nil /* w */)
	for i := 0; i < count; i++ {
		msg, err := backend.Receive()
		require.NoError(t, err)
		q := msg.(*pgproto3.Query)

		expectedStr := "SELECT 1"
		if i%2 == 1 {
			expectedStr = "SELECT 2"
		}
		require.Equal(t, expectedStr, q.String)
	}
}
