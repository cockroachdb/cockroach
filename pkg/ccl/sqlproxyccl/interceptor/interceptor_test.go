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
	"io"
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

	reader, writer := io.Pipe()

	// Negative buffer size.
	pgi, err := newPgInterceptor(reader, writer, -1)
	require.EqualError(t, err, ErrSmallBuffer.Error())
	require.Nil(t, pgi)

	// Small buffer size.
	pgi, err = newPgInterceptor(reader, writer, pgHeaderSizeBytes-1)
	require.EqualError(t, err, ErrSmallBuffer.Error())
	require.Nil(t, pgi)

	// Buffer that fits the header exactly.
	pgi, err = newPgInterceptor(reader, writer, pgHeaderSizeBytes)
	require.NoError(t, err)
	require.NotNil(t, pgi)
	require.Len(t, pgi.buf, pgHeaderSizeBytes)

	// Normal buffer size.
	pgi, err = newPgInterceptor(reader, writer, 1024)
	require.NoError(t, err)
	require.NotNil(t, pgi)
	require.Len(t, pgi.buf, 1024)
	require.Equal(t, reader, pgi.src)
	require.Equal(t, writer, pgi.dst)
}

func TestPGInterceptor_ensureNextNBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("invalid n", func(t *testing.T) {
		pgi, err := newPgInterceptor(nil /* src */, nil /* dst */, 8)
		require.NoError(t, err)

		require.EqualError(t, pgi.ensureNextNBytes(-1),
			"invalid number of bytes -1 for buffer size 8")
		require.EqualError(t, pgi.ensureNextNBytes(9),
			"invalid number of bytes 9 for buffer size 8")
	})

	t.Run("buffer already has n bytes", func(t *testing.T) {
		buf := bytes.NewBufferString("foobarbaz")

		pgi, err := newPgInterceptor(iotest.OneByteReader(buf), nil /* dst */, 8)
		require.NoError(t, err)

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

		pgi, err := newPgInterceptor(iotest.OneByteReader(buf), nil /* dst */, 9)
		require.NoError(t, err)

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

		pgi, err := newPgInterceptor(buf, nil /* dst */, 10)
		require.NoError(t, err)

		// Request for only 1 byte.
		require.NoError(t, pgi.ensureNextNBytes(1))

		// Verify that buf has "foobarbaz".
		require.Equal(t, "foobarbaz", string(pgi.buf[pgi.readPos:pgi.writePos]))

		// Should be a no-op.
		_, err = buf.WriteString("car")
		require.NoError(t, err)
		require.NoError(t, pgi.ensureNextNBytes(9))
		require.Equal(t, 3, buf.Len())
		require.Equal(t, "foobarbaz", string(pgi.buf[pgi.readPos:pgi.writePos]))
	})
}

func TestPGInterceptor_PeekMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("interceptor is closed", func(t *testing.T) {
		pgi, err := newPgInterceptor(nil /* src */, nil /* dst */, 10)
		require.NoError(t, err)
		pgi.Close()

		typ, size, err := pgi.PeekMsg()
		require.EqualError(t, err, ErrInterceptorClosed.Error())
		require.Equal(t, byte(0), typ)
		require.Equal(t, 0, size)
	})

	t.Run("read error", func(t *testing.T) {
		r := iotest.ErrReader(errors.New("read error"))

		pgi, err := newPgInterceptor(r, nil /* dst */, 10)
		require.NoError(t, err)

		typ, size, err := pgi.PeekMsg()
		require.EqualError(t, err, "read error")
		require.Equal(t, byte(0), typ)
		require.Equal(t, 0, size)
	})

	t.Run("protocol error", func(t *testing.T) {
		data := make([]byte, 10)
		buf := new(bytes.Buffer)
		_, err := buf.Write(data)
		require.NoError(t, err)

		pgi, err := newPgInterceptor(buf, nil /* dst */, 10)
		require.NoError(t, err)

		typ, size, err := pgi.PeekMsg()
		require.EqualError(t, err, ErrProtocolError.Error())
		require.Equal(t, byte(0), typ)
		require.Equal(t, 0, size)
	})

	t.Run("successful", func(t *testing.T) {
		buf := new(bytes.Buffer)
		_, err := buf.Write((&pgproto3.Query{String: "SELECT 1"}).Encode(nil))
		require.NoError(t, err)

		pgi, err := newPgInterceptor(buf, nil /* dst */, 10)
		require.NoError(t, err)

		typ, size, err := pgi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, byte(pgwirebase.ClientMsgSimpleQuery), typ)
		require.Equal(t, 9, size)
		require.Equal(t, 4, buf.Len())

		// Invoking Peek should not advance the interceptor.
		typ, size, err = pgi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, byte(pgwirebase.ClientMsgSimpleQuery), typ)
		require.Equal(t, 9, size)
		require.Equal(t, 4, buf.Len())
	})
}

func TestPGInterceptor_WriteMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("interceptor is closed", func(t *testing.T) {
		pgi, err := newPgInterceptor(nil /* src */, nil /* dst */, 10)
		require.NoError(t, err)
		pgi.Close()

		n, err := pgi.WriteMsg([]byte{})
		require.EqualError(t, err, ErrInterceptorClosed.Error())
		require.Equal(t, 0, n)
	})

	t.Run("write error", func(t *testing.T) {
		pgi, err := newPgInterceptor(nil /* src */, &errReadWriter{w: io.Discard}, 10)
		require.NoError(t, err)

		n, err := pgi.WriteMsg([]byte{})
		require.EqualError(t, err, io.ErrClosedPipe.Error())
		require.Equal(t, 0, n)
		require.True(t, pgi.closed)
	})

	t.Run("successful", func(t *testing.T) {
		buf := new(bytes.Buffer)
		pgi, err := newPgInterceptor(nil /* src */, buf, 10)
		require.NoError(t, err)

		n, err := pgi.WriteMsg([]byte("hello"))
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.False(t, pgi.closed)
		require.Equal(t, "hello", buf.String())
	})
}

func TestPGInterceptor_ReadMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("interceptor is closed", func(t *testing.T) {
		pgi, err := newPgInterceptor(nil /* src */, nil /* dst */, 10)
		require.NoError(t, err)
		pgi.Close()

		msg, err := pgi.ReadMsg()
		require.EqualError(t, err, ErrInterceptorClosed.Error())
		require.Nil(t, msg)
	})

	q := (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)

	buildSrc := func(t *testing.T, count int) *bytes.Buffer {
		t.Helper()
		src := new(bytes.Buffer)
		for i := 0; i < count; i++ {
			// Alternate between SELECT 1 and 2 to ensure correctness.
			if i%2 == 0 {
				q[12] = '1'
			} else {
				q[12] = '2'
			}
			_, err := src.Write(q)
			require.NoError(t, err)
		}
		return src
	}

	t.Run("message fits", func(t *testing.T) {
		const count = 101 // Inclusive of warm-up run in AllocsPerRun.

		buf := buildSrc(t, count)

		// Set buffer's size to be a multiple of the message so that we'll
		// always hit the case where the message fits.
		pgi, err := newPgInterceptor(buf, nil /* dst */, len(q)*3)
		require.NoError(t, err)

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

	t.Run("message overflows", func(t *testing.T) {
		const count = 101 // Inclusive of warm-up run in AllocsPerRun.

		buf := buildSrc(t, count)

		// Set the buffer to be large enough to fit more bytes than the header,
		// but not the entire message.
		pgi, err := newPgInterceptor(buf, nil /* dst */, 7)
		require.NoError(t, err)

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

	t.Run("read error after allocate", func(t *testing.T) {
		q := (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)
		buf := new(bytes.Buffer)
		_, err := buf.Write(q)
		require.NoError(t, err)

		src := &errReadWriter{r: buf, count: 2}
		pgi, err := newPgInterceptor(src, nil /* dst */, 6)
		require.NoError(t, err)

		msg, err := pgi.ReadMsg()
		require.EqualError(t, err, io.ErrClosedPipe.Error())
		require.Nil(t, msg)

		// Ensure that interceptor is closed.
		require.True(t, pgi.closed)
		require.Equal(t, 8, buf.Len())
	})
}

func TestPGInterceptor_ForwardMsg(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("interceptor is closed", func(t *testing.T) {
		pgi, err := newPgInterceptor(nil /* src */, nil /* dst */, 10)
		require.NoError(t, err)
		pgi.Close()

		n, err := pgi.ForwardMsg()
		require.EqualError(t, err, ErrInterceptorClosed.Error())
		require.Equal(t, 0, n)
	})

	q := (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)

	buildSrc := func(t *testing.T, count int) *bytes.Buffer {
		t.Helper()
		src := new(bytes.Buffer)
		for i := 0; i < count; i++ {
			// Alternate between SELECT 1 and 2 to ensure correctness.
			if i%2 == 0 {
				q[12] = '1'
			} else {
				q[12] = '2'
			}
			_, err := src.Write(q)
			require.NoError(t, err)
		}
		return src
	}

	validateDst := func(t *testing.T, dst io.Reader, count int) {
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

	t.Run("message fits", func(t *testing.T) {
		const count = 101 // Inclusive of warm-up run in AllocsPerRun.

		src := buildSrc(t, count)
		dst := new(bytes.Buffer)

		// Set buffer's size to be a multiple of the message so that we'll
		// always hit the case where the message fits.
		pgi, err := newPgInterceptor(src, dst, len(q)*3)
		require.NoError(t, err)

		// Forward all the messages, and ensure 0 allocations.
		n := testing.AllocsPerRun(count-1, func() {
			n, err := pgi.ForwardMsg()
			require.NoError(t, err)
			require.Equal(t, 14, n)
		})
		require.Equal(t, float64(0), n, "should not allocate")
		require.Equal(t, 0, src.Len())

		// Validate messages.
		validateDst(t, dst, count)
		require.Equal(t, 0, dst.Len())
	})

	t.Run("message overflows", func(t *testing.T) {
		const count = 151 // Inclusive of warm-up run in AllocsPerRun.

		src := buildSrc(t, count)
		dst := new(bytes.Buffer)

		// Set the buffer to be large enough to fit more bytes than the header,
		// but not the entire message.
		pgi, err := newPgInterceptor(src, dst, 7)
		require.NoError(t, err)

		n := testing.AllocsPerRun(count-1, func() {
			n, err := pgi.ForwardMsg()
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

	t.Run("write error", func(t *testing.T) {
		q := (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)
		dst := new(bytes.Buffer)

		pgi, err := newPgInterceptor(src, &errReadWriter{w: dst, count: 2}, 6)
		require.NoError(t, err)

		n, err := pgi.ForwardMsg()
		require.EqualError(t, err, io.ErrClosedPipe.Error())
		require.Equal(t, 6, n)

		// Ensure that interceptor is closed.
		require.True(t, pgi.closed)
		require.Equal(t, 6, dst.Len())
	})
}

func TestPGInterceptor_ReadSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("interceptor is closed", func(t *testing.T) {
		buf := bytes.NewBufferString("foobarbaz")

		pgi, err := newPgInterceptor(buf, nil /* dst */, 9)
		require.NoError(t, err)
		require.NoError(t, pgi.ensureNextNBytes(1))

		require.Equal(t, 9, pgi.ReadSize())
		pgi.Close()
		require.Equal(t, 0, pgi.ReadSize())
	})

	t.Run("valid", func(t *testing.T) {
		buf := bytes.NewBufferString("foobarbazz")
		pgi, err := newPgInterceptor(iotest.OneByteReader(buf), nil /* dst */, 10)
		require.NoError(t, err)

		// No reads to internal buffer.
		require.Equal(t, 0, pgi.ReadSize())

		// Attempt reads to buffer.
		require.NoError(t, pgi.ensureNextNBytes(3))
		require.Equal(t, 3, pgi.ReadSize())

		// Read until buffer is full.
		require.NoError(t, pgi.ensureNextNBytes(10))
		require.Equal(t, 10, pgi.ReadSize())
	})
}

func TestPGInterceptor_WriteSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("interceptor is closed", func(t *testing.T) {
		pgi, err := newPgInterceptor(nil /* src */, nil /* dst */, 9)
		require.NoError(t, err)

		require.Equal(t, 9, pgi.WriteSize())
		pgi.Close()
		require.Equal(t, 0, pgi.WriteSize())
	})

	t.Run("valid", func(t *testing.T) {
		buf := bytes.NewBufferString("foobarbazz")
		pgi, err := newPgInterceptor(iotest.OneByteReader(buf), nil /* dst */, 10)
		require.NoError(t, err)

		// No writes to internal buffer.
		require.Equal(t, 10, pgi.WriteSize())

		// Attempt writes to buffer.
		require.NoError(t, pgi.ensureNextNBytes(3))
		require.Equal(t, 7, pgi.WriteSize())

		// Attempt more writes to buffer until full.
		require.NoError(t, pgi.ensureNextNBytes(10))
		require.Equal(t, 0, pgi.WriteSize())
	})
}

func TestPGInterceptor_Close(t *testing.T) {
	defer leaktest.AfterTest(t)()
	pgi, err := newPgInterceptor(nil /* src */, nil /* dst */, 10)
	require.NoError(t, err)
	require.False(t, pgi.closed)
	pgi.Close()
	require.True(t, pgi.closed)
}

// TestFrontendInterceptor tests the FrontendInterceptor. Note that the tests
// here are shallow. For detailed ones, see the tests for the internal
// interceptor.
func TestFrontendInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(nil)

	t.Run("bufSize too small", func(t *testing.T) {
		fi, err := NewFrontendInterceptor(nil /* src */, nil /* dst */, 1)
		require.Error(t, err)
		require.Nil(t, fi)
	})

	t.Run("PeekMsg returns the right message type", func(t *testing.T) {
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)

		fi, err := NewFrontendInterceptor(src, nil /* dst */, 16)
		require.NoError(t, err)
		require.NotNil(t, fi)

		typ, size, err := fi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, pgwirebase.ServerMsgReady, typ)
		require.Equal(t, 1, size)
	})

	t.Run("WriteMsg writes data to dst", func(t *testing.T) {
		dst := new(bytes.Buffer)
		fi, err := NewFrontendInterceptor(nil /* src */, dst, 10)
		require.NoError(t, err)
		require.NotNil(t, fi)

		// This is a frontend interceptor, so writing goes to the client.
		n, err := fi.WriteMsg(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		require.NoError(t, err)
		require.Equal(t, 6, n)
		require.Equal(t, 6, dst.Len())
	})

	t.Run("ReadMsg decodes the message correctly", func(t *testing.T) {
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)

		fi, err := NewFrontendInterceptor(src, nil /* dst */, 16)
		require.NoError(t, err)
		require.NotNil(t, fi)

		msg, err := fi.ReadMsg()
		require.NoError(t, err)
		rmsg, ok := msg.(*pgproto3.ReadyForQuery)
		require.True(t, ok)
		require.Equal(t, byte('I'), rmsg.TxStatus)
	})

	t.Run("ForwardMsg forwards data to dst", func(t *testing.T) {
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)
		dst := new(bytes.Buffer)

		fi, err := NewFrontendInterceptor(src, dst, 16)
		require.NoError(t, err)
		require.NotNil(t, fi)

		n, err := fi.ForwardMsg()
		require.NoError(t, err)
		require.Equal(t, 6, n)
		require.Equal(t, 6, dst.Len())
	})
}

// TestBackendInterceptor tests the BackendInterceptor. Note that the tests
// here are shallow. For detailed ones, see the tests for the internal
// interceptor.
func TestBackendInterceptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	q := (&pgproto3.Query{String: "SELECT 1"}).Encode(nil)

	t.Run("bufSize too small", func(t *testing.T) {
		bi, err := NewBackendInterceptor(nil /* src */, nil /* dst */, 1)
		require.Error(t, err)
		require.Nil(t, bi)
	})

	t.Run("PeekMsg returns the right message type", func(t *testing.T) {
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)

		bi, err := NewBackendInterceptor(src, nil /* dst */, 16)
		require.NoError(t, err)
		require.NotNil(t, bi)

		typ, size, err := bi.PeekMsg()
		require.NoError(t, err)
		require.Equal(t, pgwirebase.ClientMsgSimpleQuery, typ)
		require.Equal(t, 9, size)
	})

	t.Run("WriteMsg writes data to dst", func(t *testing.T) {
		dst := new(bytes.Buffer)
		bi, err := NewBackendInterceptor(nil /* src */, dst, 10)
		require.NoError(t, err)
		require.NotNil(t, bi)

		// This is a backend interceptor, so writing goes to the server.
		n, err := bi.WriteMsg(&pgproto3.Query{String: "SELECT 1"})
		require.NoError(t, err)
		require.Equal(t, 14, n)
		require.Equal(t, 14, dst.Len())
	})

	t.Run("ReadMsg decodes the message correctly", func(t *testing.T) {
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)

		bi, err := NewBackendInterceptor(src, nil /* dst */, 16)
		require.NoError(t, err)
		require.NotNil(t, bi)

		msg, err := bi.ReadMsg()
		require.NoError(t, err)
		rmsg, ok := msg.(*pgproto3.Query)
		require.True(t, ok)
		require.Equal(t, "SELECT 1", rmsg.String)
	})

	t.Run("ForwardMsg forwards data to dst", func(t *testing.T) {
		src := new(bytes.Buffer)
		_, err := src.Write(q)
		require.NoError(t, err)
		dst := new(bytes.Buffer)

		bi, err := NewBackendInterceptor(src, dst, 16)
		require.NoError(t, err)
		require.NotNil(t, bi)

		n, err := bi.ForwardMsg()
		require.NoError(t, err)
		require.Equal(t, 14, n)
		require.Equal(t, 14, dst.Len())
	})
}

// TestSimpleProxy illustrates how the frontend and backend interceptors can be
// used as a proxy.
func TestSimpleProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const bufferSize = 16

	// These represents connections for client<->proxy and proxy<->server.
	fromClient := new(bytes.Buffer)
	toClient := new(bytes.Buffer)
	fromServer := new(bytes.Buffer)
	toServer := new(bytes.Buffer)

	// Create client and server interceptors.
	clientInt, err := NewBackendInterceptor(fromClient, toServer, bufferSize)
	require.NoError(t, err)
	serverInt, err := NewFrontendInterceptor(fromServer, toClient, bufferSize)
	require.NoError(t, err)

	t.Run("client to server", func(t *testing.T) {
		// Client sends a list of SQL queries.
		queries := []pgproto3.FrontendMessage{
			&pgproto3.Query{String: "SELECT 1"},
			&pgproto3.Query{String: "SELECT * FROM foo.bar"},
			&pgproto3.Query{String: "UPDATE foo SET x = 42"},
			&pgproto3.Sync{},
			&pgproto3.Terminate{},
		}
		for _, msg := range queries {
			_, err := fromClient.Write(msg.Encode(nil))
			require.NoError(t, err)
		}
		totalBytes := fromClient.Len()

		customQuery := &pgproto3.Query{
			String: "SELECT * FROM crdb_internal.serialize_session()"}

		for {
			typ, _, err := clientInt.PeekMsg()
			require.NoError(t, err)

			// Forward message to server.
			_, err = clientInt.ForwardMsg()
			require.NoError(t, err)

			if typ == pgwirebase.ClientMsgTerminate {
				// Right before we terminate, we could also craft a custom
				// message, and send it to the server.
				_, err := clientInt.WriteMsg(customQuery)
				require.NoError(t, err)
				break
			}
		}
		require.Equal(t, 0, fromClient.Len())
		require.Equal(t, totalBytes+len(customQuery.Encode(nil)), toServer.Len())
	})

	t.Run("server to client", func(t *testing.T) {
		// Server sends back responses.
		queries := []pgproto3.BackendMessage{
			// Forward these back to the client.
			&pgproto3.CommandComplete{CommandTag: []byte("averylongstring")},
			&pgproto3.BackendKeyData{ProcessID: 100, SecretKey: 42},
			// Do not forward back to the client.
			&pgproto3.CommandComplete{CommandTag: []byte("short")},
			// Terminator.
			&pgproto3.ReadyForQuery{},
		}
		for _, msg := range queries {
			_, err := fromServer.Write(msg.Encode(nil))
			require.NoError(t, err)
		}
		// Exclude bytes from second message.
		totalBytes := fromServer.Len() - len(queries[2].Encode(nil))

		for {
			typ, size, err := serverInt.PeekMsg()
			require.NoError(t, err)

			switch typ {
			case pgwirebase.ServerMsgCommandComplete:
				// Assuming that we're only interested in small messages, then
				// we could skip all the large ones.
				if size > 12 {
					_, err := serverInt.ForwardMsg()
					require.NoError(t, err)
					continue
				}

				// Decode message.
				msg, err := serverInt.ReadMsg()
				require.NoError(t, err)

				// Once we've decoded the message, we could store the message
				// somewhere, and not forward it back to the client.
				dmsg, ok := msg.(*pgproto3.CommandComplete)
				require.True(t, ok)
				require.Equal(t, "short", string(dmsg.CommandTag))
			case pgwirebase.ServerMsgBackendKeyData:
				msg, err := serverInt.ReadMsg()
				require.NoError(t, err)

				dmsg, ok := msg.(*pgproto3.BackendKeyData)
				require.True(t, ok)

				// We could even rewrite the message before sending it back to
				// the client.
				dmsg.SecretKey = 100

				_, err = serverInt.WriteMsg(dmsg)
				require.NoError(t, err)
			default:
				// Forward message that we're not interested to the client.
				_, err := serverInt.ForwardMsg()
				require.NoError(t, err)
			}

			if typ == pgwirebase.ServerMsgReady {
				break
			}
		}
		require.Equal(t, 0, fromServer.Len())
		require.Equal(t, totalBytes, toClient.Len())
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
