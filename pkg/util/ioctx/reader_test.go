// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ioctx

import (
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// mockReadCloserCtx is a mock that implements ReadCloserCtx, ReaderAtCtx, and SeekerCtx
type mockReadCloserCtx struct {
	data      []byte
	pos       int64
	closed    bool
	seekCalls int
	readCalls int
}

func newMockReadCloserCtx(data []byte) *mockReadCloserCtx {
	return &mockReadCloserCtx{
		data: data,
	}
}

func (m *mockReadCloserCtx) Read(ctx context.Context, p []byte) (int, error) {
	m.readCalls++
	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *mockReadCloserCtx) Close(ctx context.Context) error {
	m.closed = true
	return nil
}

func (m *mockReadCloserCtx) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *mockReadCloserCtx) Seek(ctx context.Context, offset int64, whence int) (int64, error) {
	m.seekCalls++
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = m.pos + offset
	case io.SeekEnd:
		newPos = int64(len(m.data)) + offset
	default:
		return 0, errors.Newf("invalid whence: %d", whence)
	}

	if newPos < 0 {
		return 0, errors.New("negative position")
	}

	m.pos = newPos
	return newPos, nil
}

// mockReadCloserCtxNoSeek implements ReadCloserCtx but NOT SeekerCtx
type mockReadCloserCtxNoSeek struct {
	data []byte
	pos  int64
}

func (m *mockReadCloserCtxNoSeek) Read(ctx context.Context, p []byte) (int, error) {
	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *mockReadCloserCtxNoSeek) Close(ctx context.Context) error {
	return nil
}

func (m *mockReadCloserCtxNoSeek) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// mockReadCloserCtxBrokenSeek implements SeekerCtx but Seek always fails
type mockReadCloserCtxBrokenSeek struct {
	data []byte
	pos  int64
}

func (m *mockReadCloserCtxBrokenSeek) Read(ctx context.Context, p []byte) (int, error) {
	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *mockReadCloserCtxBrokenSeek) Close(ctx context.Context) error {
	return nil
}

func (m *mockReadCloserCtxBrokenSeek) ReadAt(
	ctx context.Context, p []byte, off int64,
) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *mockReadCloserCtxBrokenSeek) Seek(
	ctx context.Context, offset int64, whence int,
) (int64, error) {
	return 0, errors.New("seek operation failed")
}

func TestReaderAtSeekerAdapterBasicOperations(t *testing.T) {
	ctx := context.Background()
	testData := []byte("Hello, World! This is test data for seeking.")
	mock := newMockReadCloserCtx(testData)

	adapter, err := ReaderAtSeekerAdapter(ctx, mock)
	require.NoError(t, err)
	require.NotNil(t, adapter)

	// Test that the initial seek test was called
	require.Equal(t, 1, mock.seekCalls, "adapter should test seek during construction")

	// Test Read
	buf := make([]byte, 5)
	n, err := adapter.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("Hello"), buf)

	// Test ReadAt (should read at specific offset)
	buf = make([]byte, 5)
	n, err = adapter.ReadAt(buf, 7)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("World"), buf)

	// Test Seek to start
	pos, err := adapter.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)

	// Test Seek current
	pos, err = adapter.Seek(5, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(5), pos)

	// Test Seek from end
	pos, err = adapter.Seek(-5, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(len(testData)-5), pos)

	// Test Close
	err = adapter.Close()
	require.NoError(t, err)
	require.True(t, mock.closed)
}

func TestReaderAtSeekerAdapterMissingReaderAtCtx(t *testing.T) {
	ctx := context.Background()

	// Create a reader that doesn't implement ReaderAtCtx
	reader := ReadCloserAdapter(io.NopCloser(nil))

	adapter, err := ReaderAtSeekerAdapter(ctx, reader)
	require.Error(t, err)
	require.Nil(t, adapter)
	require.Contains(t, err.Error(), "does not implement ioctx.ReaderAtCtx")
}

func TestReaderAtSeekerAdapterMissingSeekerCtx(t *testing.T) {
	ctx := context.Background()

	// Create a reader that implements ReaderAtCtx but not SeekerCtx
	reader := &mockReadCloserCtxNoSeek{
		data: []byte("test data"),
	}

	adapter, err := ReaderAtSeekerAdapter(ctx, reader)
	require.Error(t, err)
	require.Nil(t, adapter)
	require.Contains(t, err.Error(), "does not implement ioctx.SeekerCtx")
}

func TestReaderAtSeekerAdapterBrokenSeek(t *testing.T) {
	ctx := context.Background()

	// Create a reader that implements SeekerCtx but seek fails
	reader := &mockReadCloserCtxBrokenSeek{
		data: []byte("test data"),
	}

	adapter, err := ReaderAtSeekerAdapter(ctx, reader)
	require.Error(t, err)
	require.Nil(t, adapter)
	require.Contains(t, err.Error(), "test seek failed")
}

func TestReaderAtSeekerAdapterReadAtEOF(t *testing.T) {
	ctx := context.Background()
	testData := []byte("short")
	mock := newMockReadCloserCtx(testData)

	adapter, err := ReaderAtSeekerAdapter(ctx, mock)
	require.NoError(t, err)

	// Try to read past EOF
	buf := make([]byte, 10)
	n, err := adapter.ReadAt(buf, int64(len(testData)))
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)
}

func TestReaderAtSeekerAdapterInvalidSeek(t *testing.T) {
	ctx := context.Background()
	testData := []byte("test data")
	mock := newMockReadCloserCtx(testData)

	adapter, err := ReaderAtSeekerAdapter(ctx, mock)
	require.NoError(t, err)

	// Negative position
	_, err = adapter.Seek(-10, io.SeekStart)
	require.Error(t, err)
	require.Contains(t, err.Error(), "negative position")
}
