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

// mockReadCloser implements ReadCloserCtx for testing
type mockReadCloser struct {
	data   string
	offset int64
	pos    int64
}

func newMockReadCloser(data string, offset int64) *mockReadCloser {
	return &mockReadCloser{
		data:   data,
		offset: offset,
		pos:    offset,
	}
}

func (m *mockReadCloser) Read(ctx context.Context, p []byte) (int, error) {
	if m.pos >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[m.pos:])
	m.pos += int64(n)
	return n, nil
}

func (m *mockReadCloser) Close(ctx context.Context) error {
	return nil
}

// TestRandomAccessReaderReadAt tests the ReadAt implementation
func TestRandomAccessReaderReadAt(t *testing.T) {
	ctx := context.Background()
	testData := "0123456789abcdefghijklmnopqrstuvwxyz"

	// Create opener function that simulates cloud storage offset reads
	openAt := func(ctx context.Context, offset int64, endHint int64) (ReadCloserCtx, error) {
		if offset >= int64(len(testData)) {
			return nil, io.EOF
		}
		return newMockReadCloser(testData, offset), nil
	}

	reader := NewRandomAccessReader(ctx, int64(len(testData)), openAt)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	t.Run("read-at-beginning", func(t *testing.T) {
		p := make([]byte, 5)
		n, err := reader.ReadAt(p, 0)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, "01234", string(p))
	})

	t.Run("read-at-middle", func(t *testing.T) {
		p := make([]byte, 5)
		n, err := reader.ReadAt(p, 10)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, "abcde", string(p))
	})

	t.Run("read-at-end", func(t *testing.T) {
		p := make([]byte, 5)
		n, err := reader.ReadAt(p, int64(len(testData))-5)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, "vwxyz", string(p))
	})

	t.Run("read-past-end", func(t *testing.T) {
		p := make([]byte, 5)
		_, err := reader.ReadAt(p, int64(len(testData)))
		require.Equal(t, io.EOF, err)
	})

	t.Run("read-partial-at-end", func(t *testing.T) {
		// Request 10 bytes but only 3 available
		p := make([]byte, 10)
		n, err := reader.ReadAt(p, int64(len(testData))-3)
		// io.ReadFull returns ErrUnexpectedEOF when it can't fill the buffer
		require.Equal(t, io.ErrUnexpectedEOF, err)
		require.Equal(t, 3, n)
		require.Equal(t, "xyz", string(p[:n]))
	})

	t.Run("concurrent-reads", func(t *testing.T) {
		// ReadAt should be safe for concurrent calls
		done := make(chan bool, 2)

		go func() {
			p := make([]byte, 5)
			n, err := reader.ReadAt(p, 0)
			require.NoError(t, err)
			require.Equal(t, 5, n)
			require.Equal(t, "01234", string(p))
			done <- true
		}()

		go func() {
			p := make([]byte, 5)
			n, err := reader.ReadAt(p, 10)
			require.NoError(t, err)
			require.Equal(t, 5, n)
			require.Equal(t, "abcde", string(p))
			done <- true
		}()

		<-done
		<-done
	})
}

// TestRandomAccessReaderSeek tests the Seek implementation
func TestRandomAccessReaderSeek(t *testing.T) {
	ctx := context.Background()
	testData := "0123456789"

	openAt := func(ctx context.Context, offset int64, endHint int64) (ReadCloserCtx, error) {
		if offset >= int64(len(testData)) {
			return nil, io.EOF
		}
		return newMockReadCloser(testData, offset), nil
	}

	reader := NewRandomAccessReader(ctx, int64(len(testData)), openAt)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	t.Run("seek-start", func(t *testing.T) {
		pos, err := reader.Seek(5, io.SeekStart)
		require.NoError(t, err)
		require.Equal(t, int64(5), pos)
	})

	t.Run("seek-current", func(t *testing.T) {
		// Position is 5 from previous test
		pos, err := reader.Seek(2, io.SeekCurrent)
		require.NoError(t, err)
		require.Equal(t, int64(7), pos)
	})

	t.Run("seek-end", func(t *testing.T) {
		pos, err := reader.Seek(-3, io.SeekEnd)
		require.NoError(t, err)
		require.Equal(t, int64(7), pos)
	})

	t.Run("seek-negative", func(t *testing.T) {
		_, err := reader.Seek(-1, io.SeekStart)
		require.Error(t, err)
		require.Contains(t, err.Error(), "negative position")
	})

	t.Run("seek-invalid-whence", func(t *testing.T) {
		_, err := reader.Seek(0, 999)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid whence")
	})
}

// TestRandomAccessReaderRead tests sequential Read operations
func TestRandomAccessReaderRead(t *testing.T) {
	ctx := context.Background()
	testData := "0123456789"

	openAt := func(ctx context.Context, offset int64, endHint int64) (ReadCloserCtx, error) {
		if offset >= int64(len(testData)) {
			return nil, io.EOF
		}
		return newMockReadCloser(testData, offset), nil
	}

	reader := NewRandomAccessReader(ctx, int64(len(testData)), openAt)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	t.Run("sequential-reads", func(t *testing.T) {
		p := make([]byte, 3)

		// First read
		n, err := reader.Read(p)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, "012", string(p))

		// Second read
		n, err = reader.Read(p)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, "345", string(p))

		// Third read
		n, err = reader.Read(p)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, "678", string(p))
	})
}

// TestRandomAccessReaderSeekThenRead tests Seek followed by Read
func TestRandomAccessReaderSeekThenRead(t *testing.T) {
	ctx := context.Background()
	testData := "0123456789"

	openAt := func(ctx context.Context, offset int64, endHint int64) (ReadCloserCtx, error) {
		if offset >= int64(len(testData)) {
			return nil, io.EOF
		}
		return newMockReadCloser(testData, offset), nil
	}

	reader := NewRandomAccessReader(ctx, int64(len(testData)), openAt)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	// Seek to position 5
	pos, err := reader.Seek(5, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(5), pos)

	// Read from that position
	p := make([]byte, 3)
	n, err := reader.Read(p)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "567", string(p))
}

// TestRandomAccessReaderOpenerError tests error handling from opener function
func TestRandomAccessReaderOpenerError(t *testing.T) {
	ctx := context.Background()

	expectedErr := errors.New("storage unavailable")
	openAt := func(ctx context.Context, offset int64, endHint int64) (ReadCloserCtx, error) {
		return nil, expectedErr
	}

	reader := NewRandomAccessReader(ctx, 100, openAt)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	t.Run("readat-opener-error", func(t *testing.T) {
		p := make([]byte, 5)
		_, err := reader.ReadAt(p, 0)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})

	t.Run("read-opener-error", func(t *testing.T) {
		p := make([]byte, 5)
		_, err := reader.Read(p)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
	})
}

// TestRandomAccessReaderEmptyFile tests reading from an empty file
func TestRandomAccessReaderEmptyFile(t *testing.T) {
	ctx := context.Background()

	openAt := func(ctx context.Context, offset int64, endHint int64) (ReadCloserCtx, error) {
		return newMockReadCloser("", offset), nil
	}

	reader := NewRandomAccessReader(ctx, 0, openAt)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	t.Run("readat-empty", func(t *testing.T) {
		p := make([]byte, 5)
		_, err := reader.ReadAt(p, 0)
		require.Equal(t, io.EOF, err)
	})
}
