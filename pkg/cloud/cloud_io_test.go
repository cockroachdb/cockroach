// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"fmt"
	"io"
	"strings"
	"syscall"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestResumingReader tests the basic functionality of ResumingReader
func TestResumingReader(t *testing.T) {
	ctx := context.Background()
	rf := &fakeReaderFactory{
		data: "hello world",
	}

	t.Run("open-then-read", func(t *testing.T) {
		reader := NewResumingReader(ctx, rf.newReaderAt, nil, 0, 0, "", nil, nil)
		require.Nil(t, reader.Reader)
		require.Equal(t, int64(0), reader.Size)

		require.NoError(t, reader.Open(ctx))
		require.NotNil(t, reader.Reader)
		require.Equal(t, int64(len(rf.data)), reader.Size)

		actualBytes, err := io.ReadAll(reader.Reader)
		require.NoError(t, err)
		require.Equal(t, rf.data, string(actualBytes))
	})

	t.Run("open-with-retry", func(t *testing.T) {
		reader := NewResumingReader(ctx, rf.newReaderAt, nil, 0, 0, "", nil, nil)
		require.Nil(t, reader.Reader)

		injectedErr := errors.New("injected error")

		t.Run("err-count-less-than-limit", func(t *testing.T) {
			ep := newNErrorsProducer(MaxDelayedRetryAttempts-1, injectedErr)
			rfWithErr := fakeReaderFactory{
				data: "hello world",
				newReaderAtKnob: func() error {
					return ep.maybeProduceErr()
				},
			}

			reader := NewResumingReader(ctx, rfWithErr.newReaderAt, nil, 0, 0, "", nil, nil)
			require.Nil(t, reader.Reader)
			require.NoError(t, reader.Open(ctx))
			require.NotNil(t, reader.Reader)
		})

		t.Run("err-count-more-than-limit", func(t *testing.T) {
			ep := newNErrorsProducer(MaxDelayedRetryAttempts, injectedErr)
			rfWithErr := fakeReaderFactory{
				data: "hello world",
				newReaderAtKnob: func() error {
					return ep.maybeProduceErr()
				},
			}

			reader := NewResumingReader(ctx, rfWithErr.newReaderAt, nil, 0, 0, "", nil, nil)
			require.Nil(t, reader.Reader)
			require.ErrorIs(t, reader.Open(ctx), injectedErr)
		})
	})

	t.Run("read-with-explicit-reader", func(t *testing.T) {
		usedReader := &fakeReaderWithKnobs{
			reader: strings.NewReader("actual contents"),
		}

		reader := NewResumingReader(ctx, rf.newReaderAt, usedReader, 0, 0, "", nil, nil)
		actualData, err := ioctx.ReadAll(ctx, reader)
		require.NoError(t, err)
		require.Equal(t, "actual contents", string(actualData))
	})

	t.Run("read-with-retry", func(t *testing.T) {
		customErr := errors.New("injected test error")

		for _, tc := range []struct {
			name         string
			retriableErr error
			retryOnErrFn func(error) bool
		}{
			{
				name:         "default-fn",
				retriableErr: syscall.ECONNRESET,
				retryOnErrFn: nil,
			},
			{
				name:         "custom-fn",
				retriableErr: customErr,
				retryOnErrFn: func(err error) bool {
					return errors.Is(err, customErr)
				},
			},
		} {
			for _, retriable := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/retriable=%t", tc.name, retriable), func(t *testing.T) {
					injectedErr := tc.retriableErr
					if !retriable {
						injectedErr = errors.Newf("non-retriable error")
					}

					ep := newNErrorsProducer(1, injectedErr)

					rfWithErr := fakeReaderFactory{
						data: "hello world",
						afterReadKnob: func(n int, err error) error {
							return ep.maybeProduceErr()
						},
					}

					reader := NewResumingReader(ctx, rfWithErr.newReaderAt, nil, 0, 0, "", tc.retryOnErrFn, nil)
					var actualData []byte
					buf := make([]byte, 8)
					var err error
					for {
						var n int
						n, err = reader.Read(ctx, buf)
						if err != nil {
							break
						}
						actualData = append(actualData, buf[:n]...)
					}
					if err == io.EOF {
						err = nil
					}
					if retriable {
						require.NoError(t, err)
						require.Equal(t, "hello world", string(actualData))
					} else {
						require.ErrorContains(t, err, "non-retriable error")
					}
				})
			}
		}
	})
}

// TestResumingReaderErrorOnLastRead tests that if the last read of
// ResumingReader, which is expected to read 0 bytes and return io.EOF, errors
// out with a retriable error, the read still succeeds on retry. More
// concretely, this tests that ResumingReader does not attempt to call Open with
// a position equal to the size of the underlying data, since
// fakeReaderFactory.newReaderAt will return an error.
func TestResumingReaderErrorOnLastRead(t *testing.T) {
	ctx := context.Background()

	ep := newNErrorsProducer(1, syscall.ECONNRESET)
	// Produce an ECONNRESET once on a Read call at the end of the file. This
	// Read will return 0 bytes and an io.EOF error.
	afterRead := func(n int, err error) error {
		if n == 0 && errors.Is(err, io.EOF) {
			return ep.maybeProduceErr()
		}
		return nil
	}

	rf := &fakeReaderFactory{
		data:          "hello world",
		afterReadKnob: afterRead,
	}
	reader := NewResumingReader(ctx, rf.newReaderAt, nil, 0, 0, "", nil, nil)
	data, err := ioctx.ReadAll(ctx, reader)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(data))

	// Sanity check that we did inject the error at the end of the file.
	require.Equal(t, 1, ep.attempts)
}

// fakeReaderWithKnobs is a wrapper around an io.Reader that allows for
// additional knobs to be injected into Read calls.
type fakeReaderWithKnobs struct {
	reader io.Reader

	afterReadKnob func(n int, err error) error
}

var _ io.ReadCloser = &fakeReaderWithKnobs{}

func (r *fakeReaderWithKnobs) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if r.afterReadKnob != nil {
		kerr := r.afterReadKnob(n, err)
		if kerr != nil {
			return n, kerr
		}
	}

	return n, err
}

func (r *fakeReaderWithKnobs) Close() error {
	// no-op
	return nil
}

type fakeReaderFactory struct {
	data string

	afterReadKnob func(n int, err error) error

	newReaderAtKnob func() error
}

func (f *fakeReaderFactory) newReaderAt(
	ctx context.Context, pos int64,
) (io.ReadCloser, int64, error) {
	if pos < 0 || pos >= int64(len(f.data)) {
		return nil, 0, errors.Newf("cannot open reader at pos=%d", pos)
	}

	if f.newReaderAtKnob != nil {
		if err := f.newReaderAtKnob(); err != nil {
			return nil, 0, err
		}
	}

	return &fakeReaderWithKnobs{
		reader:        strings.NewReader(f.data[pos:]),
		afterReadKnob: f.afterReadKnob,
	}, int64(len(f.data)), nil
}

type nErrorsProducer struct {
	numErrors int
	err       error
	attempts  int
}

func newNErrorsProducer(n int, err error) nErrorsProducer {
	return nErrorsProducer{numErrors: n, err: err}
}

func (p *nErrorsProducer) maybeProduceErr() error {
	p.attempts++
	if p.attempts <= p.numErrors {
		return errors.Wrapf(p.err, "produced test error %d out of %d", p.attempts, p.numErrors)
	}
	return nil
}
