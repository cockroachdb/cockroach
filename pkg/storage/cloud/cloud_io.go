// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/errors"
)

// Timeout is a cluster setting used for cloud storage interactions.
var Timeout = settings.RegisterDurationSetting(
	"cloudstorage.timeout",
	"the timeout for import/export storage operations",
	10*time.Minute,
).WithPublic()

var httpCustomCA = settings.RegisterStringSetting(
	"cloudstorage.http.custom_ca",
	"custom root CA (appended to system's default CAs) for verifying certificates when interacting with HTTPS storage",
	"",
).WithPublic()

// HTTPRetryOptions defines the tunable settings which control the retry of HTTP
// operations.
var HTTPRetryOptions = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     2 * time.Second,
	MaxRetries:     32,
	Multiplier:     4,
}

// MakeHTTPClient makes an http client configured with the common settings used
// for interacting with cloud storage (timeouts, retries, CA certs, etc).
func MakeHTTPClient(settings *cluster.Settings) (*http.Client, error) {
	var tlsConf *tls.Config
	if pem := httpCustomCA.Get(&settings.SV); pem != "" {
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.Wrap(err, "could not load system root CA pool")
		}
		if !roots.AppendCertsFromPEM([]byte(pem)) {
			return nil, errors.Errorf("failed to parse root CA certificate from %q", pem)
		}
		tlsConf = &tls.Config{RootCAs: roots}
	}
	// Copy the defaults from http.DefaultTransport. We cannot just copy the
	// entire struct because it has a sync Mutex. This has the unfortunate problem
	// that if Go adds fields to DefaultTransport they won't be copied here,
	// but this is ok for now.
	t := http.DefaultTransport.(*http.Transport)
	return &http.Client{Transport: &http.Transport{
		Proxy:                 t.Proxy,
		DialContext:           t.DialContext,
		MaxIdleConns:          t.MaxIdleConns,
		IdleConnTimeout:       t.IdleConnTimeout,
		TLSHandshakeTimeout:   t.TLSHandshakeTimeout,
		ExpectContinueTimeout: t.ExpectContinueTimeout,

		// Add our custom CA.
		TLSClientConfig: tlsConf,
	}}, nil
}

// MaxDelayedRetryAttempts is the number of times the delayedRetry method will
// re-run the provided function.
const MaxDelayedRetryAttempts = 3

// DelayedRetry runs fn and re-runs it a limited number of times if it
// fails. It knows about specific kinds of errors that need longer retry
// delays than normal.
func DelayedRetry(
	ctx context.Context, customDelay func(error) time.Duration, fn func() error,
) error {
	return retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), MaxDelayedRetryAttempts, func() error {
		err := fn()
		if err == nil {
			return nil
		}
		if customDelay != nil {
			if d := customDelay(err); d > 0 {
				select {
				case <-time.After(d):
				case <-ctx.Done():
				}
			}
		}
		// See https:github.com/GoogleCloudPlatform/google-cloudimpl-go/issues/1012#issuecomment-393606797
		// which suggests this GCE error message could be due to auth quota limits
		// being reached.
		if strings.Contains(err.Error(), "net/http: timeout awaiting response headers") {
			select {
			case <-time.After(time.Second * 5):
			case <-ctx.Done():
			}
		}
		return err
	})
}

// isResumableHTTPError returns true if we can
// resume download after receiving an error 'err'.
// We can attempt to resume download if the error is ErrUnexpectedEOF.
// In particular, we should not worry about a case when error is io.EOF.
// The reason for this is two-fold:
//   1. The underlying http library converts io.EOF to io.ErrUnexpectedEOF
//   if the number of bytes transferred is less than the number of
//   bytes advertised in the Content-Length header.  So if we see
//   io.ErrUnexpectedEOF we can simply request the next range.
//   2. If the server did *not* advertise Content-Length, then
//   there is really nothing we can do: http standard says that
//   the stream ends when the server terminates connection.
// In addition, we treat connection reset by peer errors (which can
// happen if we didn't read from the connection too long due to e.g. load),
// the same as unexpected eof errors.
func isResumableHTTPError(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		sysutil.IsErrConnectionReset(err) ||
		sysutil.IsErrConnectionRefused(err)
}

// Maximum number of times we can attempt to retry reading from external storage,
// without making any progress.
const maxNoProgressReads = 3

// ReaderOpenerAt describes a function that opens a ReadCloser at the passed
// offset.
type ReaderOpenerAt func(ctx context.Context, pos int64) (io.ReadCloser, error)

// ResumingReader is a reader which retries reads in case of a transient errors.
type ResumingReader struct {
	Ctx    context.Context           // Reader context
	Opener ReaderOpenerAt            // Get additional content
	Reader io.ReadCloser             // Currently opened reader
	Pos    int64                     // How much data was received so far
	ErrFn  func(error) time.Duration // custom error delay picker
}

var _ io.ReadCloser = &ResumingReader{}

// Open opens the reader at its current offset.
func (r *ResumingReader) Open() error {
	return DelayedRetry(r.Ctx, r.ErrFn, func() error {
		var readErr error
		r.Reader, readErr = r.Opener(r.Ctx, r.Pos)
		return readErr
	})
}

// Read implements io.Reader.
func (r *ResumingReader) Read(p []byte) (int, error) {
	var lastErr error
	for retries := 0; lastErr == nil; retries++ {
		if r.Reader == nil {
			lastErr = r.Open()
		}

		if lastErr == nil {
			n, readErr := r.Reader.Read(p)
			if readErr == nil || readErr == io.EOF {
				r.Pos += int64(n)
				return n, readErr
			}
			lastErr = readErr
		}

		if !errors.IsAny(lastErr, io.EOF, io.ErrUnexpectedEOF) {
			log.Errorf(r.Ctx, "Read err: %s", lastErr)
		}

		if isResumableHTTPError(lastErr) {
			if retries >= maxNoProgressReads {
				return 0, errors.Wrap(lastErr, "multiple Read calls return no data")
			}
			log.Errorf(r.Ctx, "Retry IO: error %s", lastErr)
			lastErr = nil
			r.Reader = nil
		}
	}

	// NB: Go says Read() callers need to expect n > 0 *and* non-nil error, and do
	// something with what was read before the error, but this mostly applies to
	// err = EOF case which we handle above, so likely OK that we're discarding n
	// here and pretending it was zero.
	return 0, lastErr
}

// Close implements io.Closer.
func (r *ResumingReader) Close() error {
	if r.Reader != nil {
		return r.Reader.Close()
	}
	return nil
}

// CheckHTTPContentRangeHeader parses Content-Range header and ensures that
// range start offset is the same as the expected 'pos'. It returns the total
// size of the remote object as extracted from the header.
// See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range
func CheckHTTPContentRangeHeader(h string, pos int64) (int64, error) {
	if len(h) == 0 {
		return 0, errors.New("http server does not honor download resume")
	}

	h = strings.TrimPrefix(h, "bytes ")
	dash := strings.IndexByte(h, '-')
	if dash <= 0 {
		return 0, errors.Errorf("malformed Content-Range header: %s", h)
	}

	resume, err := strconv.ParseInt(h[:dash], 10, 64)
	if err != nil {
		return 0, errors.Errorf("malformed start offset in Content-Range header: %s", h)
	}

	if resume != pos {
		return 0, errors.Errorf(
			"expected resume position %d, found %d instead in Content-Range header: %s",
			pos, resume, h)
	}

	slash := strings.IndexByte(h, '/')
	if slash <= 0 {
		return 0, errors.Errorf("malformed Content-Range header: %s", h)
	}
	size, err := strconv.ParseInt(h[slash+1:], 10, 64)
	if err != nil {
		return 0, errors.Errorf("malformed slash offset in Content-Range header: %s", h)
	}

	return size, nil
}

// BackgroundPipe is a helper for providing a Writer that is backed by a pipe
// that has a background process reading from it. It *must* be Closed().
func BackgroundPipe(
	ctx context.Context, fn func(ctx context.Context, pr io.Reader) error,
) io.WriteCloser {
	pr, pw := io.Pipe()
	w := &backgroundPipe{w: pw, grp: ctxgroup.WithContext(ctx), ctx: ctx}
	w.grp.GoCtx(func(ctc context.Context) error {
		err := fn(ctx, pr)
		if err != nil {
			closeErr := pr.CloseWithError(err)
			err = errors.CombineErrors(err, closeErr)
		} else {
			err = pr.Close()
		}
		return err
	})
	return w
}

type backgroundPipe struct {
	w   *io.PipeWriter
	grp ctxgroup.Group
	ctx context.Context
}

// Write writes to the writer.
func (s *backgroundPipe) Write(p []byte) (int, error) {
	return s.w.Write(p)
}

// Close closes the writer, finishing the write operation.
func (s *backgroundPipe) Close() error {
	err := s.w.CloseWithError(s.ctx.Err())
	return errors.CombineErrors(err, s.grp.Wait())
}

// WriteFile is a helper for writing the content of a Reader to the given path
// of an ExternalStorage.
func WriteFile(ctx context.Context, dest ExternalStorage, basename string, src io.Reader) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	w, err := dest.Writer(ctx, basename)
	if err != nil {
		return errors.Wrap(err, "opening object for writing")
	}
	if _, err := io.Copy(w, src); err != nil {
		cancel()
		return errors.CombineErrors(w.Close(), err)
	}
	return errors.Wrap(w.Close(), "closing object")
}
