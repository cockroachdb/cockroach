// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// Timeout is a cluster setting used for cloud storage interactions.
var Timeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"cloudstorage.timeout",
	"the timeout for import/export storage operations",
	10*time.Minute,
	settings.WithPublic)

var httpCustomCA = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	"cloudstorage.http.custom_ca",
	"custom root CA (appended to system's default CAs) for verifying certificates when interacting with HTTPS storage",
	"",
	settings.WithPublic)

// WriteChunkSize is used to control the size of each chunk that is buffered and
// uploaded by the cloud storage client.
var WriteChunkSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"cloudstorage.write_chunk.size",
	"controls the size of each file chunk uploaded by the cloud storage client",
	5<<20,
)

var retryConnectionTimedOut = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"cloudstorage.connection_timed_out_retries.enabled",
	"retry generic connection timed out errors; use with extreme caution",
	false,
)

// HTTPRetryOptions defines the tunable settings which control the retry of HTTP
// operations.
var HTTPRetryOptions = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     2 * time.Second,
	MaxRetries:     32,
	Multiplier:     4,
}

var httpMetrics = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"cloudstorage.http.detailed_metrics.enabled",
	"enabled collection of detailed metrics on cloud http requests",
	true)

// MakeHTTPClient makes an http client configured with the common settings used
// for interacting with cloud storage (timeouts, retries, CA certs, etc).
func MakeHTTPClient(
	settings *cluster.Settings, metrics *Metrics, cloud, bucket, client string,
) (*http.Client, error) {
	t, err := MakeTransport(settings, metrics, cloud, bucket, client)
	if err != nil {
		return nil, err
	}
	return MakeHTTPClientForTransport(t)
}

// MakeHTTPClientForTransport creates a new http.Client with the given
// transport.
//
// NB: This indirection is a little silly. But the goal is to prevent
// us from modifying the defaults on some clients and not others.
func MakeHTTPClientForTransport(t http.RoundTripper) (*http.Client, error) {
	return &http.Client{Transport: t}, nil
}

// MakeTransport makes an http transport configured with the common settings
// used for interacting with cloud storage (timeouts, retries, CA certs, etc).
// Prefer MakeHTTPClient where possible.
func MakeTransport(
	settings *cluster.Settings, metrics *Metrics, cloud, bucket, client string,
) (*http.Transport, error) {
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

	t := http.DefaultTransport.(*http.Transport).Clone()

	// Add our custom CA.
	t.TLSClientConfig = tlsConf
	// Bump up the default idle conn pool size as we have many parallel workers in
	// most bulk jobs.
	t.MaxIdleConnsPerHost = 64
	if metrics != nil {
		t.DialContext = metrics.NetMetrics.Wrap(t.DialContext, cloud, bucket, client)
	}
	return t, nil
}

// MaxDelayedRetryAttempts is the number of times the delayedRetry method will
// re-run the provided function.
const MaxDelayedRetryAttempts = 3

// DelayedRetry runs fn and re-runs it a limited number of times if it
// fails. It knows about specific kinds of errors that need longer retry
// delays than normal.
func DelayedRetry(
	ctx context.Context, opName string, customDelay func(error) time.Duration, fn func() error,
) error {
	ctx, sp := tracing.ChildSpan(ctx, fmt.Sprintf("cloud.DelayedRetry.%s", opName))
	defer sp.Finish()

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

// IsResumableHTTPError returns true if we can
// resume download after receiving an error 'err'.
// We can attempt to resume download if the error is ErrUnexpectedEOF.
// In particular, we should not worry about a case when error is io.EOF.
// The reason for this is two-fold:
//  1. The underlying http library converts io.EOF to io.ErrUnexpectedEOF
//     if the number of bytes transferred is less than the number of
//     bytes advertised in the Content-Length header.  So if we see
//     io.ErrUnexpectedEOF we can simply request the next range.
//  2. If the server did *not* advertise Content-Length, then
//     there is really nothing we can do: http standard says that
//     the stream ends when the server terminates connection.
//
// In addition, we treat connection reset by peer errors (which can
// happen if we didn't read from the connection too long due to e.g. load),
// the same as unexpected eof errors.
func IsResumableHTTPError(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) ||
		sysutil.IsErrConnectionReset(err) ||
		sysutil.IsErrConnectionRefused(err)
}

// ResumingReaderRetryOnErrFnForSettings returns a function that can
// be passed as a RetryOnErrFn to NewResumingReader.
func ResumingReaderRetryOnErrFnForSettings(
	ctx context.Context, st *cluster.Settings,
) func(error) bool {
	return func(err error) bool {
		if IsResumableHTTPError(err) {
			return true
		}

		retryTimeouts := retryConnectionTimedOut.Get(&st.SV)
		if retryTimeouts && sysutil.IsErrTimedOut(err) {
			log.Warningf(ctx, "retrying connection timed out because %s = true", retryConnectionTimedOut.Name())
			return true
		}
		return false
	}
}

// Maximum number of times we can attempt to retry reading from external storage,
// without making any progress.
const maxNoProgressReads = 3

// ReaderOpenerAt describes a function that opens a ReadCloser at the passed
// offset.
type ReaderOpenerAt func(ctx context.Context, pos int64) (io.ReadCloser, int64, error)

// ResumingReader is a reader which retries reads in case of a transient errors.
type ResumingReader struct {
	Opener       ReaderOpenerAt   // Get additional content
	Reader       io.ReadCloser    // Currently opened reader
	Filename     string           // Used for logging
	Pos          int64            // How much data was received so far
	Size         int64            // Total size of the file
	RetryOnErrFn func(error) bool // custom retry-on-error function
	// ErrFn injects a delay between retries on errors. nil means no delay.
	ErrFn func(error) time.Duration
}

var _ ioctx.ReadCloserCtx = &ResumingReader{}

// NewResumingReader returns a ResumingReader instance. Reader does not have to
// be provided, and will be created with the opener if it's not provided. Size
// can also be empty, and will be determined by the opener on the next open of
// the file.
func NewResumingReader(
	ctx context.Context,
	opener ReaderOpenerAt,
	reader io.ReadCloser,
	pos int64,
	size int64,
	filename string,
	retryOnErrFn func(error) bool,
	errFn func(error) time.Duration,
) *ResumingReader {
	r := &ResumingReader{
		Opener:       opener,
		Reader:       reader,
		Pos:          pos,
		Size:         size,
		Filename:     filename,
		RetryOnErrFn: retryOnErrFn,
		ErrFn:        errFn,
	}
	if r.RetryOnErrFn == nil {
		log.Warning(ctx, "no RetryOnErrFn specified when configuring ResumingReader, setting to default value")
		r.RetryOnErrFn = sysutil.IsErrConnectionReset
	}
	return r
}

// Open opens the reader at its current offset.
func (r *ResumingReader) Open(ctx context.Context) error {
	if r.Size > 0 && r.Pos >= r.Size {
		// Don't try to open a file if the size has been set and the position is
		// at size. This generally results in an invalid range error for the
		// request. Note that we still allow reads at Pos 0 even if Size is 0
		// since this seems to be allowed in the cloud SDKs.
		return io.EOF
	}

	return DelayedRetry(ctx, "Open", r.ErrFn, func() error {
		var readErr error
		r.Reader, r.Size, readErr = r.Opener(ctx, r.Pos)
		if readErr != nil {
			return errors.Wrapf(readErr, "open %s", r.Filename)
		}
		return nil
	})
}

// Read implements ioctx.ReaderCtx.
func (r *ResumingReader) Read(ctx context.Context, p []byte) (int, error) {
	ctx, sp := tracing.ChildSpan(ctx, "cloud.ResumingReader.Read")
	defer sp.Finish()

	var read int

	var lastErr error
	for retries := 0; lastErr == nil; retries++ {
		if r.Reader == nil {
			lastErr = r.Open(ctx)
		}

		if lastErr == nil {
			n, readErr := r.Reader.Read(p[read:])
			read += n
			r.Pos += int64(n)
			if readErr == nil || readErr == io.EOF {
				return read, readErr
			}
			if r.Size > 0 && r.Pos == r.Size {
				log.Warningf(ctx, "read %s ignoring read error received after completed read (%d): %v", r.Filename, r.Pos, readErr)
				return read, io.EOF
			}
			lastErr = errors.Wrapf(readErr, "read %s", r.Filename)
		}

		if !errors.IsAny(lastErr, io.EOF, io.ErrUnexpectedEOF) {
			log.Errorf(ctx, "%s", lastErr)
		}

		// Use the configured retry-on-error decider to check for a resumable error.
		if r.RetryOnErrFn(lastErr) {
			if retries >= maxNoProgressReads {
				return read, errors.Wrapf(lastErr, "multiple Read calls (%d) return no data", retries)
			}
			log.Errorf(ctx, "Retry IO error: %s", lastErr)
			lastErr = nil
			if r.Reader != nil {
				r.Reader.Close()
			}
			r.Reader = nil
		}
	}

	// NB: Go says Read() callers need to expect n > 0 *and* non-nil error, and do
	// something with what was read before the error, but this mostly applies to
	// err = EOF case which we handle above, so likely OK that we're discarding n
	// here and pretending it was zero.
	return read, lastErr
}

// Close implements io.Closer.
func (r *ResumingReader) Close(ctx context.Context) error {
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
	w.grp.GoCtx(func(ctx context.Context) error {
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
	var span *tracing.Span
	ctx, span = tracing.ChildSpan(ctx, fmt.Sprintf("%s.WriteFile", dest.Conf().Provider.String()))
	defer span.Finish()

	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	w, err := dest.Writer(ctx, basename)
	if err != nil {
		return errors.Wrap(err, "opening object for writing")
	}
	_, err = io.Copy(w, src)
	if err != nil {
		cancel()
		return errors.CombineErrors(w.Close(), err)
	}
	return errors.Wrap(w.Close(), "closing object")
}
