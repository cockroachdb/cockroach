// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloudimpl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

type httpStorage struct {
	base     *url.URL
	client   *http.Client
	hosts    []string
	settings *cluster.Settings
}

var _ cloud.ExternalStorage = &httpStorage{}

type retryableHTTPError struct {
	cause error
}

func (e *retryableHTTPError) Error() string {
	return fmt.Sprintf("retryable http error: %s", e.cause)
}

// HTTPRetryOptions defines the tunable settings which control the retry of HTTP
// operations.
var HTTPRetryOptions = retry.Options{
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     2 * time.Second,
	MaxRetries:     32,
	Multiplier:     4,
}

func makeHTTPClient(settings *cluster.Settings) (*http.Client, error) {
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

// MakeHTTPStorage returns an instance of HTTPStorage ExternalStorage.
func MakeHTTPStorage(base string, settings *cluster.Settings) (cloud.ExternalStorage, error) {
	if base == "" {
		return nil, errors.Errorf("HTTP storage requested but prefix path not provided")
	}

	client, err := makeHTTPClient(settings)
	if err != nil {
		return nil, err
	}
	uri, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	return &httpStorage{
		base:     uri,
		client:   client,
		hosts:    strings.Split(uri.Host, ","),
		settings: settings,
	}, nil
}

func (h *httpStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider: roachpb.ExternalStorageProvider_Http,
		HttpPath: roachpb.ExternalStorage_Http{
			BaseUri: h.base.String(),
		},
	}
}

type resumingHTTPReader struct {
	body      io.ReadCloser
	canResume bool  // Can we resume if download aborts prematurely?
	pos       int64 // How much data was received so far.
	ctx       context.Context
	url       string
	client    *httpStorage
}

var _ io.ReadCloser = &resumingHTTPReader{}

func newResumingHTTPReader(
	ctx context.Context, client *httpStorage, url string,
) (*resumingHTTPReader, error) {
	r := &resumingHTTPReader{
		ctx:    ctx,
		client: client,
		url:    url,
	}

	resp, err := r.sendRequest(nil)
	if err != nil {
		return nil, err
	}

	r.canResume = resp.Header.Get("Accept-Ranges") == "bytes"
	r.body = resp.Body
	return r, nil
}

func (r *resumingHTTPReader) Close() error {
	if r.body != nil {
		return r.body.Close()
	}
	return nil
}

// checkHTTPContentRangeHeader parses Content-Range header and
// ensures that range start offset is the same as 'pos'
// See https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Range
func checkHTTPContentRangeHeader(h string, pos int64) error {
	if len(h) == 0 {
		return errors.New("http server does not honor download resume")
	}

	h = strings.TrimPrefix(h, "bytes ")
	dash := strings.IndexByte(h, '-')
	if dash <= 0 {
		return errors.Errorf("malformed Content-Range header: %s", h)
	}

	resume, err := strconv.ParseInt(h[:dash], 10, 64)
	if err != nil {
		return errors.Errorf("malformed start offset in Content-Range header: %s", h)
	}

	if resume != pos {
		return errors.Errorf(
			"expected resume position %d, found %d instead in Content-Range header: %s",
			pos, resume, h)
	}
	return nil
}

func (r *resumingHTTPReader) sendRequest(
	reqHeaders map[string]string,
) (resp *http.Response, err error) {
	// Initialize err to the context.Canceled: if our context is canceled, we will
	// never enter the loop below; in this case we want to return "nil, canceled"
	err = context.Canceled
	for attempt, retries := 0,
		retry.StartWithCtx(r.ctx, HTTPRetryOptions); retries.Next(); attempt++ {
		resp, err = r.client.req(r.ctx, "GET", r.url, nil, reqHeaders)

		if err == nil {
			return
		}

		log.Errorf(r.ctx, "HTTP:Req error: err=%s (attempt %d)", err, attempt)

		if !errors.HasType(err, (*retryableHTTPError)(nil)) {
			return
		}
	}

	return
}

// requestNextRanges issues additional http request
// to continue downloading next range of bytes.
func (r *resumingHTTPReader) requestNextRange() (err error) {
	if err := r.body.Close(); err != nil {
		return err
	}

	r.body = nil
	var resp *http.Response
	resp, err = r.sendRequest(map[string]string{"Range": fmt.Sprintf("bytes=%d-", r.pos)})

	if err == nil {
		err = checkHTTPContentRangeHeader(resp.Header.Get("Content-Range"), r.pos)
	}

	if err == nil {
		r.body = resp.Body
	}
	return
}

// Read implements io.Reader interface to read the data from the underlying
// http stream, issuing additional requests in case download is interrupted.
func (r *resumingHTTPReader) Read(p []byte) (n int, err error) {
	for retries := 0; n == 0 && err == nil; retries++ {
		n, err = r.body.Read(p)
		r.pos += int64(n)

		if err != nil && !errors.IsAny(err, io.EOF, io.ErrUnexpectedEOF) {
			log.Errorf(r.ctx, "HTTP:Read err: %s", err)
		}

		// Resume download if the http server supports this.
		if r.canResume && isResumableHTTPError(err) {
			log.Errorf(r.ctx, "HTTP:Retry: error %s", err)
			if retries >= maxNoProgressReads {
				err = errors.Wrap(err, "multiple Read calls return no data")
				return
			}
			err = r.requestNextRange()
		}
	}

	return
}

func (h *httpStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	// https://github.com/cockroachdb/cockroach/issues/23859
	return newResumingHTTPReader(ctx, h, basename)
}

func (h *httpStorage) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {
	return contextutil.RunWithTimeout(ctx, fmt.Sprintf("PUT %s", basename),
		timeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
			_, err := h.reqNoBody(ctx, "PUT", basename, content)
			return err
		})
}

func (h *httpStorage) ListFiles(_ context.Context, _ string) ([]string, error) {
	return nil, errors.Mark(errors.New("http storage does not support listing"), ErrListingUnsupported)
}

func (h *httpStorage) Delete(ctx context.Context, basename string) error {
	return contextutil.RunWithTimeout(ctx, fmt.Sprintf("DELETE %s", basename),
		timeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
			_, err := h.reqNoBody(ctx, "DELETE", basename, nil)
			return err
		})
}

func (h *httpStorage) Size(ctx context.Context, basename string) (int64, error) {
	var resp *http.Response
	if err := contextutil.RunWithTimeout(ctx, fmt.Sprintf("HEAD %s", basename),
		timeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
			var err error
			resp, err = h.reqNoBody(ctx, "HEAD", basename, nil)
			return err
		}); err != nil {
		return 0, err
	}
	if resp.ContentLength < 0 {
		return 0, errors.Errorf("bad ContentLength: %d", resp.ContentLength)
	}
	return resp.ContentLength, nil
}

func (h *httpStorage) Close() error {
	return nil
}

// reqNoBody is like req but it closes the response body.
func (h *httpStorage) reqNoBody(
	ctx context.Context, method, file string, body io.Reader,
) (*http.Response, error) {
	resp, err := h.req(ctx, method, file, body, nil)
	if resp != nil {
		resp.Body.Close()
	}
	return resp, err
}

func (h *httpStorage) req(
	ctx context.Context, method, file string, body io.Reader, headers map[string]string,
) (*http.Response, error) {
	dest := *h.base
	if hosts := len(h.hosts); hosts > 1 {
		if file == "" {
			return nil, errors.New("cannot use a multi-host HTTP basepath for single file")
		}
		hash := fnv.New32a()
		if _, err := hash.Write([]byte(file)); err != nil {
			panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
		}
		dest.Host = h.hosts[int(hash.Sum32())%hosts]
	}
	dest.Path = filepath.Join(dest.Path, file)
	url := dest.String()
	req, err := http.NewRequest(method, url, body)

	if err != nil {
		return nil, errors.Wrapf(err, "error constructing request %s %q", method, url)
	}
	req = req.WithContext(ctx)

	for key, val := range headers {
		req.Header.Add(key, val)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		// We failed to establish connection to the server (we don't even have
		// a response object/server response code). Those errors (e.g. due to
		// network blip, or DNS resolution blip, etc) are usually transient. The
		// client may choose to retry the request few times before giving up.
		return nil, &retryableHTTPError{err}
	}

	switch resp.StatusCode {
	case 200, 201, 204, 206:
	// Pass.
	default:
		body, _ := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		err := errors.Errorf("error response from server: %s %q", resp.Status, body)
		if err != nil && resp.StatusCode == 404 {
			err = errors.Wrapf(ErrFileDoesNotExist, "http storage file does not exist: %s", err.Error())
		}
		return nil, err
	}
	return resp, nil
}
