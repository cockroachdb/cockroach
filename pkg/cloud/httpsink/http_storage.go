// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package httpsink

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

func parseHTTPURL(uri *url.URL) (cloudpb.ExternalStorage, error) {
	conf := cloudpb.ExternalStorage{}
	conf.Provider = cloudpb.ExternalStorageProvider_http
	conf.HttpPath.BaseUri = uri.String()
	return conf, nil
}

type httpStorage struct {
	base     *url.URL
	client   *http.Client
	hosts    []string
	settings *cluster.Settings
	ioConf   base.ExternalIODirConfig
}

var _ cloud.ExternalStorage = &httpStorage{}

type retryableHTTPError struct {
	cause error
}

func (e *retryableHTTPError) Error() string {
	return fmt.Sprintf("retryable http error: %s", e.cause)
}

// MakeHTTPStorage returns an instance of HTTPStorage ExternalStorage.
func MakeHTTPStorage(
	ctx context.Context, args cloud.EarlyBootExternalStorageContext, dest cloudpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.http")
	if args.IOConf.DisableHTTP {
		return nil, errors.New("external http access disabled")
	}
	base := dest.HttpPath.BaseUri
	if base == "" {
		return nil, errors.Errorf("HTTP storage requested but prefix path not provided")
	}

	clientName := args.ExternalStorageOptions().ClientName
	client, err := cloud.MakeHTTPClient(args.Settings, args.MetricsRecorder, "http", base, clientName)
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
		settings: args.Settings,
		ioConf:   args.IOConf,
	}, nil
}

func (h *httpStorage) Conf() cloudpb.ExternalStorage {
	return cloudpb.ExternalStorage{
		Provider: cloudpb.ExternalStorageProvider_http,
		HttpPath: cloudpb.ExternalStorage_Http{
			BaseUri: h.base.String(),
		},
	}
}

func (h *httpStorage) ExternalIOConf() base.ExternalIODirConfig {
	return h.ioConf
}

func (h *httpStorage) RequiresExternalIOAccounting() bool { return true }

func (h *httpStorage) Settings() *cluster.Settings {
	return h.settings
}

func (h *httpStorage) openStreamAt(
	ctx context.Context, url string, pos int64,
) (*http.Response, error) {
	var headers map[string]string
	if pos > 0 {
		headers = map[string]string{"Range": fmt.Sprintf("bytes=%d-", pos)}
	}

	for attempt, retries := 0, retry.StartWithCtx(ctx, cloud.HTTPRetryOptions); retries.Next(); attempt++ {
		resp, err := h.req(ctx, "GET", url, nil, headers)
		if err == nil {
			return resp, err
		}

		log.Errorf(ctx, "HTTP:Req error: err=%s (attempt %d)", err, attempt)

		if !errors.HasType(err, (*retryableHTTPError)(nil)) {
			return nil, err
		}
	}
	if ctx.Err() == nil {
		return nil, errors.New("too many retries; giving up")
	}

	return nil, ctx.Err()
}

func (h *httpStorage) ReadFile(
	ctx context.Context, basename string, opts cloud.ReadOptions,
) (_ ioctx.ReadCloserCtx, fileSize int64, _ error) {
	stream, err := h.openStreamAt(ctx, basename, opts.Offset)
	if err != nil {
		return nil, 0, err
	}

	var size int64
	if opts.Offset == 0 {
		size = stream.ContentLength
	} else {
		size, err = cloud.CheckHTTPContentRangeHeader(stream.Header.Get("Content-Range"), opts.Offset)
		if err != nil {
			return nil, 0, err
		}
	}

	canResume := stream.Header.Get("Accept-Ranges") == "bytes"
	if canResume {
		opener := func(ctx context.Context, pos int64) (io.ReadCloser, int64, error) {
			s, err := h.openStreamAt(ctx, basename, pos)
			if err != nil {
				return nil, 0, err
			}
			return s.Body, size, err
		}
		return cloud.NewResumingReader(ctx, opener, stream.Body, opts.Offset, size, basename,
			cloud.ResumingReaderRetryOnErrFnForSettings(ctx, h.settings), nil), size, nil
	}
	return ioctx.ReadCloserAdapter(stream.Body), size, nil
}

func (h *httpStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	return cloud.BackgroundPipe(ctx, func(ctx context.Context, r io.Reader) error {
		_, err := h.reqNoBody(ctx, "PUT", basename, r)
		return err
	}), nil
}

func (h *httpStorage) List(_ context.Context, _, _ string, _ cloud.ListingFn) error {
	return errors.Mark(errors.New("http storage does not support listing"), cloud.ErrListingUnsupported)
}

func (h *httpStorage) Delete(ctx context.Context, basename string) error {
	return timeutil.RunWithTimeout(ctx, redact.Sprintf("DELETE %s", basename),
		cloud.Timeout.Get(&h.settings.SV), func(ctx context.Context) error {
			_, err := h.reqNoBody(ctx, "DELETE", basename, nil)
			if errors.Is(err, cloud.ErrFileDoesNotExist) {
				return nil
			}
			return err
		})
}

func (h *httpStorage) Size(ctx context.Context, basename string) (int64, error) {
	var resp *http.Response
	if err := timeutil.RunWithTimeout(ctx, redact.Sprintf("HEAD %s", basename),
		cloud.Timeout.Get(&h.settings.SV), func(ctx context.Context) error {
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

func isNotFoundErr(resp *http.Response) bool {
	return resp != nil && resp.StatusCode == http.StatusNotFound
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
	dest.Path = path.Join(dest.Path, file)
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
		return resp, nil
	default:
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		err := errors.Errorf("error response from server: %s %q", resp.Status, body)
		if isNotFoundErr(resp) {
			return nil, cloud.WrapErrFileDoesNotExist(err, "http storage file does not exist")
		}
		return nil, err
	}
}

func init() {
	cloud.RegisterExternalStorageProvider(cloudpb.ExternalStorageProvider_http,
		cloud.RegisteredProvider{
			EarlyBootParseFn:     parseHTTPURL,
			EarlyBootConstructFn: MakeHTTPStorage,
			RedactedParams:       cloud.RedactedParams(),
			Schemes:              []string{"http", "https"},
		})
}
