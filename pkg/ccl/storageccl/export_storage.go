// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const (
	// S3AccessKeyParam is the query parameter for access_key in an S3 URI.
	S3AccessKeyParam = "AWS_ACCESS_KEY_ID"
	// S3SecretParam is the query parameter for the 'secret' in an S3 URI.
	S3SecretParam = "AWS_SECRET_ACCESS_KEY"
	// S3TempTokenParam is the query parameter for session_token in an S3 URI.
	S3TempTokenParam = "AWS_SESSION_TOKEN"
	// S3EndpointParam is the query parameter for the 'endpoint' in an S3 URI.
	S3EndpointParam = "AWS_ENDPOINT"
	// S3RegionParam is the query parameter for the 'endpoint' in an S3 URI.
	S3RegionParam = "AWS_REGION"

	// AzureAccountNameParam is the query parameter for account_name in an azure URI.
	AzureAccountNameParam = "AZURE_ACCOUNT_NAME"
	// AzureAccountKeyParam is the query parameter for account_key in an azure URI.
	AzureAccountKeyParam = "AZURE_ACCOUNT_KEY"

	// GoogleBillingProjectParam is the query parameter for the billing project
	// in a gs URI.
	GoogleBillingProjectParam = "GOOGLE_BILLING_PROJECT"

	// AuthParam is the query parameter for the cluster settings named
	// key in a URI.
	AuthParam          = "AUTH"
	authParamImplicit  = "implicit"
	authParamDefault   = "default"
	authParamSpecified = "specified"

	// CredentialsParam is the query parameter for the base64-encoded contents of
	// the Google Application Credentials JSON file.
	CredentialsParam = "CREDENTIALS"

	cloudstoragePrefix = "cloudstorage"
	cloudstorageGS     = cloudstoragePrefix + ".gs"
	cloudstorageHTTP   = cloudstoragePrefix + ".http"

	cloudstorageDefault = ".default"
	cloudstorageKey     = ".key"

	cloudstorageGSDefault    = cloudstorageGS + cloudstorageDefault
	cloudstorageGSDefaultKey = cloudstorageGSDefault + cloudstorageKey

	cloudstorageHTTPCASetting = cloudstorageHTTP + ".custom_ca"

	cloudStorageTimeout = cloudstoragePrefix + ".timeout"
)

// ExportStorageConfFromURI generates an ExportStorage config from a URI string.
func ExportStorageConfFromURI(path string) (roachpb.ExportStorage, error) {
	conf := roachpb.ExportStorage{}
	uri, err := url.Parse(path)
	if err != nil {
		return conf, err
	}
	switch uri.Scheme {
	case "s3":
		conf.Provider = roachpb.ExportStorageProvider_S3
		conf.S3Config = &roachpb.ExportStorage_S3{
			Bucket:    uri.Host,
			Prefix:    uri.Path,
			AccessKey: uri.Query().Get(S3AccessKeyParam),
			Secret:    uri.Query().Get(S3SecretParam),
			TempToken: uri.Query().Get(S3TempTokenParam),
			Endpoint:  uri.Query().Get(S3EndpointParam),
			Region:    uri.Query().Get(S3RegionParam),
			Auth:      uri.Query().Get(AuthParam),
		}
		conf.S3Config.Prefix = strings.TrimLeft(conf.S3Config.Prefix, "/")
		// AWS secrets often contain + characters, which must be escaped when
		// included in a query string; otherwise, they represent a space character.
		// More than a few users have been bitten by this.
		//
		// Luckily, AWS secrets are base64-encoded data and thus will never actually
		// contain spaces. We can convert any space characters we see to +
		// characters to recover the original secret.
		conf.S3Config.Secret = strings.Replace(conf.S3Config.Secret, " ", "+", -1)
	case "gs":
		conf.Provider = roachpb.ExportStorageProvider_GoogleCloud
		conf.GoogleCloudConfig = &roachpb.ExportStorage_GCS{
			Bucket:         uri.Host,
			Prefix:         uri.Path,
			Auth:           uri.Query().Get(AuthParam),
			BillingProject: uri.Query().Get(GoogleBillingProjectParam),
			Credentials:    uri.Query().Get(CredentialsParam),
		}
		conf.GoogleCloudConfig.Prefix = strings.TrimLeft(conf.GoogleCloudConfig.Prefix, "/")
	case "azure":
		conf.Provider = roachpb.ExportStorageProvider_Azure
		conf.AzureConfig = &roachpb.ExportStorage_Azure{
			Container:   uri.Host,
			Prefix:      uri.Path,
			AccountName: uri.Query().Get(AzureAccountNameParam),
			AccountKey:  uri.Query().Get(AzureAccountKeyParam),
		}
		if conf.AzureConfig.AccountName == "" {
			return conf, errors.Errorf("azure uri missing %q parameter", AzureAccountNameParam)
		}
		if conf.AzureConfig.AccountKey == "" {
			return conf, errors.Errorf("azure uri missing %q parameter", AzureAccountKeyParam)
		}
		conf.AzureConfig.Prefix = strings.TrimLeft(conf.AzureConfig.Prefix, "/")
	case "http", "https":
		conf.Provider = roachpb.ExportStorageProvider_Http
		conf.HttpPath.BaseUri = path
	case "nodelocal":
		nodeID, err := strconv.Atoi(uri.Host)
		if err != nil && uri.Host != "" {
			return conf, errors.Errorf("host component of nodelocal URI must be a node ID: %s", path)
		}
		conf.Provider = roachpb.ExportStorageProvider_LocalFile
		conf.LocalFile.Path = uri.Path
		conf.LocalFile.NodeID = roachpb.NodeID(nodeID)
	case "experimental-workload", "workload":
		conf.Provider = roachpb.ExportStorageProvider_Workload
		if conf.WorkloadConfig, err = ParseWorkloadConfig(uri); err != nil {
			return conf, err
		}
	default:
		return conf, errors.Errorf("unsupported storage scheme: %q", uri.Scheme)
	}
	return conf, nil
}

// ExportStorageFromURI returns an ExportStorage for the given URI.
func ExportStorageFromURI(
	ctx context.Context, uri string, settings *cluster.Settings,
) (ExportStorage, error) {
	conf, err := ExportStorageConfFromURI(uri)
	if err != nil {
		return nil, err
	}
	return MakeExportStorage(ctx, conf, settings)
}

// SanitizeExportStorageURI returns the export storage URI with sensitive
// credentials stripped.
func SanitizeExportStorageURI(path string) (string, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	if uri.Scheme == "experimental-workload" || uri.Scheme == "workload" {
		return path, nil
	}
	// All current export storage providers store credentials in the query string,
	// if they store it in the URI at all.
	uri.RawQuery = ""
	return uri.String(), nil
}

// MakeExportStorage creates an ExportStorage from the given config.
func MakeExportStorage(
	ctx context.Context, dest roachpb.ExportStorage, settings *cluster.Settings,
) (ExportStorage, error) {
	switch dest.Provider {
	case roachpb.ExportStorageProvider_LocalFile:
		telemetry.Count("external-io.nodelocal")
		return makeLocalStorage(dest.LocalFile, settings)
	case roachpb.ExportStorageProvider_Http:
		telemetry.Count("external-io.http")
		return makeHTTPStorage(dest.HttpPath.BaseUri, settings)
	case roachpb.ExportStorageProvider_S3:
		telemetry.Count("external-io.s3")
		return makeS3Storage(ctx, dest.S3Config, settings)
	case roachpb.ExportStorageProvider_GoogleCloud:
		telemetry.Count("external-io.google_cloud")
		return makeGCSStorage(ctx, dest.GoogleCloudConfig, settings)
	case roachpb.ExportStorageProvider_Azure:
		telemetry.Count("external-io.azure")
		return makeAzureStorage(dest.AzureConfig, settings)
	case roachpb.ExportStorageProvider_Workload:
		telemetry.Count("external-io.workload")
		return makeWorkloadStorage(dest.WorkloadConfig)
	}
	return nil, errors.Errorf("unsupported export destination type: %s", dest.Provider.String())
}

// ExportStorage provides functions to read and write files in some storage,
// namely various cloud storage providers, for example to store backups.
// Generally an implementation is instantiated pointing to some base path or
// prefix and then gets and puts files using the various methods to interact
// with individual files contained within that path or prefix.
// However, implementations must also allow callers to provide the full path to
// a given file as the "base" path, and then read or write it with the methods
// below by simply passing an empty filename. Implementations that use stdlib's
// `path.Join` to concatenate their base path with the provided filename will
// find its semantics well suited to this -- it elides empty components and does
// not append surplus slashes.
type ExportStorage interface {
	io.Closer

	// Conf should return the serializable configuration required to reconstruct
	// this ExportStorage implementation.
	Conf() roachpb.ExportStorage

	// ReadFile should return a Reader for requested name.
	ReadFile(ctx context.Context, basename string) (io.ReadCloser, error)

	// WriteFile should write the content to requested name.
	WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error

	// Delete removes the named file from the store.
	Delete(ctx context.Context, basename string) error

	// Size returns the length of the named file in bytes.
	Size(ctx context.Context, basename string) (int64, error)
}

var (
	gcsDefault = settings.RegisterStringSetting(
		cloudstorageGSDefaultKey,
		"if set, JSON key to use during Google Cloud Storage operations",
		"",
	)
	httpCustomCA = settings.RegisterStringSetting(
		cloudstorageHTTPCASetting,
		"custom root CA (appended to system's default CAs) for verifying certificates when interacting with HTTPS storage",
		"",
	)
	timeoutSetting = settings.RegisterDurationSetting(
		cloudStorageTimeout,
		"the timeout for import/export storage operations",
		10*time.Minute)
)

type localFileStorage struct {
	cfg  roachpb.ExportStorage_LocalFilePath // constains un-prefixed base -- DO NOT use for I/O ops.
	base string                              // the prefixed base, for I/O ops on this node.
}

var _ ExportStorage = &localFileStorage{}

// MakeLocalStorageURI converts a local path (absolute or relative) to a
// valid nodelocal URI.
func MakeLocalStorageURI(path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("nodelocal://%s", path), nil
}

func makeLocalStorage(
	cfg roachpb.ExportStorage_LocalFilePath, settings *cluster.Settings,
) (ExportStorage, error) {
	if cfg.Path == "" {
		return nil, errors.Errorf("Local storage requested but path not provided")
	}
	// TODO(dt): check that this node is cfg.NodeID if non-zero.

	localBase := cfg.Path
	// In non-server execution we have no settings and no restriction on local IO.
	if settings != nil {
		if settings.ExternalIODir == "" {
			return nil, errors.Errorf("local file access is disabled")
		}
		// we prefix with the IO dir
		localBase = filepath.Clean(filepath.Join(settings.ExternalIODir, localBase))
		// ... and make sure we didn't ../ our way back out.
		if !strings.HasPrefix(localBase, settings.ExternalIODir) {
			return nil, errors.Errorf("local file access to paths outside of external-io-dir is not allowed")
		}
	}
	return &localFileStorage{base: localBase, cfg: cfg}, nil
}

func (l *localFileStorage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider:  roachpb.ExportStorageProvider_LocalFile,
		LocalFile: l.cfg,
	}
}

func (l *localFileStorage) WriteFile(
	_ context.Context, basename string, content io.ReadSeeker,
) error {
	p := filepath.Join(l.base, basename)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return errors.Wrap(err, "creating local export storage path")
	}
	tmpP := p + `.tmp`
	f, err := os.Create(tmpP)
	if err != nil {
		return errors.Wrapf(err, "creating local export tmp file %q", tmpP)
	}
	defer func() {
		f.Close()
		if err == nil {
			err = errors.Wrapf(os.Rename(tmpP, p), "renaming to local export file %q", p)
		}
	}()
	_, err = io.Copy(f, content)
	if err != nil {
		return errors.Wrapf(err, "writing to local export tmp file %q", tmpP)
	}
	if err := f.Sync(); err != nil {
		return errors.Wrapf(err, "syncing to local export tmp file %q", tmpP)
	}
	return err
}

func (l *localFileStorage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(l.base, basename))
}

func (l *localFileStorage) Delete(_ context.Context, basename string) error {
	return os.Remove(filepath.Join(l.base, basename))
}

func (l *localFileStorage) Size(_ context.Context, basename string) (int64, error) {
	fi, err := os.Stat(filepath.Join(l.base, basename))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (*localFileStorage) Close() error {
	return nil
}

type httpStorage struct {
	base     *url.URL
	client   *http.Client
	hosts    []string
	settings *cluster.Settings
}

var _ ExportStorage = &httpStorage{}

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

func makeHTTPStorage(base string, settings *cluster.Settings) (ExportStorage, error) {
	if base == "" {
		return nil, errors.Errorf("HTTP storage requested but base path not provided")
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

func (h *httpStorage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider: roachpb.ExportStorageProvider_Http,
		HttpPath: roachpb.ExportStorage_Http{
			BaseUri: h.base.String(),
		},
	}
}

type reqSender = func(reqHeaders map[string]string) (io.ReadCloser, http.Header, error)

type resumingHTTPReader struct {
	b         io.ReadCloser
	canResume bool      // Can we resume if download aborts prematurely?
	pos       int64     // How much data was received so far.
	retryReq  reqSender // Retry request
}

var _ io.ReadCloser = &resumingHTTPReader{}

const maxNoProgressHTTPRetries = 16

func (r *resumingHTTPReader) Close() error {
	return r.b.Close()
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

// requestNextRanges issues additional http request
// to continue downloading next range of bytes.
func (r *resumingHTTPReader) requestNextRange() (err error) {
	if err := r.b.Close(); err != nil {
		return err
	}

	var header http.Header
	r.b, header, err = r.retryReq(map[string]string{"Range": fmt.Sprintf("bytes=%d-", r.pos)})

	if err != nil {
		return err
	}

	return checkHTTPContentRangeHeader(header.Get("Content-Range"), r.pos)
}

// Read implements io.Reader interface to read the data from the underlying
// http stream, issuing additional requests in case download is interrupted.
func (r *resumingHTTPReader) Read(p []byte) (n int, err error) {
	retries := 0

	for n == 0 && err == nil && retries < maxNoProgressHTTPRetries {
		n, err = r.b.Read(p)
		r.pos += int64(n)

		if n > 0 {
			// As long as download progresses, reset the retries.
			retries = 0
		}

		// Resume download if the http server supports this.
		// We can attempt to resume download iff the error is ErrUnexpectedEOF.
		// In particular, we should not worry about a case when error is io.EOF.
		// The reason for this is two-fold:
		//   1. The underlying http library converts io.EOF to io.ErrUnexpectedEOF
		//   if the number of bytes transferred is less than the number of
		//   bytes advertised in the Content-Length header.  So if we see
		//   io.ErrUnexpectedEOF we can simply request the next range.
		//   2. If the server did *not* advertise Content-Length, then
		//   there is really nothing we can do: http standard says that
		//   the stream ends when the server terminates connection.
		if errors.Is(err, io.ErrUnexpectedEOF) && r.canResume {
			retries++
			err = r.requestNextRange()
		}
	}
	return
}

func (h *httpStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	// https://github.com/cockroachdb/cockroach/issues/23859
	sender := func(reqHeaders map[string]string) (io.ReadCloser, http.Header, error) {
		resp, err := h.req(ctx, "GET", basename, nil, reqHeaders)
		if err != nil {
			return nil, nil, err
		}
		return resp.Body, resp.Header, nil
	}

	body, headers, err := sender(nil)
	if err != nil {
		return nil, err
	}
	return &resumingHTTPReader{
		b:         body,
		canResume: headers.Get("Accept-Ranges") == "bytes",
		pos:       0,
		retryReq:  sender,
	}, nil
}

func (h *httpStorage) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {
	return contextutil.RunWithTimeout(ctx, fmt.Sprintf("PUT %s", basename),
		timeoutSetting.Get(&h.settings.SV), func(ctx context.Context) error {
			_, err := h.reqNoBody(ctx, "PUT", basename, content)
			return err
		})
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
	dest.Path = path.Join(dest.Path, file)
	url := dest.String()
	req, err := http.NewRequest(method, url, body)
	req = req.WithContext(ctx)

	for key, val := range headers {
		req.Header.Add(key, val)
	}

	if err != nil {
		return nil, errors.Wrapf(err, "error constructing request %s %q", method, url)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "error executing request %s %q", method, url)
	}
	switch resp.StatusCode {
	case 200, 201, 204, 206:
		// Pass.
	default:
		body, _ := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, errors.Errorf("error response from server: %s %q", resp.Status, body)
	}
	return resp, nil
}

type s3Storage struct {
	bucket   *string
	conf     *roachpb.ExportStorage_S3
	prefix   string
	s3       *s3.S3
	settings *cluster.Settings
}

// delayedRetry runs fn and re-runs it a limited number of times if it
// fails. It knows about specific kinds of errors that need longer retry
// delays than normal.
func delayedRetry(ctx context.Context, fn func() error) error {
	const maxAttempts = 3
	return retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		err := fn()
		if err == nil {
			return nil
		}
		if s3err, ok := err.(s3.RequestFailure); ok {
			// A 503 error could mean we need to reduce our request rate. Impose an
			// arbitrary slowdown in that case.
			// See http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
			if s3err.StatusCode() == 503 {
				select {
				case <-time.After(time.Second * 5):
				case <-ctx.Done():
				}
			}
		}
		// See https:github.com/GoogleCloudPlatform/google-cloud-go/issues/1012#issuecomment-393606797
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

var _ ExportStorage = &s3Storage{}

func makeS3Storage(
	ctx context.Context, conf *roachpb.ExportStorage_S3, settings *cluster.Settings,
) (ExportStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("s3 upload requested but info missing")
	}
	region := conf.Region
	config := conf.Keys()
	if conf.Endpoint != "" {
		config.Endpoint = &conf.Endpoint
		if conf.Region == "" {
			region = "default-region"
		}
		client, err := makeHTTPClient(settings)
		if err != nil {
			return nil, err
		}
		config.HTTPClient = client
	}

	// "specified": use credentials provided in URI params; error if not present.
	// "implicit": enable SharedConfig, which loads in credentials from environment.
	//             Detailed in https://docs.aws.amazon.com/sdk-for-go/api/aws/session/
	// "": default to `specified`.
	opts := session.Options{}
	switch conf.Auth {
	case "", authParamSpecified:
		if conf.AccessKey == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				AuthParam,
				authParamSpecified,
				S3AccessKeyParam,
			)
		}
		if conf.Secret == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				AuthParam,
				authParamSpecified,
				S3SecretParam,
			)
		}
		opts.Config.MergeIn(config)
	case authParamImplicit:
		opts.SharedConfigState = session.SharedConfigEnable
	default:
		return nil, errors.Errorf("unsupported value %s for %s", conf.Auth, AuthParam)
	}

	sess, err := session.NewSessionWithOptions(opts)
	if err != nil {
		return nil, errors.Wrap(err, "new aws session")
	}
	if region == "" {
		err = delayedRetry(ctx, func() error {
			var err error
			region, err = s3manager.GetBucketRegion(ctx, sess, conf.Bucket, "us-east-1")
			return err
		})
		if err != nil {
			return nil, errors.Wrap(err, "could not find s3 bucket's region")
		}
	}
	sess.Config.Region = aws.String(region)
	if conf.Endpoint != "" {
		sess.Config.S3ForcePathStyle = aws.Bool(true)
	}
	return &s3Storage{
		bucket:   aws.String(conf.Bucket),
		conf:     conf,
		prefix:   conf.Prefix,
		s3:       s3.New(sess),
		settings: settings,
	}, nil
}

func (s *s3Storage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider: roachpb.ExportStorageProvider_S3,
		S3Config: s.conf,
	}
}

func (s *s3Storage) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {
	err := contextutil.RunWithTimeout(ctx, "put s3 object",
		timeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			_, err := s.s3.PutObjectWithContext(ctx, &s3.PutObjectInput{
				Bucket: s.bucket,
				Key:    aws.String(path.Join(s.prefix, basename)),
				Body:   content,
			})
			return err
		})
	return errors.Wrap(err, "failed to put s3 object")
}

func (s *s3Storage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	// https://github.com/cockroachdb/cockroach/issues/23859
	out, err := s.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: s.bucket,
		Key:    aws.String(path.Join(s.prefix, basename)),
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get s3 object")
	}
	return out.Body, nil
}

func (s *s3Storage) Delete(ctx context.Context, basename string) error {
	return contextutil.RunWithTimeout(ctx, "delete s3 object",
		timeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			_, err := s.s3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
				Bucket: s.bucket,
				Key:    aws.String(path.Join(s.prefix, basename)),
			})
			return err
		})
}

func (s *s3Storage) Size(ctx context.Context, basename string) (int64, error) {
	var out *s3.HeadObjectOutput
	err := contextutil.RunWithTimeout(ctx, "get s3 object header",
		timeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			var err error
			out, err = s.s3.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
				Bucket: s.bucket,
				Key:    aws.String(path.Join(s.prefix, basename)),
			})
			return err
		})
	if err != nil {
		return 0, errors.Wrap(err, "failed to get s3 object headers")
	}
	return *out.ContentLength, nil
}

func (s *s3Storage) Close() error {
	return nil
}

type gcsStorage struct {
	bucket   *gcs.BucketHandle
	client   *gcs.Client
	conf     *roachpb.ExportStorage_GCS
	prefix   string
	settings *cluster.Settings
}

var _ ExportStorage = &gcsStorage{}

func (g *gcsStorage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider:          roachpb.ExportStorageProvider_GoogleCloud,
		GoogleCloudConfig: g.conf,
	}
}

func makeGCSStorage(
	ctx context.Context, conf *roachpb.ExportStorage_GCS, settings *cluster.Settings,
) (ExportStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("google cloud storage upload requested but info missing")
	}
	const scope = gcs.ScopeReadWrite
	opts := []option.ClientOption{option.WithScopes(scope)}

	// "default": only use the key in the settings; error if not present.
	// "specified": the JSON object for authentication is given by the CREDENTIALS param.
	// "implicit": only use the environment data.
	// "": if default key is in the settings use it; otherwise use environment data.
	switch conf.Auth {
	case "", authParamDefault:
		var key string
		if settings != nil {
			key = gcsDefault.Get(&settings.SV)
		}
		// We expect a key to be present if default is specified.
		if conf.Auth == authParamDefault && key == "" {
			return nil, errors.Errorf("expected settings value for %s", cloudstorageGSDefaultKey)
		}
		if key != "" {
			source, err := google.JWTConfigFromJSON([]byte(key), scope)
			if err != nil {
				return nil, errors.Wrap(err, "creating GCS oauth token source")
			}
			opts = append(opts, option.WithTokenSource(source.TokenSource(ctx)))
		}
	case authParamSpecified:
		if conf.Credentials == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				AuthParam,
				authParamSpecified,
				CredentialsParam,
			)
		}
		decodedKey, err := base64.StdEncoding.DecodeString(conf.Credentials)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("decoding value of %s", CredentialsParam))
		}
		source, err := google.JWTConfigFromJSON(decodedKey, scope)
		if err != nil {
			return nil, errors.Wrap(err, "creating GCS oauth token source from specified credentials")
		}
		opts = append(opts, option.WithTokenSource(source.TokenSource(ctx)))
	case authParamImplicit:
		// Do nothing; use implicit params:
		// https://godoc.org/golang.org/x/oauth2/google#FindDefaultCredentials
	default:
		return nil, errors.Errorf("unsupported value %s for %s", conf.Auth, AuthParam)
	}
	g, err := gcs.NewClient(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create google cloud client")
	}
	bucket := g.Bucket(conf.Bucket)
	if conf.BillingProject != `` {
		bucket = bucket.UserProject(conf.BillingProject)
	}
	return &gcsStorage{
		bucket:   bucket,
		client:   g,
		conf:     conf,
		prefix:   conf.Prefix,
		settings: settings,
	}, nil
}

func (g *gcsStorage) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {
	const maxAttempts = 3
	err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		if _, err := content.Seek(0, io.SeekStart); err != nil {
			return err
		}
		// Set the timeout within the retry loop.
		return contextutil.RunWithTimeout(ctx, "put gcs file", timeoutSetting.Get(&g.settings.SV),
			func(ctx context.Context) error {
				w := g.bucket.Object(path.Join(g.prefix, basename)).NewWriter(ctx)
				if _, err := io.Copy(w, content); err != nil {
					_ = w.Close()
					return err
				}
				return w.Close()
			})
	})
	return errors.Wrap(err, "write to google cloud")
}

func (g *gcsStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	// https://github.com/cockroachdb/cockroach/issues/23859

	var rc io.ReadCloser
	err := delayedRetry(ctx, func() error {
		var readErr error
		rc, readErr = g.bucket.Object(path.Join(g.prefix, basename)).NewReader(ctx)
		return readErr
	})
	return rc, err
}

func (g *gcsStorage) Delete(ctx context.Context, basename string) error {
	return contextutil.RunWithTimeout(ctx, "delete gcs file",
		timeoutSetting.Get(&g.settings.SV),
		func(ctx context.Context) error {
			return g.bucket.Object(path.Join(g.prefix, basename)).Delete(ctx)
		})
}

func (g *gcsStorage) Size(ctx context.Context, basename string) (int64, error) {
	var r *gcs.Reader
	if err := contextutil.RunWithTimeout(ctx, "size gcs file",
		timeoutSetting.Get(&g.settings.SV),
		func(ctx context.Context) error {
			var err error
			r, err = g.bucket.Object(path.Join(g.prefix, basename)).NewReader(ctx)
			return err
		}); err != nil {
		return 0, err
	}
	sz := r.Attrs.Size
	_ = r.Close()
	return sz, nil
}

func (g *gcsStorage) Close() error {
	return g.client.Close()
}

type azureStorage struct {
	conf      *roachpb.ExportStorage_Azure
	container azblob.ContainerURL
	prefix    string
	settings  *cluster.Settings
}

var _ ExportStorage = &azureStorage{}

func makeAzureStorage(
	conf *roachpb.ExportStorage_Azure, settings *cluster.Settings,
) (ExportStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("azure upload requested but info missing")
	}
	credential, err := azblob.NewSharedKeyCredential(conf.AccountName, conf.AccountKey)
	if err != nil {
		return nil, errors.Wrap(err, "azure credential")
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	u, err := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net", conf.AccountName))
	if err != nil {
		return nil, errors.Wrap(err, "azure: account name is not valid")
	}
	serviceURL := azblob.NewServiceURL(*u, p)
	return &azureStorage{
		conf:      conf,
		container: serviceURL.NewContainerURL(conf.Container),
		prefix:    conf.Prefix,
		settings:  settings,
	}, nil
}

func (s *azureStorage) getBlob(basename string) azblob.BlockBlobURL {
	name := path.Join(s.prefix, basename)
	return s.container.NewBlockBlobURL(name)
}

func (s *azureStorage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider:    roachpb.ExportStorageProvider_Azure,
		AzureConfig: s.conf,
	}
}

func (s *azureStorage) WriteFile(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	err := contextutil.RunWithTimeout(ctx, "write azure file", timeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			blob := s.getBlob(basename)
			_, err := blob.Upload(ctx, content, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
			return err
		})
	return errors.Wrapf(err, "write file: %s", basename)
}

func (s *azureStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	// https://github.com/cockroachdb/cockroach/issues/23859
	blob := s.getBlob(basename)
	get, err := blob.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create azure reader")
	}
	reader := get.Body(azblob.RetryReaderOptions{MaxRetryRequests: 3})
	return reader, nil
}

func (s *azureStorage) Delete(ctx context.Context, basename string) error {
	err := contextutil.RunWithTimeout(ctx, "delete azure file", timeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			blob := s.getBlob(basename)
			_, err := blob.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
			return err
		})
	return errors.Wrap(err, "delete file")
}

func (s *azureStorage) Size(ctx context.Context, basename string) (int64, error) {
	var props *azblob.BlobGetPropertiesResponse
	err := contextutil.RunWithTimeout(ctx, "size azure file", timeoutSetting.Get(&s.settings.SV),
		func(ctx context.Context) error {
			blob := s.getBlob(basename)
			var err error
			props, err = blob.GetProperties(ctx, azblob.BlobAccessConditions{})
			return err
		})
	if err != nil {
		return 0, errors.Wrap(err, "get file properties")
	}
	return props.ContentLength(), nil
}

func (s *azureStorage) Close() error {
	return nil
}

// ParseWorkloadConfig parses a workload config URI to a proto config.
func ParseWorkloadConfig(uri *url.URL) (*roachpb.ExportStorage_Workload, error) {
	c := &roachpb.ExportStorage_Workload{}
	pathParts := strings.Split(strings.Trim(uri.Path, `/`), `/`)
	if len(pathParts) != 3 {
		return nil, errors.Errorf(
			`path must be of the form /<format>/<generator>/<table>: %s`, uri.Path)
	}
	c.Format, c.Generator, c.Table = pathParts[0], pathParts[1], pathParts[2]
	q := uri.Query()
	if _, ok := q[`version`]; !ok {
		return nil, errors.New(`parameter version is required`)
	}
	c.Version = q.Get(`version`)
	q.Del(`version`)
	if s := q.Get(`row-start`); len(s) > 0 {
		q.Del(`row-start`)
		var err error
		if c.BatchBegin, err = strconv.ParseInt(s, 10, 64); err != nil {
			return nil, err
		}
	}
	if e := q.Get(`row-end`); len(e) > 0 {
		q.Del(`row-end`)
		var err error
		if c.BatchEnd, err = strconv.ParseInt(e, 10, 64); err != nil {
			return nil, err
		}
	}
	for k, vs := range q {
		for _, v := range vs {
			c.Flags = append(c.Flags, `--`+k+`=`+v)
		}
	}
	return c, nil
}

type workloadStorage struct {
	conf  *roachpb.ExportStorage_Workload
	gen   workload.Generator
	table workload.Table
}

var _ ExportStorage = &workloadStorage{}

func makeWorkloadStorage(conf *roachpb.ExportStorage_Workload) (ExportStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("workload upload requested but info missing")
	}
	if strings.ToLower(conf.Format) != `csv` {
		return nil, errors.Errorf(`unsupported format: %s`, conf.Format)
	}
	meta, err := workload.Get(conf.Generator)
	if err != nil {
		return nil, err
	}
	// Different versions of the workload could generate different data, so
	// disallow this.
	if meta.Version != conf.Version {
		return nil, errors.Errorf(
			`expected %s version "%s" but got "%s"`, meta.Name, conf.Version, meta.Version)
	}
	gen := meta.New()
	if f, ok := gen.(workload.Flagser); ok {
		if err := f.Flags().Parse(conf.Flags); err != nil {
			return nil, errors.Wrapf(err, `parsing parameters %s`, strings.Join(conf.Flags, ` `))
		}
	}
	s := &workloadStorage{
		conf: conf,
		gen:  gen,
	}
	for _, t := range gen.Tables() {
		if t.Name == conf.Table {
			s.table = t
			break
		}
	}
	if s.table.Name == `` {
		return nil, errors.Wrapf(err, `unknown table %s for generator %s`, conf.Table, meta.Name)
	}
	return s, nil
}

func (s *workloadStorage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider:       roachpb.ExportStorageProvider_Workload,
		WorkloadConfig: s.conf,
	}
}

func (s *workloadStorage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	if basename != `` {
		return nil, errors.New(`basenames are not supported by workload storage`)
	}
	r := workload.NewCSVRowsReader(s.table, int(s.conf.BatchBegin), int(s.conf.BatchEnd))
	return ioutil.NopCloser(r), nil
}

func (s *workloadStorage) WriteFile(_ context.Context, _ string, _ io.ReadSeeker) error {
	return errors.Errorf(`workload storage does not support writes`)
}
func (s *workloadStorage) Delete(_ context.Context, _ string) error {
	return errors.Errorf(`workload storage does not support writes`)
}
func (s *workloadStorage) Size(_ context.Context, _ string) (int64, error) {
	return 0, errors.Errorf(`workload storage does not support sizing`)
}
func (s *workloadStorage) Close() error {
	return nil
}
