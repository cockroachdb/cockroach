// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/ccl/LICENSE

package storageccl

import (
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	gcs "cloud.google.com/go/storage"
	azr "github.com/Azure/azure-sdk-for-go/storage"
	"github.com/pkg/errors"
	"github.com/rlmcpherson/s3gof3r"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

const (
	// S3AccessKeyParam is the query parameter for access_key in an S3 URI.
	S3AccessKeyParam = "AWS_ACCESS_KEY_ID"
	// S3SecretParam is the query parameter for the 'secret' in an S3 URI.
	S3SecretParam = "AWS_SECRET_ACCESS_KEY"

	// AzureAccountNameParam is the query parameter for account_name in an azure URI.
	AzureAccountNameParam = "AZURE_ACCOUNT_NAME"
	// AzureAccountKeyParam is the query parameter for account_key in an azure URI.
	AzureAccountKeyParam = "AZURE_ACCOUNT_KEY"
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
			AccessKey: getParamOrEnv(uri, S3AccessKeyParam),
			Secret:    getParamOrEnv(uri, S3SecretParam),
		}
		if conf.S3Config.AccessKey == "" {
			return conf, errors.Errorf("s3 uri missing %q parameter", S3AccessKeyParam)
		}
		if conf.S3Config.Secret == "" {
			return conf, errors.Errorf("s3 uri missing %q parameter", S3SecretParam)
		}
		conf.S3Config.Prefix = strings.TrimLeft(conf.S3Config.Prefix, "/")
	case "gs":
		conf.Provider = roachpb.ExportStorageProvider_GoogleCloud
		conf.GoogleCloudConfig = &roachpb.ExportStorage_GCS{
			Bucket: uri.Host,
			Prefix: uri.Path,
		}
		conf.GoogleCloudConfig.Prefix = strings.TrimLeft(conf.GoogleCloudConfig.Prefix, "/")
	case "azure":
		conf.Provider = roachpb.ExportStorageProvider_Azure
		conf.AzureConfig = &roachpb.ExportStorage_Azure{
			Container:   uri.Host,
			Prefix:      uri.Path,
			AccountName: getParamOrEnv(uri, AzureAccountNameParam),
			AccountKey:  getParamOrEnv(uri, AzureAccountKeyParam),
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
	case "", "file":
		conf.Provider = roachpb.ExportStorageProvider_LocalFile
		conf.LocalFile.Path = uri.Path
	default:
		return conf, errors.Errorf("unsupported storage scheme: %q", uri.Scheme)
	}
	return conf, nil
}

func getParamOrEnv(u *url.URL, name string) string {
	s := u.Query().Get(name)
	if s != "" {
		return s
	}
	return envutil.EnvOrDefaultString(name, "")
}

// ExportStorageFromURI returns an ExportStorage for the given URI.
func ExportStorageFromURI(ctx context.Context, uri string) (ExportStorage, error) {
	conf, err := ExportStorageConfFromURI(uri)
	if err != nil {
		return nil, err
	}
	return MakeExportStorage(ctx, conf)
}

// MakeExportStorage creates an ExportStorage from the given config.
func MakeExportStorage(ctx context.Context, dest roachpb.ExportStorage) (ExportStorage, error) {
	switch dest.Provider {
	case roachpb.ExportStorageProvider_LocalFile:
		return makeLocalStorage(dest.LocalFile.Path)
	case roachpb.ExportStorageProvider_Http:
		return makeHTTPStorage(dest.HttpPath.BaseUri)
	case roachpb.ExportStorageProvider_S3:
		return makeS3Storage(dest.S3Config)
	case roachpb.ExportStorageProvider_GoogleCloud:
		return makeGCSStorage(ctx, dest.GoogleCloudConfig)
	case roachpb.ExportStorageProvider_Azure:
		return makeAzureStorage(dest.AzureConfig)
	}
	return nil, errors.Errorf("unsupported export destination type: %s", dest.Provider.String())
}

// ExportStorage handles reading and writing files in an export.
type ExportStorage interface {
	io.Closer
	// Conf should return the serializable configuration required to reconstruct
	// this ExportStorage implementation.
	Conf() roachpb.ExportStorage
	// PutFile is used to prepare to write a file to the storage.
	// See ExportFileWriter.
	PutFile(ctx context.Context, basename string) (ExportFileWriter, error)
	// ReadFile should return a Reader for requested name.
	ReadFile(ctx context.Context, basename string) (io.ReadCloser, error)
	// FetchFile returns the path to a local file containing the content of
	// the requested filename. Implementations may wish to use the `fetchFile`
	// helper for copying the content of ReadFile to a temporary file.
	FetchFile(ctx context.Context, basename string) (string, error)
	// Delete removes the named file from the store.
	Delete(ctx context.Context, basename string) error
}

// ExportFileWriter provides a local file or pipe that can be written to before
// calling Finish() to store the content of said file. As some writers may be
// non-Go (e.g. the RocksDB SSTable Writer), this is intentionally the path to a
// file, not an io.Writer, and the required call to `Finish` gives
// implementations the opportunity to move/copy/upload/etc as needed.
type ExportFileWriter interface {
	// LocalFile returns the path to a local path to which a caller should write.
	LocalFile() string
	// Finish indicates that no further writes to the local file are expected and
	// that the implementation should store the content (copy it, upload, etc) if
	// that has not already been done in a streaming fashion (e.g. via a pipe).
	Finish() error
	// Cleanup removes any temporary files or resources that were made on behalf
	// of this writer. If `Finish` has not been called, any writes to
	// `LocalFile` will be lost. Implementations of `Cleanup` are required to be
	// idempotent and should log any errors.
	Cleanup()
}

type localFileStorage struct {
	base string
}

var _ ExportStorage = &localFileStorage{}

func makeLocalStorage(base string) (ExportStorage, error) {
	if base == "" {
		return nil, errors.Errorf("Local storage requested but path not provided")
	}
	return &localFileStorage{base: base}, nil
}

func (l *localFileStorage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider: roachpb.ExportStorageProvider_LocalFile,
		LocalFile: roachpb.ExportStorage_LocalFilePath{
			Path: l.base,
		},
	}
}

func (l *localFileStorage) PutFile(_ context.Context, basename string) (ExportFileWriter, error) {
	if err := os.MkdirAll(l.base, 0777); err != nil {
		return nil, errors.Wrapf(err, "failed to create %q", l.base)
	}
	return localFileStorageWriter{path: filepath.Join(l.base, basename)}, nil
}

func (l *localFileStorage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(l.base, basename))
}

func (l *localFileStorage) FetchFile(_ context.Context, basename string) (string, error) {
	return filepath.Join(l.base, basename), nil
}

func (l *localFileStorage) Delete(_ context.Context, basename string) error {
	return os.Remove(filepath.Join(l.base, basename))
}

func (*localFileStorage) Close() error {
	return nil
}

type localFileStorageWriter struct {
	path string
}

var _ ExportFileWriter = localFileStorageWriter{}

func (l localFileStorageWriter) LocalFile() string {
	return fmt.Sprintf("%s.tmp", l.path)
}

func (l localFileStorageWriter) Finish() error {
	return os.Rename(l.LocalFile(), l.path)
}

func (l localFileStorageWriter) Cleanup() {
	if err := os.RemoveAll(l.LocalFile()); err != nil {
		log.Warningf(context.TODO(), "could not remove %q: %+v", l.LocalFile(), err)
	}
}

type httpStorage struct {
	client *http.Client
	base   string
	tmp    tmpHelper
}

var _ ExportStorage = &httpStorage{}

func makeHTTPStorage(base string) (ExportStorage, error) {
	if base == "" {
		return nil, errors.Errorf("HTTP storage requested but base path not provided")
	}
	if base[len(base)-1] != '/' {
		return nil, errors.Errorf("HTTP storage path must end in '/'")
	}
	client := &http.Client{Transport: &http.Transport{}}
	return &httpStorage{client: client, base: base}, nil
}

func (h *httpStorage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider: roachpb.ExportStorageProvider_Http,
		HttpPath: roachpb.ExportStorage_Http{
			BaseUri: h.base,
		},
	}
}

func (h *httpStorage) PutFile(_ context.Context, basename string) (ExportFileWriter, error) {
	local, err := h.tmp.tmpFile(basename)
	if err != nil {
		return nil, err
	}
	url := h.base + basename
	return httpStorageWriter{client: h.client, local: local, url: url}, nil
}

func (h *httpStorage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	return runHTTPRequest(h.client, "GET", h.base+basename, nil)
}

func (h *httpStorage) FetchFile(ctx context.Context, basename string) (string, error) {
	body, err := h.ReadFile(ctx, basename)
	if err != nil {
		return "", err
	}
	return fetchFile(body, &h.tmp, basename)
}

func (h *httpStorage) Delete(_ context.Context, basename string) error {
	_, err := runHTTPRequest(h.client, "DELETE", h.base+basename, nil)
	return err
}

func (h *httpStorage) Close() error {
	return h.tmp.Close()
}

type httpStorageWriter struct {
	client *http.Client
	local  string
	url    string
}

var _ ExportFileWriter = httpStorageWriter{}

func (l httpStorageWriter) LocalFile() string {
	return l.local
}

func (l httpStorageWriter) Finish() error {
	f, err := os.Open(l.local)
	if err != nil {
		return err
	}
	_, err = runHTTPRequest(l.client, "PUT", l.url, f)
	return err
}

func runHTTPRequest(c *http.Client, method, url string, body io.Reader) (io.ReadCloser, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, errors.Wrapf(err, "error constructing request %s %q", method, url)
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "error exeucting request %s %q", method, url)
	}
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, errors.Errorf("error response from server: %s %q", resp.Status, body)
	}
	return resp.Body, nil
}

func (l httpStorageWriter) Cleanup() {
	return // No-op.
}

type s3Storage struct {
	conf   *roachpb.ExportStorage_S3
	bucket *s3gof3r.Bucket
	prefix string
	tmp    tmpHelper
}

var _ ExportStorage = &s3Storage{}

type s3StorageWriter struct {
	w     io.WriteCloser
	local string
}

var _ ExportFileWriter = &s3StorageWriter{}

func makeS3Storage(conf *roachpb.ExportStorage_S3) (ExportStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("s3 upload requested but info missing")
	}
	s3 := s3gof3r.New("", conf.Keys())
	b := s3.Bucket(conf.Bucket)
	return &s3Storage{
		conf:   conf,
		bucket: b,
		prefix: conf.Prefix,
	}, nil
}

func (s *s3Storage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider: roachpb.ExportStorageProvider_S3,
		S3Config: s.conf,
	}
}

func (s *s3Storage) PutFile(_ context.Context, basename string) (ExportFileWriter, error) {
	local, err := s.tmp.tmpFile(basename)
	if err != nil {
		return nil, err
	}
	w, err := s.bucket.PutWriter(filepath.Join(s.prefix, basename), nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create s3 writer")
	}
	return &s3StorageWriter{
		local: local,
		w:     w,
	}, nil
}

func (s *s3StorageWriter) LocalFile() string {
	return s.local
}

func (s *s3StorageWriter) Finish() error {
	f, err := os.Open(s.local)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(s.w, f); err != nil {
		return errors.Wrap(err, "failed to copy to s3")
	}
	return s.w.Close()
}

func (s *s3StorageWriter) Cleanup() {
	if err := s.w.Close(); err != nil {
		log.Warningf(context.TODO(), "couldn't cleanup s3StorageWriter: %+v", err)
	}
}

func (s *s3Storage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	r, _, err := s.bucket.GetReader(filepath.Join(s.prefix, basename), nil)
	return r, errors.Wrap(err, "failed to create s3 reader")
}

func fetchFile(r io.ReadCloser, t *tmpHelper, basename string) (string, error) {
	defer r.Close()
	if err := t.init(); err != nil {
		return "", err
	}
	f, err := ioutil.TempFile(t.path, basename)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return "", err
	}
	return f.Name(), nil
}

func (s *s3Storage) FetchFile(ctx context.Context, basename string) (string, error) {
	r, err := s.ReadFile(ctx, basename)
	if err != nil {
		return "", errors.Wrap(err, "failed to create s3 reader for fetch")
	}
	return fetchFile(r, &s.tmp, basename)
}

func (s *s3Storage) Delete(_ context.Context, basename string) error {
	return s.bucket.Delete(filepath.Join(s.prefix, basename))
}

func (s *s3Storage) Close() error {
	return s.tmp.Close()
}

type gcsStorage struct {
	conf   *roachpb.ExportStorage_GCS
	tmp    tmpHelper
	client *gcs.Client
	bucket *gcs.BucketHandle
	prefix string
}

var _ ExportStorage = &gcsStorage{}

type gcsStorageWriter struct {
	w     *gcs.Writer
	local string
}

var _ ExportFileWriter = &gcsStorageWriter{}

func (g *gcsStorage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider:          roachpb.ExportStorageProvider_GoogleCloud,
		GoogleCloudConfig: g.conf,
	}
}

func makeGCSStorage(ctx context.Context, conf *roachpb.ExportStorage_GCS) (ExportStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("google cloud storage upload requested but info missing")
	}
	g, err := gcs.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create google cloud client")
	}
	return &gcsStorage{
		conf:   conf,
		client: g,
		bucket: g.Bucket(conf.Bucket),
		prefix: conf.Prefix,
	}, nil
}

func (g *gcsStorage) PutFile(ctx context.Context, basename string) (ExportFileWriter, error) {
	local, err := g.tmp.tmpFile(basename)
	if err != nil {
		return nil, err
	}
	w := g.bucket.Object(filepath.Join(g.prefix, basename)).NewWriter(ctx)
	return &gcsStorageWriter{w: w, local: local}, nil
}

func (g *gcsStorageWriter) LocalFile() string {
	return g.local
}

func (g *gcsStorageWriter) Finish() error {
	f, err := os.Open(g.local)
	if err != nil {
		return errors.Wrap(err, "failed to open local file")
	}
	defer f.Close()
	if _, err := io.Copy(g.w, f); err != nil {
		return errors.Wrap(err, "failed to write to google cloud")
	}
	return g.w.Close()
}

func (g *gcsStorageWriter) Cleanup() {
	if err := g.w.Close(); err != nil {
		log.Warningf(context.TODO(), "couldn't cleanup gcsStorageWriter: %+v", err)
	}
}

func (g *gcsStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	obj := g.bucket.Object(filepath.Join(g.prefix, basename))
	return obj.NewReader(ctx)
}

func (g *gcsStorage) FetchFile(ctx context.Context, basename string) (string, error) {
	r, err := g.ReadFile(ctx, basename)
	if err != nil {
		return "", err
	}
	return fetchFile(r, &g.tmp, basename)
}

func (g *gcsStorage) Delete(ctx context.Context, basename string) error {
	obj := g.bucket.Object(filepath.Join(g.prefix, basename))
	return obj.Delete(ctx)
}

func (g *gcsStorage) Close() error {
	gErr := g.client.Close()
	err := g.tmp.Close()
	if err != nil {
		return err
	}
	return gErr
}

type azureStorage struct {
	conf   *roachpb.ExportStorage_Azure
	client azr.BlobStorageClient
	prefix string
	tmp    tmpHelper
}

var _ ExportStorage = &azureStorage{}

type azureStorageWriter struct {
	ctx       context.Context
	client    azr.BlobStorageClient
	local     string
	container string
	name      string
}

var _ ExportFileWriter = &azureStorageWriter{}

func makeAzureStorage(conf *roachpb.ExportStorage_Azure) (ExportStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("azure upload requested but info missing")
	}
	client, err := azr.NewBasicClient(conf.AccountName, conf.AccountKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create azure client")
	}
	return &azureStorage{
		conf:   conf,
		client: client.GetBlobService(),
		prefix: conf.Prefix,
	}, err
}

func (s *azureStorage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider:    roachpb.ExportStorageProvider_Azure,
		AzureConfig: s.conf,
	}
}

func (s *azureStorage) PutFile(ctx context.Context, basename string) (ExportFileWriter, error) {
	local, err := s.tmp.tmpFile(basename)
	if err != nil {
		return nil, err
	}
	return &azureStorageWriter{
		ctx:       ctx,
		local:     local,
		client:    s.client,
		container: s.conf.Container,
		name:      filepath.Join(s.prefix, basename),
	}, nil
}

func (s *azureStorageWriter) LocalFile() string {
	return s.local
}

func (s *azureStorageWriter) Finish() error {
	f, err := os.Open(s.local)
	if err != nil {
		return err
	}
	defer f.Close()

	// A blob in Azure is composed of an ordered list of blocks. To create a
	// blob, we must first create an empty block blob (i.e., a blob backed
	// by blocks). Then we upload the blocks. Blocks can only by 4 MiB (in
	// this version of the API) and have some identifier. Once the blocks are
	// uploaded, then we put a block list, which assigns a list of blocks to a
	// blob. When assigning blocks to a blob, we can choose between committed
	// (blocks already assigned to the blob), uncommitted (uploaded blocks not
	// yet assigned to the blob), or latest. chunkReader takes care of splitting
	// up the input file into small enough chunks the API can handle.

	const maxAttempts = 3

	if err := retry.WithMaxAttempts(s.ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		return s.client.CreateBlockBlob(s.container, s.name)
	}); err != nil {
		return errors.Wrap(err, "creating block blob")
	}
	const fourMiB = 1024 * 1024 * 4

	// NB: Azure wants Block IDs to all be the same length.
	// http://gauravmantri.com/2013/05/18/windows-azure-blob-storage-dealing-with-the-specified-blob-or-block-content-is-invalid-error/
	// 9999 * 4mb = 40gb max upload size, well over our max range size.
	const maxBlockID = 9999
	const blockIDFmt = "%04d"

	var blocks []azr.Block
	i := 1
	uploadBlockFunc := func(b []byte) error {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		default:
		}

		if len(b) < 1 {
			return errors.New("cannot upload an empty block")
		}

		if i > maxBlockID {
			return errors.Errorf("too many blocks for azure blob block writer")
		}
		id := base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf(blockIDFmt, i)))
		i++
		blocks = append(blocks, azr.Block{ID: id, Status: azr.BlockStatusUncommitted})
		return retry.WithMaxAttempts(s.ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
			return s.client.PutBlock(s.container, s.name, id, b)
		})
	}

	if err := chunkReader(f, fourMiB, uploadBlockFunc); err != nil {
		return errors.Wrap(err, "putting blocks")
	}

	if err := retry.WithMaxAttempts(s.ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		return s.client.PutBlockList(s.container, s.name, blocks)
	}); err != nil {
		return errors.Wrap(err, "putting block list")
	}

	return nil
}

// chunkReader calls f with chunks of size from r. The same underlying byte
// slice is used for each call to f. f can return an error to stop the file
// reading. If so, that error will be returned.
func chunkReader(r io.Reader, size int, f func([]byte) error) error {
	b := make([]byte, size)
	for {
		n, err := r.Read(b)

		if err != nil && err != io.EOF {
			return errors.Wrap(err, "reading chunk")
		}

		if n > 0 {
			funcErr := f(b[:n])
			if funcErr != nil {
				return funcErr
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *azureStorageWriter) Cleanup() {
}

func (s *azureStorage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	r, err := s.client.GetBlob(s.conf.Container, filepath.Join(s.prefix, basename))
	return r, errors.Wrap(err, "failed to create azure reader")
}

func (s *azureStorage) FetchFile(ctx context.Context, basename string) (string, error) {
	r, err := s.ReadFile(ctx, basename)
	if err != nil {
		return "", errors.Wrap(err, "failed to create azure reader for fetch")
	}
	return fetchFile(r, &s.tmp, basename)
}

func (s *azureStorage) Delete(_ context.Context, basename string) error {
	return errors.Wrap(
		s.client.DeleteBlob(s.conf.Container, filepath.Join(s.prefix, basename), nil),
		"deleting blob",
	)
}

func (s *azureStorage) Close() error {
	return s.tmp.Close()
}

type tmpHelper struct {
	path string
}

func (t *tmpHelper) init() error {
	if t.path == "" {
		base, err := ioutil.TempDir("", "cockroach-export")
		if err != nil {
			return err
		}
		t.path = base
	}
	return nil
}

func (t *tmpHelper) tmpFile(basename string) (string, error) {
	if err := t.init(); err != nil {
		return "", err
	}
	return filepath.Join(t.path, basename), nil
}

func (t *tmpHelper) Close() error {
	if t.path != "" {
		if err := os.RemoveAll(t.path); err != nil {
			return errors.Wrapf(err, "failed to clean up tmpdir %q for remote export storage", t.path)
		}
		t.path = ""
	}
	return nil
}
