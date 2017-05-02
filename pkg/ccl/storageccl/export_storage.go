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
			AccessKey: uri.Query().Get(S3AccessKeyParam),
			Secret:    uri.Query().Get(S3SecretParam),
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
		conf.Provider = roachpb.ExportStorageProvider_LocalFile
		conf.LocalFile.Path = uri.Path
	default:
		return conf, errors.Errorf("unsupported storage scheme: %q", uri.Scheme)
	}
	return conf, nil
}

// SanitizeExportStorageURI returns the export storage URI with sensitive
// credentials stripped.
func SanitizeExportStorageURI(path string) (string, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	// All current export storage providers store credentials in the query string,
	// if they store it in the URI at all.
	uri.RawQuery = ""
	return uri.String(), nil
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

// ExportStorage provides functions to read and write files in some storage,
// namely various cloud storage providers, for example to store backups.
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

func (l *localFileStorage) WriteFile(
	_ context.Context, basename string, content io.ReadSeeker,
) error {
	if err := os.MkdirAll(l.base, 0755); err != nil {
		return errors.Wrap(err, "creating local export storage path")
	}
	path := filepath.Join(l.base, basename)
	f, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "creating local export file %q", path)
	}
	defer f.Close()
	_, err = io.Copy(f, content)
	return errors.Wrapf(err, "writing to local export file %q", path)
}

func (l *localFileStorage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(l.base, basename))
}

func (l *localFileStorage) Delete(_ context.Context, basename string) error {
	return os.Remove(filepath.Join(l.base, basename))
}

func (*localFileStorage) Close() error {
	return nil
}

type httpStorage struct {
	client *http.Client
	base   string
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

func (h *httpStorage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	return runHTTPRequest(h.client, "GET", h.base, basename, nil)
}

func (h *httpStorage) WriteFile(_ context.Context, basename string, content io.ReadSeeker) error {
	_, err := runHTTPRequest(h.client, "PUT", h.base, basename, content)
	return err
}

func (h *httpStorage) Delete(_ context.Context, basename string) error {
	_, err := runHTTPRequest(h.client, "DELETE", h.base, basename, nil)
	return err
}

func (h *httpStorage) Close() error {
	return nil
}

func runHTTPRequest(
	c *http.Client, method, base, file string, body io.Reader,
) (io.ReadCloser, error) {
	url := base + file
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

type s3Storage struct {
	conf   *roachpb.ExportStorage_S3
	bucket *s3gof3r.Bucket
	prefix string
}

var _ ExportStorage = &s3Storage{}

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

func (s *s3Storage) WriteFile(_ context.Context, basename string, content io.ReadSeeker) error {
	w, err := s.bucket.PutWriter(filepath.Join(s.prefix, basename), nil, nil)
	if err != nil {
		return errors.Wrap(err, "creating s3 writer")
	}
	defer w.Close()
	_, err = io.Copy(w, content)
	return errors.Wrap(err, "failed to copy to s3")
}

func (s *s3Storage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	r, _, err := s.bucket.GetReader(filepath.Join(s.prefix, basename), nil)
	return r, errors.Wrap(err, "failed to create s3 reader")
}

func (s *s3Storage) Delete(_ context.Context, basename string) error {
	return s.bucket.Delete(filepath.Join(s.prefix, basename))
}

func (s *s3Storage) Close() error {
	return nil
}

type gcsStorage struct {
	conf   *roachpb.ExportStorage_GCS
	client *gcs.Client
	bucket *gcs.BucketHandle
	prefix string
}

var _ ExportStorage = &gcsStorage{}

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

func (g *gcsStorage) WriteFile(ctx context.Context, basename string, content io.ReadSeeker) error {
	const maxAttempts = 3

	err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		if _, err := content.Seek(0, io.SeekStart); err != nil {
			return err
		}
		w := g.bucket.Object(filepath.Join(g.prefix, basename)).NewWriter(ctx)
		if _, err := io.Copy(w, content); err != nil {
			_ = w.Close()
			return err
		}
		return w.Close()
	})
	return errors.Wrap(err, "write to google cloud")
}

func (g *gcsStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	return g.bucket.Object(filepath.Join(g.prefix, basename)).NewReader(ctx)
}

func (g *gcsStorage) Delete(ctx context.Context, basename string) error {
	return g.bucket.Object(filepath.Join(g.prefix, basename)).Delete(ctx)
}

func (g *gcsStorage) Close() error {
	return g.client.Close()
}

type azureStorage struct {
	conf   *roachpb.ExportStorage_Azure
	client azr.BlobStorageClient
	prefix string
}

var _ ExportStorage = &azureStorage{}

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

func (s *azureStorage) WriteFile(
	ctx context.Context, basename string, content io.ReadSeeker,
) error {
	name := filepath.Join(s.prefix, basename)
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

	writeFile := func() error {
		if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
			return s.client.CreateBlockBlob(s.conf.Container, name)
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
			case <-ctx.Done():
				return ctx.Err()
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
			return retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
				return s.client.PutBlock(s.conf.Container, name, id, b)
			})
		}

		if err := chunkReader(content, fourMiB, uploadBlockFunc); err != nil {
			return errors.Wrap(err, "putting blocks")
		}

		err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
			return s.client.PutBlockList(s.conf.Container, name, blocks)
		})
		return errors.Wrap(err, "putting block list")
	}

	err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		if _, err := content.Seek(0, io.SeekStart); err != nil {
			return errors.Wrap(err, "seek")
		}
		return writeFile()
	})
	return errors.Wrap(err, "write file")
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

func (s *azureStorage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	r, err := s.client.GetBlob(s.conf.Container, filepath.Join(s.prefix, basename))
	return r, errors.Wrap(err, "failed to create azure reader")
}

func (s *azureStorage) Delete(_ context.Context, basename string) error {
	return errors.Wrap(
		s.client.DeleteBlob(s.conf.Container, filepath.Join(s.prefix, basename), nil),
		"deleting blob",
	)
}

func (s *azureStorage) Close() error {
	return nil
}

// FetchFile returns the path to a local file containing the content of
// the requested filename, and a cleanup func to be called when done reading it.
func FetchFile(
	ctx context.Context, tempPrefix string, e ExportStorage, basename string,
) (string, func(), error) {
	cleanup := func() {}
	// special-case local files to avoid copying to tmp.
	if loc, ok := e.(*localFileStorage); ok {
		return filepath.Join(loc.base, basename), cleanup, nil
	}

	if tempPrefix == "" {
		return "", cleanup, errors.New("must provide tempdir path")
	}

	const maxAttempts = 3
	var fileName string
	if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		r, err := e.ReadFile(ctx, basename)
		if err != nil {
			return errors.Wrapf(err, "creating reader for %q", basename)
		}
		defer r.Close()

		f, err := ioutil.TempFile(tempPrefix, basename)
		if err != nil {
			return errors.Wrap(err, "creating tmpfile")
		}
		defer f.Close()
		cleanup = func() {
			if err := errors.Wrapf(os.Remove(f.Name()), "cleaning up tmpfile", f.Name()); err != nil {
				log.Warningf(ctx, "%+v", err)
			}
		}

		if _, err := io.Copy(f, r); err != nil {
			cleanup()
			cleanup = func() {}
			return errors.Wrapf(err, "fetching file content for %q", basename)
		}
		fileName = f.Name()
		return nil
	}); err != nil {
		return "", cleanup, err
	}
	return fileName, cleanup, nil
}

// ExportFileWriter provides a local path, to a file or pipe, that can be
// written to before calling Finish() to store the written content to an
// ExportStorage. This caters to non-Go clients (like RocksDB) that want to open
// and write to a file, rather than just use an Go io.Reader/io.Writer
// interface.
type ExportFileWriter interface {
	// LocalFile returns the path to a local path to which a caller should write.
	LocalFile() string

	// Finish indicates that no further writes to the local file are expected and
	// that the implementation should store the content (copy it, upload, etc) if
	// that has not already been done in a streaming fashion (e.g. via a pipe).
	Finish(ctx context.Context) error

	// Close removes any temporary files or resources that were made on behalf
	// of this writer. If `Finish` has not been called, any writes to
	// `LocalFile` will be lost. Implementations of `Close` are required to be
	// idempotent and should log any errors.
	Close(ctx context.Context)
}

// tmpWriter uses local temp files to implement an ExportFileWriter.
type tmpWriter struct {
	store   ExportStorage
	tmpfile *os.File
	name    string
}

var _ ExportFileWriter = &tmpWriter{}

// MakeExportFileTmpWriter returns an ExportFileWriter backed by a tempfile.
func MakeExportFileTmpWriter(
	_ context.Context, tempPrefix string, store ExportStorage, name string,
) (ExportFileWriter, error) {
	// the special-case that allows local stores to rename rather than copy works
	// only if we alloc the tempfile on same device, so we force dest prefix here.
	if loc, ok := store.(*localFileStorage); ok {
		if err := os.MkdirAll(loc.base, 0755); err != nil {
			return nil, errors.Wrap(err, "creating local file dest")
		}
		f, err := ioutil.TempFile(loc.base, name)
		if err != nil {
			return nil, err
		}
		return &localTmpWriter{tmpWriter{tmpfile: f}, filepath.Join(loc.base, name)}, nil
	}

	if tempPrefix == "" {
		return nil, errors.New("must provide tempdir path")
	}
	f, err := ioutil.TempFile(tempPrefix, name)
	if err != nil {
		return nil, err
	}
	return &tmpWriter{store: store, tmpfile: f, name: name}, nil
}

// LocalFile returns the local temp file.
func (e *tmpWriter) LocalFile() string {
	return e.tmpfile.Name()
}

// Finish uploads the content of the tmpfile using the store's WriteFile.
func (e *tmpWriter) Finish(ctx context.Context) error {
	return e.store.WriteFile(ctx, e.name, e.tmpfile)
}

func (e *tmpWriter) Close(ctx context.Context) {
	if e.tmpfile != nil {
		if err := errors.Wrap(e.tmpfile.Close(), "closing tempfile"); err != nil {
			log.Warningf(ctx, "%+v", err)
		}
		if err := errors.Wrap(os.Remove(e.tmpfile.Name()), "cleaning up tmpfile"); err != nil {
			log.Warningf(ctx, "%+v", err)
		}
		e.tmpfile = nil
	}
}

// localTmpWriter overrides the usual Finish to just rename.
type localTmpWriter struct {
	tmpWriter
	dest string
}

// Finish uploads the content of the tmpfile using the store's WriteFile.
func (l *localTmpWriter) Finish(ctx context.Context) error {
	if err := os.Rename(l.tmpfile.Name(), l.dest); err != nil {
		return err
	}
	// prevent the usual Close()'s deletion.
	l.tmpfile.Close()
	l.tmpfile = nil
	return nil
}
