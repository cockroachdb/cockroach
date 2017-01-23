// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/ccl/LICENSE

package storageccl

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/pkg/errors"

	gcs "cloud.google.com/go/storage"
	"github.com/rlmcpherson/s3gof3r"
)

const (
	// S3AccessKeyParam is the query parameter for access_key in an s3 URI.
	S3AccessKeyParam = "AWS_ACCESS_KEY_ID"
	// S3SecretParam is the query parameter for the 'secret' in an s3 URI .
	S3SecretParam = "AWS_SECRET_ACCESS_KEY"
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
			return conf, errors.Errorf("s3 uri missing %q paramer", S3AccessKeyParam)
		}
		if conf.S3Config.Secret == "" {
			return conf, errors.Errorf("s3 uri missing %q paramer", S3SecretParam)
		}
		conf.S3Config.Prefix = strings.TrimLeft(conf.S3Config.Prefix, "/")
	case "gs":
		conf.Provider = roachpb.ExportStorageProvider_GoogleCloud
		conf.GoogleCloudConfig = &roachpb.ExportStorage_GCS{
			Bucket: uri.Host,
			Prefix: uri.Path,
		}
		conf.GoogleCloudConfig.Prefix = strings.TrimLeft(conf.GoogleCloudConfig.Prefix, "/")
	case "", "file":
		conf.Provider = roachpb.ExportStorageProvider_LocalFile
		conf.LocalFile = roachpb.ExportStorage_LocalFilePath{Path: uri.Path}
	default:
		return conf, errors.Errorf("unsupported storage scheme: %q", uri.Scheme)
	}
	return conf, nil
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
	case roachpb.ExportStorageProvider_S3:
		return makeS3Storage(dest.S3Config)
	case roachpb.ExportStorageProvider_GoogleCloud:
		return makeGCSStorage(ctx, dest.GoogleCloudConfig)
	}
	return nil, errors.Errorf("unsupported export destination type: %s", dest.Provider.String())
}

// ExportStorage handles reading and writing files in an export.
type ExportStorage interface {
	io.Closer
	Conf() roachpb.ExportStorage
	PutFile(ctx context.Context, basename string) (ExportFileWriter, error)
	ReadFile(ctx context.Context, basename string) (io.ReadCloser, error)
	FetchFile(ctx context.Context, basename string) (string, error)
	Delete(ctx context.Context, basename string) error
}

// ExportFileWriter provides a local file that an export can write to before
// calling Finish() to store said file. As some callers may be non-Go (e.g. the
// RocksDB SSTable Writer), this exposes the path to a file, not an io.Writer.
type ExportFileWriter interface {
	io.Closer
	LocalFile() string
	Finish() error
}

type localFileStorage struct {
	base string
}

var _ ExportStorage = &localFileStorage{}

func makeLocalStorage(base string) (ExportStorage, error) {
	if base == "" {
		return nil, errors.Errorf("Local storage requested but path not provided")
	}
	if err := os.MkdirAll(base, 0777); err != nil {
		return nil, errors.Wrapf(err, "failed to create %q", base)
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

func (l localFileStorageWriter) Close() error {
	return nil
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
	return s.Close()
}

func (s *s3StorageWriter) Close() error {
	return s.w.Close()
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
	return g.Close()
}

func (g *gcsStorageWriter) Close() error {
	return g.w.Close()
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
