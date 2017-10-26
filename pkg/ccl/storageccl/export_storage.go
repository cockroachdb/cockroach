// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package storageccl

import (
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
	"strings"
	"time"

	"google.golang.org/api/option"

	gcs "cloud.google.com/go/storage"
	azr "github.com/Azure/azure-sdk-for-go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

	// AuthParam is the query parameter for the cluster settings named
	// key in a URI.
	AuthParam         = "AUTH"
	authParamImplicit = "implicit"
	authParamDefault  = "default"

	cloudstoragePrefix       = "cloudstorage"
	cloudstorageGS           = cloudstoragePrefix + ".gs"
	cloudstorageDefault      = ".default"
	cloudstorageKey          = ".key"
	cloudstorageGSDefault    = cloudstorageGS + cloudstorageDefault
	cloudstorageGSDefaultKey = cloudstorageGSDefault + cloudstorageKey
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
			Auth:   uri.Query().Get(AuthParam),
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
		if uri.Host != "" {
			return conf, errors.Errorf("nodelocal does not support hosts: %s", path)
		}
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
func MakeExportStorage(
	ctx context.Context, dest roachpb.ExportStorage, settings *cluster.Settings,
) (ExportStorage, error) {
	switch dest.Provider {
	case roachpb.ExportStorageProvider_LocalFile:
		return makeLocalStorage(dest.LocalFile.Path)
	case roachpb.ExportStorageProvider_Http:
		return makeHTTPStorage(dest.HttpPath.BaseUri)
	case roachpb.ExportStorageProvider_S3:
		return makeS3Storage(ctx, dest.S3Config)
	case roachpb.ExportStorageProvider_GoogleCloud:
		return makeGCSStorage(ctx, dest.GoogleCloudConfig, settings)
	case roachpb.ExportStorageProvider_Azure:
		return makeAzureStorage(dest.AzureConfig)
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
)

type localFileStorage struct {
	base string
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
	p := filepath.Join(l.base, basename)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return errors.Wrap(err, "creating local export storage path")
	}
	f, err := os.Create(p)
	if err != nil {
		return errors.Wrapf(err, "creating local export file %q", p)
	}
	defer f.Close()
	_, err = io.Copy(f, content)
	if err != nil {
		return errors.Wrapf(err, "writing to local export file %q", p)
	}
	return f.Sync()
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
	client *http.Client
	base   *url.URL
	hosts  []string
}

var _ ExportStorage = &httpStorage{}

func makeHTTPStorage(base string) (ExportStorage, error) {
	if base == "" {
		return nil, errors.Errorf("HTTP storage requested but base path not provided")
	}
	client := &http.Client{Transport: &http.Transport{}}
	uri, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	return &httpStorage{client: client, base: uri, hosts: strings.Split(uri.Host, ",")}, nil
}

func (h *httpStorage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider: roachpb.ExportStorageProvider_Http,
		HttpPath: roachpb.ExportStorage_Http{
			BaseUri: h.base.String(),
		},
	}
}

func (h *httpStorage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	resp, err := h.req("GET", basename, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (h *httpStorage) WriteFile(_ context.Context, basename string, content io.ReadSeeker) error {
	_, err := h.req("PUT", basename, content)
	return err
}

func (h *httpStorage) Delete(_ context.Context, basename string) error {
	_, err := h.req("DELETE", basename, nil)
	return err
}

func (h *httpStorage) Size(_ context.Context, basename string) (int64, error) {
	resp, err := h.req("HEAD", basename, nil)
	if err != nil {
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

func (h *httpStorage) req(method, file string, body io.Reader) (*http.Response, error) {
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
	resp, err := h.client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "error exeucting request %s %q", method, url)
	}
	if resp.StatusCode != 200 {
		body, _ := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, errors.Errorf("error response from server: %s %q", resp.Status, body)
	}
	return resp, nil
}

type s3Storage struct {
	conf   *roachpb.ExportStorage_S3
	prefix string
	bucket *string
	s3     *s3.S3
}

func s3Retry(ctx context.Context, fn func() error) error {
	const maxAttempts = 3
	return retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		err := fn()
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
		return err
	})
}

var _ ExportStorage = &s3Storage{}

func makeS3Storage(ctx context.Context, conf *roachpb.ExportStorage_S3) (ExportStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("s3 upload requested but info missing")
	}
	sess, err := session.NewSession(conf.Keys())
	if err != nil {
		return nil, errors.Wrap(err, "new aws session")
	}
	var region string
	err = s3Retry(ctx, func() error {
		var err error
		region, err = s3manager.GetBucketRegion(ctx, sess, conf.Bucket, "us-east-1")
		return err
	})
	if err != nil {
		return nil, errors.Wrap(err, "could not find s3 bucket's region")
	}
	sess.Config.Region = aws.String(region)
	return &s3Storage{
		conf:   conf,
		prefix: conf.Prefix,
		bucket: aws.String(conf.Bucket),
		s3:     s3.New(sess),
	}, nil
}

func (s *s3Storage) Conf() roachpb.ExportStorage {
	return roachpb.ExportStorage{
		Provider: roachpb.ExportStorageProvider_S3,
		S3Config: s.conf,
	}
}

func (s *s3Storage) WriteFile(_ context.Context, basename string, content io.ReadSeeker) error {
	_, err := s.s3.PutObject(&s3.PutObjectInput{
		Bucket: s.bucket,
		Key:    aws.String(path.Join(s.prefix, basename)),
		Body:   content,
	})
	return errors.Wrap(err, "failed to put s3 object")
}

func (s *s3Storage) ReadFile(_ context.Context, basename string) (io.ReadCloser, error) {
	out, err := s.s3.GetObject(&s3.GetObjectInput{
		Bucket: s.bucket,
		Key:    aws.String(path.Join(s.prefix, basename)),
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get s3 object")
	}
	return out.Body, nil
}

func (s *s3Storage) Delete(_ context.Context, basename string) error {
	_, err := s.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: s.bucket,
		Key:    aws.String(path.Join(s.prefix, basename)),
	})
	return err
}

func (s *s3Storage) Size(_ context.Context, basename string) (int64, error) {
	out, err := s.s3.HeadObject(&s3.HeadObjectInput{
		Bucket: s.bucket,
		Key:    aws.String(path.Join(s.prefix, basename)),
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

func makeGCSStorage(
	ctx context.Context, conf *roachpb.ExportStorage_GCS, settings *cluster.Settings,
) (ExportStorage, error) {
	if conf == nil {
		return nil, errors.Errorf("google cloud storage upload requested but info missing")
	}
	const scope = gcs.ScopeReadWrite
	opts := []option.ClientOption{
		option.WithScopes(scope),
	}

	// "default": only use the key in the settings; error if not present.
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
		w := g.bucket.Object(path.Join(g.prefix, basename)).NewWriter(ctx)
		if _, err := io.Copy(w, content); err != nil {
			_ = w.Close()
			return err
		}
		return w.Close()
	})
	return errors.Wrap(err, "write to google cloud")
}

func (g *gcsStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	return g.bucket.Object(path.Join(g.prefix, basename)).NewReader(ctx)
}

func (g *gcsStorage) Delete(ctx context.Context, basename string) error {
	return g.bucket.Object(path.Join(g.prefix, basename)).Delete(ctx)
}

func (g *gcsStorage) Size(ctx context.Context, basename string) (int64, error) {
	r, err := g.bucket.Object(path.Join(g.prefix, basename)).NewReader(ctx)
	if err != nil {
		return 0, err
	}
	sz := r.Size()
	_ = r.Close()
	return sz, nil
}

func (g *gcsStorage) Close() error {
	return g.client.Close()
}

type azureStorage struct {
	conf      *roachpb.ExportStorage_Azure
	container *azr.Container
	prefix    string
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
	blobClient := client.GetBlobService()
	return &azureStorage{
		conf:      conf,
		container: blobClient.GetContainerReference(conf.Container),
		prefix:    conf.Prefix,
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
	name := path.Join(s.prefix, basename)
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

	blob := s.container.GetBlobReference(name)

	writeFile := func() error {
		if err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
			return blob.CreateBlockBlob(nil)
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
				return blob.PutBlock(id, b, nil)
			})
		}

		if err := chunkReader(content, fourMiB, uploadBlockFunc); err != nil {
			return errors.Wrap(err, "putting blocks")
		}

		err := retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
			return blob.PutBlockList(blocks, nil)
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
	r, err := s.container.GetBlobReference(path.Join(s.prefix, basename)).Get(nil)
	return r, errors.Wrap(err, "failed to create azure reader")
}

func (s *azureStorage) Delete(_ context.Context, basename string) error {
	return errors.Wrap(s.container.GetBlobReference(path.Join(s.prefix, basename)).Delete(nil), "failed to delete blob")
}

func (s *azureStorage) Size(_ context.Context, basename string) (int64, error) {
	b := s.container.GetBlobReference(path.Join(s.prefix, basename))
	err := b.GetProperties(nil)
	return b.Properties.ContentLength, errors.Wrap(err, "failed to get blob properties")
}

func (s *azureStorage) Close() error {
	return nil
}
