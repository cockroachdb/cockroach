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
	"encoding/base64"
	"io"
	"net/url"
	"path"
	"strings"

	gcs "cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func parseGSURL(_ ExternalStorageURIContext, uri *url.URL) (roachpb.ExternalStorage, error) {
	conf := roachpb.ExternalStorage{}
	conf.Provider = roachpb.ExternalStorageProvider_GoogleCloud
	conf.GoogleCloudConfig = &roachpb.ExternalStorage_GCS{
		Bucket:         uri.Host,
		Prefix:         uri.Path,
		Auth:           uri.Query().Get(AuthParam),
		BillingProject: uri.Query().Get(GoogleBillingProjectParam),
		Credentials:    uri.Query().Get(CredentialsParam),
		/* NB: additions here should also update gcsQueryParams() serializer */
	}
	conf.GoogleCloudConfig.Prefix = strings.TrimLeft(conf.GoogleCloudConfig.Prefix, "/")
	return conf, nil
}

func gcsQueryParams(conf *roachpb.ExternalStorage_GCS) string {
	q := make(url.Values)
	if conf.Auth != "" {
		q.Set(AuthParam, conf.Auth)
	}
	if conf.Credentials != "" {
		q.Set(CredentialsParam, conf.Credentials)
	}
	if conf.BillingProject != "" {
		q.Set(GoogleBillingProjectParam, conf.BillingProject)
	}
	return q.Encode()
}

type gcsStorage struct {
	bucket   *gcs.BucketHandle
	client   *gcs.Client
	conf     *roachpb.ExternalStorage_GCS
	ioConf   base.ExternalIODirConfig
	prefix   string
	settings *cluster.Settings
}

var _ cloud.ExternalStorage = &gcsStorage{}

func (g *gcsStorage) Conf() roachpb.ExternalStorage {
	return roachpb.ExternalStorage{
		Provider:          roachpb.ExternalStorageProvider_GoogleCloud,
		GoogleCloudConfig: g.conf,
	}
}

func (g *gcsStorage) ExternalIOConf() base.ExternalIODirConfig {
	return g.ioConf
}

func (g *gcsStorage) Settings() *cluster.Settings {
	return g.settings
}

func makeGCSStorage(
	ctx context.Context, args ExternalStorageContext, dest roachpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.google_cloud")
	conf := dest.GoogleCloudConfig
	if conf == nil {
		return nil, errors.Errorf("google cloud storage upload requested but info missing")
	}
	const scope = gcs.ScopeReadWrite
	opts := []option.ClientOption{option.WithScopes(scope)}

	// "default": only use the key in the settings; error if not present.
	// "specified": the JSON object for authentication is given by the CREDENTIALS param.
	// "implicit": only use the environment data.
	// "": if default key is in the settings use it; otherwise use environment data.
	if args.IOConf.DisableImplicitCredentials && conf.Auth != AuthParamSpecified {
		return nil, errors.New(
			"implicit credentials disallowed for gs due to --external-io-disable-implicit-credentials flag")
	}

	switch conf.Auth {
	case "", AuthParamDefault:
		var key string
		if args.Settings != nil {
			key = GcsDefault.Get(&args.Settings.SV)
		}
		// We expect a key to be present if default is specified.
		if conf.Auth == AuthParamDefault && key == "" {
			return nil, errors.Errorf("expected settings value for %s", CloudstorageGSDefaultKey)
		}
		if key != "" {
			source, err := google.JWTConfigFromJSON([]byte(key), scope)
			if err != nil {
				return nil, errors.Wrap(err, "creating GCS oauth token source")
			}
			opts = append(opts, option.WithTokenSource(source.TokenSource(ctx)))
		}
	case AuthParamSpecified:
		if conf.Credentials == "" {
			return nil, errors.Errorf(
				"%s is set to '%s', but %s is not set",
				AuthParam,
				AuthParamSpecified,
				CredentialsParam,
			)
		}
		decodedKey, err := base64.StdEncoding.DecodeString(conf.Credentials)
		if err != nil {
			return nil, errors.Wrapf(err, "decoding value of %s", CredentialsParam)
		}
		source, err := google.JWTConfigFromJSON(decodedKey, scope)
		if err != nil {
			return nil, errors.Wrap(err, "creating GCS oauth token source from specified credentials")
		}
		opts = append(opts, option.WithTokenSource(source.TokenSource(ctx)))
	case AuthParamImplicit:
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
		ioConf:   args.IOConf,
		prefix:   conf.Prefix,
		settings: args.Settings,
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

// ReadFile is shorthand for ReadFileAt with offset 0.
func (g *gcsStorage) ReadFile(ctx context.Context, basename string) (io.ReadCloser, error) {
	reader, _, err := g.ReadFileAt(ctx, basename, 0)
	return reader, err
}

func (g *gcsStorage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (io.ReadCloser, int64, error) {
	object := path.Join(g.prefix, basename)
	r := &resumingReader{
		ctx: ctx,
		opener: func(ctx context.Context, pos int64) (io.ReadCloser, error) {
			return g.bucket.Object(object).NewRangeReader(ctx, pos, -1)
		},
		pos: offset,
	}

	if err := r.openStream(); err != nil {
		if errors.Is(err, gcs.ErrObjectNotExist) {
			// Callers of this method sometimes look at the returned error to determine
			// if file does not exist.  Regardless why we couldn't open the stream
			// (whether its invalid bucket or file doesn't exist),
			// return our internal ErrFileDoesNotExist.
			err = errors.Wrapf(cloud.ErrFileDoesNotExist, "gcs object does not exist: %s", err.Error())
		}
		return nil, 0, err
	}
	return r.reader, r.reader.(*gcs.Reader).Attrs.Size, nil
}

func (g *gcsStorage) ListFiles(ctx context.Context, patternSuffix string) ([]string, error) {
	var fileList []string
	it := g.bucket.Objects(ctx, &gcs.Query{
		Prefix: getPrefixBeforeWildcard(g.prefix),
	})

	pattern := g.prefix
	if patternSuffix != "" {
		if containsGlob(g.prefix) {
			return nil, errors.New("prefix cannot contain globs pattern when passing an explicit pattern")
		}
		pattern = path.Join(pattern, patternSuffix)
	}

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "unable to list files in gcs bucket")
		}

		matches, errMatch := path.Match(pattern, attrs.Name)
		if errMatch != nil {
			continue
		}
		if matches {
			if patternSuffix != "" {
				if !strings.HasPrefix(attrs.Name, g.prefix) {
					// TODO(dt): return a nice rel-path instead of erroring out.
					return nil, errors.New("pattern matched file outside of path")
				}
				fileList = append(fileList, strings.TrimPrefix(strings.TrimPrefix(attrs.Name, g.prefix), "/"))
			} else {
				gsURL := url.URL{
					Scheme:   "gs",
					Host:     attrs.Bucket,
					Path:     attrs.Name,
					RawQuery: gcsQueryParams(g.conf),
				}
				fileList = append(fileList, gsURL.String())
			}
		}
	}

	return fileList, nil
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
