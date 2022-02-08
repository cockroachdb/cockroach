// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcp

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	gcs "cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	// GoogleBillingProjectParam is the query parameter for the billing project
	// in a gs URI.
	GoogleBillingProjectParam = "GOOGLE_BILLING_PROJECT"
	// CredentialsParam is the query parameter for the base64-encoded contents of
	// the Google Application Credentials JSON file.
	CredentialsParam = "CREDENTIALS"
)

// gcsChunkingEnabled is used to enable and disable chunking of file upload to
// Google Cloud Storage.
var gcsChunkingEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"cloudstorage.gs.chunking.enabled",
	"enable chunking of file upload to Google Cloud Storage",
	true, /* default */
)

func parseGSURL(_ cloud.ExternalStorageURIContext, uri *url.URL) (roachpb.ExternalStorage, error) {
	conf := roachpb.ExternalStorage{}
	conf.Provider = roachpb.ExternalStorageProvider_gs
	conf.GoogleCloudConfig = &roachpb.ExternalStorage_GCS{
		Bucket:         uri.Host,
		Prefix:         uri.Path,
		Auth:           uri.Query().Get(cloud.AuthParam),
		BillingProject: uri.Query().Get(GoogleBillingProjectParam),
		Credentials:    uri.Query().Get(CredentialsParam),
	}
	conf.GoogleCloudConfig.Prefix = strings.TrimLeft(conf.GoogleCloudConfig.Prefix, "/")
	return conf, nil
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
		Provider:          roachpb.ExternalStorageProvider_gs,
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
	ctx context.Context, args cloud.ExternalStorageContext, dest roachpb.ExternalStorage,
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
	if args.IOConf.DisableImplicitCredentials && conf.Auth == cloud.AuthParamImplicit {
		return nil, errors.New(
			"implicit credentials disallowed for gs due to --external-io-disable-implicit-credentials flag")
	}

	switch conf.Auth {
	case cloud.AuthParamImplicit:
		// Do nothing; use implicit params:
		// https://godoc.org/golang.org/x/oauth2/google#FindDefaultCredentials
	default:
		if conf.Credentials == "" {
			return nil, errors.Errorf(
				"%s must be set unless %q is %q",
				CredentialsParam,
				cloud.AuthParam,
				cloud.AuthParamImplicit,
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

func (g *gcsStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	_, sp := tracing.ChildSpan(ctx, "gcs.Writer")
	defer sp.Finish()
	sp.RecordStructured(&types.StringValue{Value: fmt.Sprintf("gcs.Writer: %s",
		path.Join(g.prefix, basename))})

	w := g.bucket.Object(path.Join(g.prefix, basename)).NewWriter(ctx)
	if !gcsChunkingEnabled.Get(&g.settings.SV) {
		w.ChunkSize = 0
	}
	return w, nil
}

// ReadFile is shorthand for ReadFileAt with offset 0.
func (g *gcsStorage) ReadFile(ctx context.Context, basename string) (ioctx.ReadCloserCtx, error) {
	reader, _, err := g.ReadFileAt(ctx, basename, 0)
	return reader, err
}

func (g *gcsStorage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (ioctx.ReadCloserCtx, int64, error) {
	object := path.Join(g.prefix, basename)

	ctx, sp := tracing.ChildSpan(ctx, "gcs.ReadFileAt")
	defer sp.Finish()
	sp.RecordStructured(&types.StringValue{Value: fmt.Sprintf("gcs.ReadFileAt: %s",
		path.Join(g.prefix, basename))})

	r := cloud.NewResumingReader(ctx,
		func(ctx context.Context, pos int64) (io.ReadCloser, error) {
			return g.bucket.Object(object).NewRangeReader(ctx, pos, -1)
		}, // opener
		nil, //  reader
		offset,
		cloud.IsResumableHTTPError,
		nil, // errFn
	)

	if err := r.Open(ctx); err != nil {
		if errors.Is(err, gcs.ErrObjectNotExist) {
			// Callers of this method sometimes look at the returned error to determine
			// if file does not exist.  Regardless why we couldn't open the stream
			// (whether its invalid bucket or file doesn't exist),
			// return our internal ErrFileDoesNotExist.
			// nolint:errwrap
			err = errors.Wrapf(
				errors.Wrap(cloud.ErrFileDoesNotExist, "gcs object does not exist"),
				"%v",
				err.Error(),
			)
		}
		return nil, 0, err
	}
	return r, r.Reader.(*gcs.Reader).Attrs.Size, nil
}

func (g *gcsStorage) List(ctx context.Context, prefix, delim string, fn cloud.ListingFn) error {
	dest := cloud.JoinPathPreservingTrailingSlash(g.prefix, prefix)
	ctx, sp := tracing.ChildSpan(ctx, "gcs.List")
	defer sp.Finish()
	sp.RecordStructured(&types.StringValue{Value: fmt.Sprintf("gcs.List: %s", dest)})

	it := g.bucket.Objects(ctx, &gcs.Query{Prefix: dest, Delimiter: delim})

	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "unable to list files in gcs bucket")
		}
		name := attrs.Name
		if name == "" {
			name = attrs.Prefix
		}
		if err := fn(strings.TrimPrefix(name, dest)); err != nil {
			return err
		}
	}
}

func (g *gcsStorage) Delete(ctx context.Context, basename string) error {
	return contextutil.RunWithTimeout(ctx, "delete gcs file",
		cloud.Timeout.Get(&g.settings.SV),
		func(ctx context.Context) error {
			return g.bucket.Object(path.Join(g.prefix, basename)).Delete(ctx)
		})
}

func (g *gcsStorage) Size(ctx context.Context, basename string) (int64, error) {
	var r *gcs.Reader
	if err := contextutil.RunWithTimeout(ctx, "size gcs file",
		cloud.Timeout.Get(&g.settings.SV),
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

func init() {
	cloud.RegisterExternalStorageProvider(roachpb.ExternalStorageProvider_gs,
		parseGSURL, makeGCSStorage, cloud.RedactedParams(CredentialsParam), "gs")
}
