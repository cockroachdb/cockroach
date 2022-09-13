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
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
	"golang.org/x/net/http2"
	"golang.org/x/oauth2"
	"google.golang.org/api/impersonate"
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
	// AssumeRoleParam is the query parameter for the chain of service account
	// email addresses to assume.
	AssumeRoleParam = "ASSUME_ROLE"

	// BearerTokenParam is the query parameter for a temporary bearer token. There
	// is no refresh mechanism associated with this token, so it is up to the user
	// to ensure that its TTL is longer than the duration of the job or query that
	// is using the token. The job or query may irrecoverably fail if one of its
	// tokens expire before completion.
	BearerTokenParam = "BEARER_TOKEN"
)

// gcsChunkingEnabled is used to enable and disable chunking of file upload to
// Google Cloud Storage.
var gcsChunkingEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"cloudstorage.gs.chunking.enabled",
	"enable chunking of file upload to Google Cloud Storage",
	true, /* default */
)

// gcsChunkRetryTimeout is used to configure the per-chunk retry deadline when
// uploading chunks to Google Cloud Storage.
var gcsChunkRetryTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"cloudstorage.gs.chunking.retry_timeout",
	"per-chunk retry deadline when chunking of file upload to Google Cloud Storage",
	60*time.Second,
)

func parseGSURL(_ cloud.ExternalStorageURIContext, uri *url.URL) (cloudpb.ExternalStorage, error) {
	gsURL := cloud.ConsumeURL{URL: uri}
	conf := cloudpb.ExternalStorage{}
	conf.Provider = cloudpb.ExternalStorageProvider_gs
	assumeRole, delegateRoles := cloud.ParseRoleString(gsURL.ConsumeParam(AssumeRoleParam))
	conf.GoogleCloudConfig = &cloudpb.ExternalStorage_GCS{
		Bucket:              uri.Host,
		Prefix:              uri.Path,
		Auth:                gsURL.ConsumeParam(cloud.AuthParam),
		BillingProject:      gsURL.ConsumeParam(GoogleBillingProjectParam),
		Credentials:         gsURL.ConsumeParam(CredentialsParam),
		AssumeRole:          assumeRole,
		AssumeRoleDelegates: delegateRoles,
		BearerToken:         gsURL.ConsumeParam(BearerTokenParam),
	}
	conf.GoogleCloudConfig.Prefix = strings.TrimLeft(conf.GoogleCloudConfig.Prefix, "/")

	// Validate that all the passed in parameters are supported.
	if unknownParams := gsURL.RemainingQueryParams(); len(unknownParams) > 0 {
		return cloudpb.ExternalStorage{}, errors.Errorf(
			`unknown GS query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	return conf, nil
}

type gcsStorage struct {
	bucket   *gcs.BucketHandle
	client   *gcs.Client
	conf     *cloudpb.ExternalStorage_GCS
	ioConf   base.ExternalIODirConfig
	prefix   string
	settings *cluster.Settings
}

var _ cloud.ExternalStorage = &gcsStorage{}

func (g *gcsStorage) Conf() cloudpb.ExternalStorage {
	return cloudpb.ExternalStorage{
		Provider:          cloudpb.ExternalStorageProvider_gs,
		GoogleCloudConfig: g.conf,
	}
}

func (g *gcsStorage) ExternalIOConf() base.ExternalIODirConfig {
	return g.ioConf
}

func (g *gcsStorage) RequiresExternalIOAccounting() bool { return true }

func (g *gcsStorage) Settings() *cluster.Settings {
	return g.settings
}

func makeGCSStorage(
	ctx context.Context, args cloud.ExternalStorageContext, dest cloudpb.ExternalStorage,
) (cloud.ExternalStorage, error) {
	telemetry.Count("external-io.google_cloud")
	conf := dest.GoogleCloudConfig
	if conf == nil {
		return nil, errors.Errorf("google cloud storage upload requested but info missing")
	}
	const scope = gcs.ScopeReadWrite

	// "default": only use the key in the settings; error if not present.
	// "specified": the JSON object for authentication is given by the CREDENTIALS param.
	// "implicit": only use the environment data.
	// "": if default key is in the settings use it; otherwise use environment data.
	if args.IOConf.DisableImplicitCredentials && conf.Auth == cloud.AuthParamImplicit {
		return nil, errors.New(
			"implicit credentials disallowed for gs due to --external-io-disable-implicit-credentials flag")
	}

	var credentialsOpt []option.ClientOption
	switch conf.Auth {
	case cloud.AuthParamImplicit:
		// Do nothing; use implicit params:
		// https://godoc.org/golang.org/x/oauth2/google#FindDefaultCredentials
	default:
		if conf.Credentials != "" {
			authOption, err := createAuthOptionFromServiceAccountKey(conf.Credentials)
			if err != nil {
				return nil, errors.Wrapf(err, "error getting credentials from %s", CredentialsParam)
			}
			credentialsOpt = append(credentialsOpt, authOption)
		} else if conf.BearerToken != "" {
			credentialsOpt = append(credentialsOpt, createAuthOptionFromBearerToken(conf.BearerToken))
		} else {
			return nil, errors.Errorf(
				"%s or %s must be set if %q is %q",
				CredentialsParam,
				BearerTokenParam,
				cloud.AuthParam,
				cloud.AuthParamSpecified,
			)
		}
	}

	opts := []option.ClientOption{option.WithScopes(scope)}
	// Once credentials have been obtained via implicit or specified params, we
	// then check if we should use the credentials directly or whether they should
	// be used to assume another role.
	if conf.AssumeRole == "" {
		opts = append(opts, credentialsOpt...)
	} else {
		assumeOpt, err := createImpersonateCredentials(ctx, conf.AssumeRole, conf.AssumeRoleDelegates, []string{scope}, credentialsOpt...)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to assume role")
		}

		opts = append(opts, assumeOpt)
	}

	g, err := gcs.NewClient(ctx, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create google cloud client")
	}
	g.SetRetry(gcs.WithErrorFunc(shouldRetry))
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

// createAuthOptionFromServiceAccountKey creates an option.ClientOption for
// authentication with the given Service Account key.
func createAuthOptionFromServiceAccountKey(encodedKey string) (option.ClientOption, error) {
	// Service Account keys are passed in base64 encoded, so decode it first.
	credentialsJSON, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		return nil, err
	}

	return option.WithCredentialsJSON(credentialsJSON), nil
}

// createAuthOptionFromBearerToken creates an option.ClientOption for
// authentication with the given bearer token.
func createAuthOptionFromBearerToken(bearerToken string) option.ClientOption {
	token := &oauth2.Token{AccessToken: bearerToken}
	return option.WithTokenSource(oauth2.StaticTokenSource(token))
}

// createImpersonateCredentials creates an option.ClientOption for
// authentication for the specified scopes by impersonating the target,
// potentially with authentication options from authOpts.
func createImpersonateCredentials(
	ctx context.Context,
	impersonateTarget string,
	impersonateDelegates []string,
	scopes []string,
	authOpts ...option.ClientOption,
) (option.ClientOption, error) {

	cfg := impersonate.CredentialsConfig{
		TargetPrincipal: impersonateTarget,
		Scopes:          scopes,
		Delegates:       impersonateDelegates,
	}

	source, err := impersonate.CredentialsTokenSource(ctx, cfg, authOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "impersonate credentials")
	}

	return option.WithTokenSource(source), nil
}

func (g *gcsStorage) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	_, sp := tracing.ChildSpan(ctx, "gcs.Writer")
	defer sp.Finish()
	sp.RecordStructured(&types.StringValue{Value: fmt.Sprintf("gcs.Writer: %s",
		path.Join(g.prefix, basename))})

	w := g.bucket.Object(path.Join(g.prefix, basename)).NewWriter(ctx)
	w.ChunkSize = int(cloud.WriteChunkSize.Get(&g.settings.SV))
	if !gcsChunkingEnabled.Get(&g.settings.SV) {
		w.ChunkSize = 0
	}
	w.ChunkRetryDeadline = gcsChunkRetryTimeout.Get(&g.settings.SV)
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
				errors.Wrapf(cloud.ErrFileDoesNotExist, "gcs object %q does not exist", object),
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

// shouldRetry is the predicate that determines whether a GCS client error
// should be retried. The predicate combines google-cloud-go's default retry
// predicate and some additional predicates when determining whether the error
// is retried. The additional predicates are:
//
// - http2.StreamError error with code http2.ErrCodeInternal: this error has
// been recommended to be retried in several issues in the google-cloud-go repo:
// https://github.com/googleapis/google-cloud-go/issues/3735
// https://github.com/googleapis/google-cloud-go/issues/784
// Remove if this error ever becomes part of the default retry predicate.
func shouldRetry(err error) bool {
	if defaultShouldRetry(err) {
		return true
	}

	if e := (http2.StreamError{}); errors.As(err, &e) {
		if e.Code == http2.ErrCodeInternal {
			return true
		}
	}

	if e := (errors.Wrapper)(nil); errors.As(err, &e) {
		return shouldRetry(e.Unwrap())
	}

	return false
}

func init() {
	cloud.RegisterExternalStorageProvider(cloudpb.ExternalStorageProvider_gs,
		parseGSURL, makeGCSStorage, cloud.RedactedParams(CredentialsParam, BearerTokenParam), gcsScheme)
}
