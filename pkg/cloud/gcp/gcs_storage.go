// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcp

import (
	"context"
	"encoding/base64"
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
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/net/http2"
	"golang.org/x/oauth2"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	gtransport "google.golang.org/api/transport/http"
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
	settings.ApplicationLevel,
	"cloudstorage.gs.chunking.enabled",
	"enable chunking of file upload to Google Cloud Storage",
	true, /* default */
)

// gcsChunkRetryTimeout is used to configure the per-chunk retry deadline when
// uploading chunks to Google Cloud Storage.
var gcsChunkRetryTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"cloudstorage.gs.chunking.retry_timeout",
	"per-chunk retry deadline when chunking of file upload to Google Cloud Storage",
	60*time.Second,
	settings.WithName("cloudstorage.gs.chunking.per_chunk_retry.timeout"),
)

func parseGSURL(uri *url.URL) (cloudpb.ExternalStorage, error) {
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
	ctx context.Context, args cloud.EarlyBootExternalStorageContext, dest cloudpb.ExternalStorage,
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

	clientName := args.ExternalStorageOptions().ClientName
	baseTransport, err := cloud.MakeTransport(args.Settings, args.MetricsRecorder, "gcs", conf.Bucket, clientName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create http transport")
	}

	t, err := gtransport.NewTransport(ctx, baseTransport, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create gcs http transport")
	}

	httpClient, err := cloud.MakeHTTPClientForTransport(t)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create http client")
	}

	g, err := gcs.NewClient(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create google cloud client")
	}
	g.SetRetry(gcs.WithErrorFunc(shouldRetry))
	g.SetRetry(gcs.WithPolicy(gcs.RetryAlways))
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
	sp.SetTag("path", attribute.StringValue(path.Join(g.prefix, basename)))

	w := g.bucket.Object(path.Join(g.prefix, basename)).NewWriter(ctx)
	w.ChunkSize = int(cloud.WriteChunkSize.Get(&g.settings.SV))
	if !gcsChunkingEnabled.Get(&g.settings.SV) {
		w.ChunkSize = 0
	}
	w.ChunkRetryDeadline = gcsChunkRetryTimeout.Get(&g.settings.SV)
	return w, nil
}

// isNotExistErr checks if the error indicates a file does not exist
func isNotExistErr(err error) bool {
	return errors.Is(err, gcs.ErrObjectNotExist)
}

func (g *gcsStorage) ReadFile(
	ctx context.Context, basename string, opts cloud.ReadOptions,
) (ioctx.ReadCloserCtx, int64, error) {
	object := path.Join(g.prefix, basename)

	ctx, sp := tracing.ChildSpan(ctx, "gcs.ReadFile")
	defer sp.Finish()
	sp.SetTag("path", attribute.StringValue(path.Join(g.prefix, basename)))

	endPos := int64(0)
	if opts.LengthHint != 0 {
		endPos = opts.Offset + opts.LengthHint
	}

	r := cloud.NewResumingReader(ctx,
		func(ctx context.Context, pos int64) (io.ReadCloser, int64, error) {
			length := int64(-1)
			if endPos != 0 {
				length = endPos - pos
				if length <= 0 {
					return nil, 0, io.EOF
				}
			}
			r, err := g.bucket.Object(object).NewRangeReader(ctx, pos, length)
			if err != nil {
				return nil, 0, err
			}
			return r, r.Attrs.Size, nil
		}, // opener
		nil, //  reader
		opts.Offset,
		0,
		object,
		cloud.ResumingReaderRetryOnErrFnForSettings(ctx, g.settings),
		nil, // errFn
	)

	if err := r.Open(ctx); err != nil {
		if isNotExistErr(err) {
			return nil, 0, cloud.WrapErrFileDoesNotExist(err, "gcs object does not exist")
		}
		return nil, 0, err
	}
	return r, r.Reader.(*gcs.Reader).Attrs.Size, nil
}

func (g *gcsStorage) List(ctx context.Context, prefix, delim string, fn cloud.ListingFn) error {
	dest := cloud.JoinPathPreservingTrailingSlash(g.prefix, prefix)
	ctx, sp := tracing.ChildSpan(ctx, "gcs.List")
	defer sp.Finish()
	sp.SetTag("path", attribute.StringValue(dest))

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
	err := timeutil.RunWithTimeout(ctx, "delete gcs file", cloud.Timeout.Get(&g.settings.SV),
		func(ctx context.Context) error {
			return g.bucket.Object(path.Join(g.prefix, basename)).Delete(ctx)
		})
	if isNotExistErr(err) {
		return nil
	}
	return err
}

func (g *gcsStorage) Size(ctx context.Context, basename string) (int64, error) {
	var r *gcs.Reader
	if err := timeutil.RunWithTimeout(ctx, "size gcs file",
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
		cloud.RegisteredProvider{
			EarlyBootParseFn:     parseGSURL,
			EarlyBootConstructFn: makeGCSStorage,
			RedactedParams:       cloud.RedactedParams(CredentialsParam, BearerTokenParam),
			Schemes:              []string{gcsScheme},
		})
}
