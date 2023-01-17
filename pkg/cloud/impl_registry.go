// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/url"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
)

// redactedQueryParams is the set of query parameter names registered by the
// external storage providers that should be redacted from external storage URIs
// whenever they are displayed to a user.
var redactedQueryParams = map[string]struct{}{}

// confParsers maps URI schemes to a ExternalStorageURIParser for that scheme.
var confParsers = map[string]ExternalStorageURIParser{}

// implementations maps an ExternalStorageProvider enum value to a constructor
// of instances of that external storage.
var implementations = map[cloudpb.ExternalStorageProvider]ExternalStorageConstructor{}

// rateAndBurstSettings represents a pair of byteSizeSettings used to configure
// the rate a burst properties of a quotapool.RateLimiter.
type rateAndBurstSettings struct {
	rate  *settings.ByteSizeSetting
	burst *settings.ByteSizeSetting
}

type readAndWriteSettings struct {
	read, write rateAndBurstSettings
}

var limiterSettings = map[cloudpb.ExternalStorageProvider]readAndWriteSettings{}

// registerLimiterSettings registers provider specific settings that allow
// limiting the number of bytes read/written to the provider specific
// ExternalStorage.
func registerLimiterSettings(providerType cloudpb.ExternalStorageProvider) {
	sinkName := strings.ToLower(providerType.String())
	if sinkName == "null" {
		sinkName = "nullsink" // keep the settings name pieces free of reserved keywords.
	}

	readRateName := fmt.Sprintf("cloudstorage.%s.read.node_rate_limit", sinkName)
	readBurstName := fmt.Sprintf("cloudstorage.%s.read.node_burst_limit", sinkName)
	writeRateName := fmt.Sprintf("cloudstorage.%s.write.node_rate_limit", sinkName)
	writeBurstName := fmt.Sprintf("cloudstorage.%s.write.node_burst_limit", sinkName)

	limiterSettings[providerType] = readAndWriteSettings{
		read: rateAndBurstSettings{
			rate: settings.RegisterByteSizeSetting(settings.TenantWritable, readRateName,
				"limit on number of bytes per second per node across operations writing to the designated cloud storage provider if non-zero",
				0,
			),
			burst: settings.RegisterByteSizeSetting(settings.TenantWritable, readBurstName,
				"burst limit on number of bytes per second per node across operations writing to the designated cloud storage provider if non-zero",
				0,
			),
		},
		write: rateAndBurstSettings{
			rate: settings.RegisterByteSizeSetting(settings.TenantWritable, writeRateName,
				"limit on number of bytes per second per node across operations writing to the designated cloud storage provider if non-zero",
				0,
			),
			burst: settings.RegisterByteSizeSetting(settings.TenantWritable, writeBurstName,
				"burst limit on number of bytes per second per node across operations writing to the designated cloud storage provider if non-zero",
				0,
			),
		},
	}
}

// RegisterExternalStorageProvider registers an external storage provider for a
// given URI scheme and provider type.
func RegisterExternalStorageProvider(
	providerType cloudpb.ExternalStorageProvider,
	parseFn ExternalStorageURIParser,
	constructFn ExternalStorageConstructor,
	redactedParams map[string]struct{},
	schemes ...string,
) {
	for _, scheme := range schemes {
		if _, ok := confParsers[scheme]; ok {
			panic(fmt.Sprintf("external storage provider already registered for %s", scheme))
		}
		confParsers[scheme] = parseFn
		for param := range redactedParams {
			redactedQueryParams[param] = struct{}{}
		}
	}
	if _, ok := implementations[providerType]; ok {
		panic(fmt.Sprintf("external storage provider already registered for %s", providerType.String()))
	}
	implementations[providerType] = constructFn

	// We do not register limiter settings for the `external` provider. An
	// external connection object represents an underlying external resource that
	// will have its own registered limiters.
	if providerType != cloudpb.ExternalStorageProvider_external {
		registerLimiterSettings(providerType)
	}
}

// ExternalStorageConfFromURI generates an ExternalStorage config from a URI string.
func ExternalStorageConfFromURI(
	path string, user username.SQLUsername,
) (cloudpb.ExternalStorage, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return cloudpb.ExternalStorage{}, err
	}
	if fn, ok := confParsers[uri.Scheme]; ok {
		return fn(ExternalStorageURIContext{CurrentUser: user}, uri)
	}
	// TODO(adityamaru): Link dedicated ExternalStorage scheme docs once ready.
	return cloudpb.ExternalStorage{}, errors.Errorf("unsupported storage scheme: %q - refer to docs to find supported"+
		" storage schemes", uri.Scheme)
}

// ExternalStorageFromURI returns an ExternalStorage for the given URI.
func ExternalStorageFromURI(
	ctx context.Context,
	uri string,
	externalConfig base.ExternalIODirConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
	user username.SQLUsername,
	db isql.DB,
	limiters Limiters,
	metrics metric.Struct,
	opts ...ExternalStorageOption,
) (ExternalStorage, error) {
	conf, err := ExternalStorageConfFromURI(uri, user)
	if err != nil {
		return nil, err
	}
	return MakeExternalStorage(ctx, conf, externalConfig, settings, blobClientFactory,
		db, limiters, metrics, opts...)
}

// SanitizeExternalStorageURI returns the external storage URI with with some
// secrets redacted, for use when showing these URIs in the UI, to provide some
// protection from shoulder-surfing. The param is still present -- just
// redacted -- to make it clearer that that value is indeed persisted interally.
// extraParams which should be scrubbed -- for params beyond those that the
// various cloud-storage URIs supported by this package know about -- can be
// passed allowing this function to be used to scrub other URIs too (such as
// non-cloudstorage changefeed sinks).
func SanitizeExternalStorageURI(path string, extraParams []string) (string, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return "", err
	}
	if uri.Scheme == "experimental-workload" || uri.Scheme == "workload" || uri.Scheme == "null" {
		return path, nil
	}

	params := uri.Query()
	for param := range params {
		if _, ok := redactedQueryParams[param]; ok {
			params.Set(param, "redacted")
		} else {
			for _, p := range extraParams {
				if param == p {
					params.Set(param, "redacted")
				}
			}
		}
	}

	uri.RawQuery = params.Encode()
	return uri.String(), nil
}

// MakeExternalStorage creates an ExternalStorage from the given config.
func MakeExternalStorage(
	ctx context.Context,
	dest cloudpb.ExternalStorage,
	conf base.ExternalIODirConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
	db isql.DB,
	limiters Limiters,
	metrics metric.Struct,
	opts ...ExternalStorageOption,
) (ExternalStorage, error) {
	var cloudMetrics *Metrics
	var ok bool
	if cloudMetrics, ok = metrics.(*Metrics); !ok {
		return nil, errors.Newf("invalid metrics type: %T", metrics)
	}
	args := ExternalStorageContext{
		IOConf:            conf,
		Settings:          settings,
		BlobClientFactory: blobClientFactory,
		DB:                db,
		Options:           opts,
		Limiters:          limiters,
		MetricsRecorder:   cloudMetrics,
	}
	if conf.DisableOutbound && dest.Provider != cloudpb.ExternalStorageProvider_userfile {
		return nil, errors.New("external network access is disabled")
	}
	options := ExternalStorageOptions{}
	for _, o := range opts {
		o(&options)
	}
	if fn, ok := implementations[dest.Provider]; ok {
		e, err := fn(ctx, args, dest)
		if err != nil {
			return nil, err
		}

		// We do not wrap the ExternalStorage for the `external` provider. An
		// external connection object represents an underlying external resource
		// that will have its own `esWrapper`.
		if dest.Provider == cloudpb.ExternalStorageProvider_external {
			return e, nil
		}

		return &esWrapper{
			ExternalStorage: e,
			lim:             limiters[dest.Provider],
			ioRecorder:      options.ioAccountingInterceptor,
			metricsRecorder: newMetricsReadWriter(cloudMetrics),
		}, nil
	}

	return nil, errors.Errorf("unsupported external destination type: %s", dest.Provider.String())
}

type rwLimiter struct {
	read, write *quotapool.RateLimiter
}

// Limiters represents a collection of rate limiters for a given server to use
// when interacting with the providers in the collection.
type Limiters map[cloudpb.ExternalStorageProvider]rwLimiter

func makeLimiter(
	ctx context.Context, sv *settings.Values, s rateAndBurstSettings,
) *quotapool.RateLimiter {
	lim := quotapool.NewRateLimiter(s.rate.Key(), quotapool.Limit(0), 0)
	fn := func(ctx context.Context) {
		rate := quotapool.Limit(s.rate.Get(sv))
		if rate == 0 {
			rate = quotapool.Limit(math.Inf(1))
		}
		burst := s.burst.Get(sv)
		if burst == 0 {
			burst = math.MaxInt64
		}
		lim.UpdateLimit(rate, burst)
	}
	s.rate.SetOnChange(sv, fn)
	s.burst.SetOnChange(sv, fn)
	fn(ctx)
	return lim
}

// MakeLimiters makes limiters for all registered ExternalStorageProviders and
// sets them up to be updated when settings change. It should be called only
// once per server at creation.
func MakeLimiters(ctx context.Context, sv *settings.Values) Limiters {
	m := make(Limiters, len(limiterSettings))
	for k := range limiterSettings {
		l := limiterSettings[k]
		m[k] = rwLimiter{read: makeLimiter(ctx, sv, l.read), write: makeLimiter(ctx, sv, l.write)}
	}
	return m
}

type esWrapper struct {
	ExternalStorage

	lim             rwLimiter
	ioRecorder      ReadWriterInterceptor
	metricsRecorder ReadWriterInterceptor
}

func (e *esWrapper) wrapReader(ctx context.Context, r ioctx.ReadCloserCtx) ioctx.ReadCloserCtx {
	if e.lim.read != nil {
		r = &limitedReader{r: r, lim: e.lim.read}
	}
	if e.ioRecorder != nil {
		r = e.ioRecorder.Reader(ctx, e.ExternalStorage, r)
	}

	r = e.metricsRecorder.Reader(ctx, e.ExternalStorage, r)
	return r
}

func (e *esWrapper) wrapWriter(ctx context.Context, w io.WriteCloser) io.WriteCloser {
	if e.lim.write != nil {
		w = &limitedWriter{w: w, ctx: ctx, lim: e.lim.write}
	}
	if e.ioRecorder != nil {
		w = e.ioRecorder.Writer(ctx, e.ExternalStorage, w)
	}

	w = e.metricsRecorder.Writer(ctx, e.ExternalStorage, w)
	return w
}

func (e *esWrapper) ReadFile(ctx context.Context, basename string) (ioctx.ReadCloserCtx, error) {
	r, err := e.ExternalStorage.ReadFile(ctx, basename)
	if err != nil {
		return r, err
	}

	return e.wrapReader(ctx, r), nil
}

func (e *esWrapper) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (ioctx.ReadCloserCtx, int64, error) {
	r, s, err := e.ExternalStorage.ReadFileAt(ctx, basename, offset)
	if err != nil {
		return r, s, err
	}

	return e.wrapReader(ctx, r), s, nil
}

func (e *esWrapper) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	w, err := e.ExternalStorage.Writer(ctx, basename)
	if err != nil {
		return nil, err
	}

	return e.wrapWriter(ctx, w), nil
}

type limitedReader struct {
	r    ioctx.ReadCloserCtx
	lim  *quotapool.RateLimiter
	pool int64 // used to pool small write calls into fewer bigger limiter calls.
}

func (l *limitedReader) Read(ctx context.Context, p []byte) (int, error) {
	n, err := l.r.Read(ctx, p)
	// rather than go to the limiter on every single request, given those requests
	// can be small and the limiter is not cheap, add up reads until we have some
	// non-trivial size then go to the limiter with that all at once; this does
	// mean we'll be somewhat spiky but only to the batched limit size (128kb).
	l.pool += int64(n)
	const batchedWriteLimit = 128 << 10
	if l.pool > batchedWriteLimit {
		if err := l.lim.WaitN(ctx, l.pool); err != nil {
			log.Warningf(ctx, "failed to throttle write: %+v", err)
		}
		l.pool = 0
	}
	return n, err
}

func (l *limitedReader) Close(ctx context.Context) error {
	if err := l.lim.WaitN(ctx, l.pool); err != nil {
		log.Warningf(ctx, "failed to throttle closing write: %+v", err)
	}
	return l.r.Close(ctx)
}

type limitedWriter struct {
	w    io.WriteCloser
	ctx  context.Context
	lim  *quotapool.RateLimiter
	pool int64 // used to pool small write calls into fewer bigger limiter calls.
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	// rather than go to the limiter on every single request, given those requests
	// can be small and the limiter is not cheap, add up writes until we have some
	// non-trivial size then go to the limiter with that all at once; this does
	// mean we'll be somewhat spiky but only to the batched limit size (128kb).
	l.pool += int64(len(p))
	const batchedWriteLimit = 128 << 10
	if l.pool > batchedWriteLimit {
		if err := l.lim.WaitN(l.ctx, l.pool); err != nil {
			log.Warningf(l.ctx, "failed to throttle write: %+v", err)
		}
		l.pool = 0
	}
	n, err := l.w.Write(p)
	return n, err
}

func (l *limitedWriter) Close() error {
	if err := l.lim.WaitN(l.ctx, l.pool); err != nil {
		log.Warningf(l.ctx, "failed to throttle closing write: %+v", err)
	}
	return l.w.Close()
}

// A ReadWriterInterceptor providers methods that construct Readers and Writers from given Readers
// and Writers.
type ReadWriterInterceptor interface {
	Reader(context.Context, ExternalStorage, ioctx.ReadCloserCtx) ioctx.ReadCloserCtx
	Writer(context.Context, ExternalStorage, io.WriteCloser) io.WriteCloser
}
