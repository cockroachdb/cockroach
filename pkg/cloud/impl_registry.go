// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"math"
	"net/http/httptrace"
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
var earlyBootConfParsers = map[string]EarlyBootExternalStorageURIParser{}

// implementations maps an ExternalStorageProvider enum value to a constructor
// of instances of that external storage.
var implementations = map[cloudpb.ExternalStorageProvider]ExternalStorageConstructor{}
var earlyBootImplementations = map[cloudpb.ExternalStorageProvider]EarlyBootExternalStorageConstructor{}

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

	readRateName := settings.InternalKey(fmt.Sprintf("cloudstorage.%s.read.node_rate_limit", sinkName))
	readBurstName := settings.InternalKey(fmt.Sprintf("cloudstorage.%s.read.node_burst_limit", sinkName))
	writeRateName := settings.InternalKey(fmt.Sprintf("cloudstorage.%s.write.node_rate_limit", sinkName))
	writeBurstName := settings.InternalKey(fmt.Sprintf("cloudstorage.%s.write.node_burst_limit", sinkName))

	limiterSettings[providerType] = readAndWriteSettings{
		read: rateAndBurstSettings{
			rate: settings.RegisterByteSizeSetting(settings.ApplicationLevel, readRateName,
				"limit on number of bytes per second per node across operations writing to the designated cloud storage provider if non-zero",
				0,
				settings.WithPublic,
			),
			burst: settings.RegisterByteSizeSetting(settings.ApplicationLevel, readBurstName,
				"burst limit on number of bytes per second per node across operations writing to the designated cloud storage provider if non-zero",
				0,
				settings.WithPublic,
			),
		},
		write: rateAndBurstSettings{
			rate: settings.RegisterByteSizeSetting(settings.ApplicationLevel, writeRateName,
				"limit on number of bytes per second per node across operations writing to the designated cloud storage provider if non-zero",
				0,
				settings.WithPublic,
			),
			burst: settings.RegisterByteSizeSetting(settings.ApplicationLevel, writeBurstName,
				"burst limit on number of bytes per second per node across operations writing to the designated cloud storage provider if non-zero",
				0,
				settings.WithPublic,
			),
		},
	}
}

type RegisteredProvider struct {
	ParseFn     ExternalStorageURIParser
	ConstructFn ExternalStorageConstructor

	EarlyBootParseFn     EarlyBootExternalStorageURIParser
	EarlyBootConstructFn EarlyBootExternalStorageConstructor

	RedactedParams map[string]struct{}
	Schemes        []string
}

// SchemeSupportsEarlyBoot returns an error if the scheme of the
// provided URL has a provider that can't be used during early boot.
func SchemeSupportsEarlyBoot(path string) error {
	uri, err := url.Parse(path)
	if err != nil {
		return err
	}
	_, ok := earlyBootConfParsers[uri.Scheme]
	if !ok {
		return errors.Newf("scheme %s is not accessible during node startup", uri.Scheme)
	}
	return nil
}

// RegisterExternalStorageProvider registers an external storage provider for a
// given URI scheme and provider type.
func RegisterExternalStorageProvider(
	providerType cloudpb.ExternalStorageProvider, provider RegisteredProvider,
) {
	registerExternalStorageProviderImpls(providerType, provider)
	// We do not register limiter settings for the `external` provider. An
	// external connection object represents an underlying external resource that
	// will have its own registered limiters.
	if providerType != cloudpb.ExternalStorageProvider_external {
		registerLimiterSettings(providerType)
	}
}

func registerExternalStorageProviderImpls(
	providerType cloudpb.ExternalStorageProvider, provider RegisteredProvider,
) {
	for _, scheme := range provider.Schemes {
		if _, ok := confParsers[scheme]; ok {
			panic(fmt.Sprintf("external storage provider already registered for %s", scheme))
		}

		if provider.ParseFn != nil {
			confParsers[scheme] = provider.ParseFn
		} else if provider.EarlyBootParseFn != nil {
			confParsers[scheme] = func(_ ExternalStorageURIContext, u *url.URL) (cloudpb.ExternalStorage, error) {
				return provider.EarlyBootParseFn(u)
			}
		} else {
			panic(fmt.Sprintf("ParseFn or EarlyBootParseFn must be set for %s", providerType))
		}

		if provider.EarlyBootParseFn != nil {
			earlyBootConfParsers[scheme] = provider.EarlyBootParseFn
		}

		RegisterRedactedParams(provider.RedactedParams)
	}

	if _, ok := implementations[providerType]; ok {
		panic(fmt.Sprintf("external storage provider already registered for %s", providerType.String()))
	}

	if provider.ConstructFn != nil {
		implementations[providerType] = provider.ConstructFn
	} else if provider.EarlyBootConstructFn != nil {
		implementations[providerType] = func(
			ctx context.Context, args ExternalStorageContext, es cloudpb.ExternalStorage,
		) (ExternalStorage, error) {
			return provider.EarlyBootConstructFn(ctx, args.EarlyBootExternalStorageContext, es)
		}
	} else {
		panic(fmt.Sprintf("ConstructFn or EarlyBootConstructFn must be set for %s", providerType))
	}

	if provider.EarlyBootConstructFn != nil {
		earlyBootImplementations[providerType] = provider.EarlyBootConstructFn
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
	getImpl := func(provider cloudpb.ExternalStorageProvider) (func(context.Context, ExternalStorageContext, cloudpb.ExternalStorage) (ExternalStorage, error), bool) {
		fn, ok := implementations[provider]
		return fn, ok
	}
	args := ExternalStorageContext{
		EarlyBootExternalStorageContext: EarlyBootExternalStorageContext{
			IOConf:          conf,
			Settings:        settings,
			Options:         opts,
			Limiters:        limiters,
			MetricsRecorder: cloudMetrics,
		},
		BlobClientFactory: blobClientFactory,
		DB:                db,
	}

	return makeExternalStorage[ExternalStorageContext](ctx, dest, conf, limiters, metrics, settings, args, getImpl, opts...)
}

// EarlyBootExternalStorageConfFromURI generates an
// cloudpb.ExternalStorage config from a URI string. The returned
// cloudpb.ExternalStorage is guaranteed to be for an ExternalStorage
// provider that is accessible without access to SQL or the underlying
// store.
func EarlyBootExternalStorageConfFromURI(path string) (cloudpb.ExternalStorage, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return cloudpb.ExternalStorage{}, err
	}
	if fn, ok := earlyBootConfParsers[uri.Scheme]; ok {
		return fn(uri)
	}
	return cloudpb.ExternalStorage{}, errors.Errorf("unsupported storage scheme: %q - refer to docs to find supported storage schemes",
		uri.Scheme)
}

// EarlyBootExternalStorageFromURI returns an ExternalStorage for the
// given URI. Returned ExternalStorage providers are guaranteed to be
// accessible without access to SQL or the underlying store.
func EarlyBootExternalStorageFromURI(
	ctx context.Context,
	uri string,
	externalConfig base.ExternalIODirConfig,
	settings *cluster.Settings,
	limiters Limiters,
	metrics metric.Struct,
	opts ...ExternalStorageOption,
) (ExternalStorage, error) {
	conf, err := EarlyBootExternalStorageConfFromURI(uri)
	if err != nil {
		return nil, err
	}
	return MakeEarlyBootExternalStorage(ctx, conf, externalConfig, settings, limiters, metrics, opts...)
}

// MakeEarlyBootExternalStorage creates an ExternalStorage from the
// given config. The returned ExternalStorage is guaranteed to be
// accessible without access to SQL or the underlying store.
func MakeEarlyBootExternalStorage(
	ctx context.Context,
	dest cloudpb.ExternalStorage,
	conf base.ExternalIODirConfig,
	settings *cluster.Settings,
	limiters Limiters,
	metrics metric.Struct,
	opts ...ExternalStorageOption,
) (ExternalStorage, error) {
	getImpl := func(provider cloudpb.ExternalStorageProvider) (func(context.Context, EarlyBootExternalStorageContext, cloudpb.ExternalStorage) (ExternalStorage, error), bool) {
		fn, ok := earlyBootImplementations[provider]
		return fn, ok
	}
	args := EarlyBootExternalStorageContext{
		IOConf:   conf,
		Settings: settings,
		Options:  opts,
		Limiters: limiters,
	}

	return makeExternalStorage[EarlyBootExternalStorageContext](ctx, dest, conf, limiters, metrics, settings, args, getImpl, opts...)
}

type rwLimiter struct {
	read, write *quotapool.RateLimiter
}

// Limiters represents a collection of rate limiters for a given server to use
// when interacting with the providers in the collection.
type Limiters map[cloudpb.ExternalStorageProvider]rwLimiter

func makeLimiter(sv *settings.Values, s rateAndBurstSettings) *quotapool.RateLimiter {
	lim := quotapool.NewRateLimiter(string(s.rate.Name()), quotapool.Limit(0), 0)
	fn := func(_ context.Context) {
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
	fn(context.Background())
	return lim
}

// MakeLimiters makes limiters for all registered ExternalStorageProviders and
// sets them up to be updated when settings change. It should be called only
// once per server at creation.
func MakeLimiters(sv *settings.Values) Limiters {
	m := make(Limiters, len(limiterSettings))
	for k := range limiterSettings {
		l := limiterSettings[k]
		m[k] = rwLimiter{read: makeLimiter(sv, l.read), write: makeLimiter(sv, l.write)}
	}
	return m
}

type esWrapper struct {
	ExternalStorage

	lim        rwLimiter
	ioRecorder ReadWriterInterceptor
	metrics    *Metrics
	httpTracer *httptrace.ClientTrace
}

func (e *esWrapper) wrapReader(ctx context.Context, r ioctx.ReadCloserCtx) ioctx.ReadCloserCtx {
	if e.lim.read != nil {
		r = &limitedReader{r: r, lim: e.lim.read}
	}
	if e.ioRecorder != nil {
		r = e.ioRecorder.Reader(ctx, e.ExternalStorage, r)
	}

	r = e.metrics.Reader(ctx, e.ExternalStorage, r)
	return r
}

func (e *esWrapper) wrapWriter(ctx context.Context, w io.WriteCloser) io.WriteCloser {
	if e.lim.write != nil {
		w = &limitedWriter{w: w, ctx: ctx, lim: e.lim.write}
	}
	if e.ioRecorder != nil {
		w = e.ioRecorder.Writer(ctx, e.ExternalStorage, w)
	}

	w = e.metrics.Writer(ctx, e.ExternalStorage, w)
	return w
}

func (e *esWrapper) ReadFile(
	ctx context.Context, basename string, opts ReadOptions,
) (ioctx.ReadCloserCtx, int64, error) {
	if e.httpTracer != nil {
		ctx = httptrace.WithClientTrace(ctx, e.httpTracer)
	}
	r, s, err := e.ExternalStorage.ReadFile(ctx, basename, opts)
	if err != nil {
		return r, s, err
	}

	return e.wrapReader(ctx, r), s, nil
}

func (e *esWrapper) List(ctx context.Context, prefix, delimiter string, fn ListingFn) error {
	if e.httpTracer != nil {
		ctx = httptrace.WithClientTrace(ctx, e.httpTracer)
	}

	countingFn := fn
	if e.metrics != nil {
		e.metrics.Listings.Inc(1)
		countingFn = func(s string) error {
			e.metrics.ListingResults.Inc(1)
			return fn(s)
		}
	}
	return e.ExternalStorage.List(ctx, prefix, delimiter, countingFn)
}

func (e *esWrapper) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	if e.httpTracer != nil {
		ctx = httptrace.WithClientTrace(ctx, e.httpTracer)
	}

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

// makeExternalStorage creates an ExternalStorage from the given config.
func makeExternalStorage[T interface {
	ExternalStorageContext | EarlyBootExternalStorageContext
}](
	ctx context.Context,
	dest cloudpb.ExternalStorage,
	conf base.ExternalIODirConfig,
	limiters Limiters,
	metrics metric.Struct,
	settings *cluster.Settings,
	args T,
	getImpl func(cloudpb.ExternalStorageProvider) (func(context.Context, T, cloudpb.ExternalStorage) (ExternalStorage, error), bool),
	opts ...ExternalStorageOption,
) (ExternalStorage, error) {
	var cloudMetrics *Metrics
	var ok bool
	if cloudMetrics, ok = metrics.(*Metrics); !ok {
		return nil, errors.Newf("invalid metrics type: %T", metrics)
	}
	if conf.DisableOutbound && dest.Provider != cloudpb.ExternalStorageProvider_userfile {
		return nil, errors.New("external network access is disabled")
	}
	options := ExternalStorageOptions{}
	for _, o := range opts {
		o(&options)
	}
	if fn, ok := getImpl(dest.Provider); ok {
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

		var httpTracer *httptrace.ClientTrace
		if cloudMetrics != nil && httpMetrics.Get(&settings.SV) {
			httpTracer = &httptrace.ClientTrace{
				GotConn: func(info httptrace.GotConnInfo) {
					if info.Reused {
						cloudMetrics.ConnsReused.Inc(1)
					} else {
						cloudMetrics.ConnsOpened.Inc(1)
					}
				},
				TLSHandshakeDone: func(_ tls.ConnectionState, _ error) {
					cloudMetrics.TLSHandhakes.Inc(1)
				},
			}
		}

		return &esWrapper{
			ExternalStorage: e,
			lim:             limiters[dest.Provider],
			ioRecorder:      options.ioAccountingInterceptor,
			metrics:         cloudMetrics,
			httpTracer:      httpTracer,
		}, nil
	}

	return nil, errors.Errorf("unsupported external destination type: %s", dest.Provider.String())
}

// ReplaceProviderForTesting replaces an existing registered provider
// with the given alternate implementation.
//
// The returned func restores the prooduction implemenation.
func ReplaceProviderForTesting(
	providerType cloudpb.ExternalStorageProvider, provider RegisteredProvider,
) func() {
	if len(provider.Schemes) != 1 {
		panic("expected 1 scheme")
	}
	scheme := provider.Schemes[0]

	remove := func() {
		delete(implementations, providerType)
		delete(earlyBootImplementations, providerType)

		delete(confParsers, scheme)
		delete(earlyBootConfParsers, scheme)
	}

	oldImpl := implementations[providerType]
	oldParser := confParsers[scheme]
	oldEarlyImpl := earlyBootImplementations[providerType]
	oldEaryParser := earlyBootConfParsers[scheme]

	remove()
	registerExternalStorageProviderImpls(providerType, provider)

	return func() {
		remove()
		if oldImpl != nil {
			implementations[providerType] = oldImpl
		}
		if oldParser != nil {
			confParsers[scheme] = oldParser
		}
		if oldEarlyImpl != nil {
			earlyBootImplementations[providerType] = oldEarlyImpl
		}
		if oldEaryParser != nil {
			earlyBootConfParsers[scheme] = oldEaryParser
		}
	}
}

func RegisterRedactedParams(params map[string]struct{}) {
	for param := range params {
		redactedQueryParams[param] = struct{}{}
	}
}
