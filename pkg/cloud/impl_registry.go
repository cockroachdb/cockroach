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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
var implementations = map[roachpb.ExternalStorageProvider]ExternalStorageConstructor{}

type rateAndBurstSettings struct {
	rate  *settings.ByteSizeSetting
	burst *settings.ByteSizeSetting
}

var limiterSettings = map[roachpb.ExternalStorageProvider]rateAndBurstSettings{}

// RegisterExternalStorageProvider registers an external storage provider for a
// given URI scheme and provider type.
func RegisterExternalStorageProvider(
	providerType roachpb.ExternalStorageProvider,
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

	rateName := fmt.Sprintf("cloudstorage.%s.write.node_rate_limit", strings.ToLower(providerType.String()))
	burstName := fmt.Sprintf("cloudstorage.%s.write.node_burst_limit", strings.ToLower(providerType.String()))
	limiterSettings[providerType] = rateAndBurstSettings{
		rate: settings.RegisterByteSizeSetting(settings.TenantWritable, rateName,
			"limit on number of bytes per second per node across operations writing to the designated cloud storage provider if non-zero",
			0, settings.NonNegativeInt,
		),
		burst: settings.RegisterByteSizeSetting(settings.TenantWritable, burstName,
			"burst limit on number of bytes per second per node across operations writing to the designated cloud storage provider if non-zero",
			0, settings.NonNegativeInt,
		),
	}
}

// ExternalStorageConfFromURI generates an ExternalStorage config from a URI string.
func ExternalStorageConfFromURI(
	path string, user security.SQLUsername,
) (roachpb.ExternalStorage, error) {
	uri, err := url.Parse(path)
	if err != nil {
		return roachpb.ExternalStorage{}, err
	}
	if fn, ok := confParsers[uri.Scheme]; ok {
		return fn(ExternalStorageURIContext{CurrentUser: user}, uri)
	}
	// TODO(adityamaru): Link dedicated ExternalStorage scheme docs once ready.
	return roachpb.ExternalStorage{}, errors.Errorf("unsupported storage scheme: %q - refer to docs to find supported"+
		" storage schemes", uri.Scheme)
}

// ExternalStorageFromURI returns an ExternalStorage for the given URI.
func ExternalStorageFromURI(
	ctx context.Context,
	uri string,
	externalConfig base.ExternalIODirConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
	user security.SQLUsername,
	ie sqlutil.InternalExecutor,
	kvDB *kv.DB,
	limiters Limiters,
) (ExternalStorage, error) {
	conf, err := ExternalStorageConfFromURI(uri, user)
	if err != nil {
		return nil, err
	}
	return MakeExternalStorage(ctx, conf, externalConfig, settings, blobClientFactory, ie, kvDB, limiters)
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
	dest roachpb.ExternalStorage,
	conf base.ExternalIODirConfig,
	settings *cluster.Settings,
	blobClientFactory blobs.BlobClientFactory,
	ie sqlutil.InternalExecutor,
	kvDB *kv.DB,
	limiters Limiters,
) (ExternalStorage, error) {
	args := ExternalStorageContext{
		IOConf:            conf,
		Settings:          settings,
		BlobClientFactory: blobClientFactory,
		InternalExecutor:  ie,
		DB:                kvDB,
	}
	if conf.DisableOutbound && dest.Provider != roachpb.ExternalStorageProvider_userfile {
		return nil, errors.New("external network access is disabled")
	}
	if fn, ok := implementations[dest.Provider]; ok {
		e, err := fn(ctx, args, dest)
		if err != nil {
			return nil, err
		}

		return &limitWrapper{ExternalStorage: e, lim: limiters[dest.Provider]}, nil
	}
	return nil, errors.Errorf("unsupported external destination type: %s", dest.Provider.String())
}

type Limiters map[roachpb.ExternalStorageProvider]*quotapool.RateLimiter

func MakeLimiters(ctx context.Context, sv *settings.Values) Limiters {
	m := make(map[roachpb.ExternalStorageProvider]*quotapool.RateLimiter, len(limiterSettings))
	for k := range limiterSettings {
		l := limiterSettings[k]
		lim := quotapool.NewRateLimiter(l.rate.Key(), quotapool.Limit(0), 0)
		fn := func(ctx context.Context) {
			rate := quotapool.Limit(l.rate.Get(sv))
			if rate == 0 {
				rate = quotapool.Limit(math.MaxInt64)
			}
			burst := l.burst.Get(sv)
			if burst == 0 {
				burst = math.MaxInt64
			}
			lim.UpdateLimit(rate, burst)
		}
		l.rate.SetOnChange(sv, fn)
		l.burst.SetOnChange(sv, fn)
		fn(ctx)
		m[k] = lim
	}
	return m
}

type limitWrapper struct {
	ExternalStorage
	lim *quotapool.RateLimiter
}

func (l *limitWrapper) Writer(ctx context.Context, basename string) (io.WriteCloser, error) {
	w, err := l.ExternalStorage.Writer(ctx, basename)
	if err != nil {
		return nil, err
	}

	return &limitedWriter{w: w, ctx: ctx, lim: l.lim}, nil
}

type limitedWriter struct {
	w    io.WriteCloser
	ctx  context.Context
	lim  *quotapool.RateLimiter
	pool int64
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	l.pool += int64(len(p))
	if l.pool > 128<<10 {
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
	return nil
}
