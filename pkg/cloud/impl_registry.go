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
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
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
	opts ...ExternalStorageOption,
) (ExternalStorage, error) {
	conf, err := ExternalStorageConfFromURI(uri, user)
	if err != nil {
		return nil, err
	}
	return MakeExternalStorage(ctx, conf, externalConfig, settings, blobClientFactory, ie, kvDB, opts...)
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
	opts ...ExternalStorageOption,
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
	for _, o := range opts {
		o(&args)
	}
	if fn, ok := implementations[dest.Provider]; ok {
		es, err := fn(ctx, args, dest)
		if err != nil {
			return nil, err
		}
		if args.rwInterceptor != nil {
			return &rwInterceptingStorage{
				ExternalStorage: es,
				rwInterceptor:   args.rwInterceptor,
			}, nil
		}
		return es, nil
	}

	return nil, errors.Errorf("unsupported external destination type: %s", dest.Provider.String())
}

// A ReadWriterInterceptor providers methods that construct Readers and Writers from given Readers
// and Writers.
type ReadWriterInterceptor interface {
	Reader(context.Context, ExternalStorage, ioctx.ReadCloserCtx) ioctx.ReadCloserCtx
	Writer(context.Context, ExternalStorage, io.WriteCloser) io.WriteCloser
}

// rwInterceptingStorage is an ExternalStorage implementation that wraps an ExternalStorage,
// installing the ReadWriteInterceptor into any returned Reader or Writer.
type rwInterceptingStorage struct {
	ExternalStorage
	rwInterceptor ReadWriterInterceptor
}

func (r *rwInterceptingStorage) Writer(
	ctx context.Context, basename string,
) (io.WriteCloser, error) {
	inner, err := r.ExternalStorage.Writer(ctx, basename)
	if err != nil {
		return nil, err
	}
	return r.rwInterceptor.Writer(ctx, r.ExternalStorage, inner), nil
}

func (r *rwInterceptingStorage) ReadFile(
	ctx context.Context, basename string,
) (ioctx.ReadCloserCtx, error) {
	inner, err := r.ExternalStorage.ReadFile(ctx, basename)
	if err != nil {
		return nil, err
	}
	return r.rwInterceptor.Reader(ctx, r.ExternalStorage, inner), nil
}

func (r *rwInterceptingStorage) ReadFileAt(
	ctx context.Context, basename string, offset int64,
) (ioctx.ReadCloserCtx, int64, error) {
	inner, sz, err := r.ExternalStorage.ReadFileAt(ctx, basename, offset)
	if err != nil {
		return nil, 0, err
	}
	return r.rwInterceptor.Reader(ctx, r.ExternalStorage, inner), sz, nil
}
