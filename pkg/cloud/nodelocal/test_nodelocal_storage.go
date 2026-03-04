// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nodelocal

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
)

// ReplaceNodeLocalForTesting replaces the implementation of of
// nodelocal with one reads and writes directly from the given
// directory.
//
// The returned func restores the prooduction implementation.
func ReplaceNodeLocalForTesting(root string) func() {
	makeFn := func(ctx context.Context, conf cloud.EarlyBootExternalStorageContext, es cloudpb.ExternalStorage) (cloud.ExternalStorage, error) {
		return TestingMakeNodelocalStorage(root, conf.Settings, es), nil
	}
	parserFn := func(uri *url.URL) (cloudpb.ExternalStorage, error) {
		if !buildutil.CrdbTestBuild {
			panic("nodelocal test implementation in non-test build")
		}

		return cloudpb.ExternalStorage{
			Provider: cloudpb.ExternalStorageProvider_nodelocal,
			LocalFileConfig: cloudpb.ExternalStorage_LocalFileConfig{
				Path: uri.Path,
			},
			URI: uri.String(),
		}, nil
	}
	return cloud.ReplaceProviderForTesting(cloudpb.ExternalStorageProvider_nodelocal, cloud.RegisteredProvider{
		EarlyBootConstructFn: makeFn,
		EarlyBootParseFn:     parserFn,
		Schemes:              []string{scheme},
	})
}

// TestingMakeNodelocalStorage constructs a nodelocal storage for use in tests.
func TestingMakeNodelocalStorage(
	root string, settings *cluster.Settings, es cloudpb.ExternalStorage,
) cloud.ExternalStorage {
	if !buildutil.CrdbTestBuild {
		panic("nodelocal test implementation in non-test build")
	}
	c, err := blobs.NewLocalClient(root)
	if err != nil {
		panic(err)
	}
	return &localFileStorage{
		cfg:        es.LocalFileConfig,
		ioConf:     base.ExternalIODirConfig{},
		base:       es.LocalFileConfig.Path,
		blobClient: c,
		settings:   settings,
		uri:        es.URI,
	}
}

// ReplaceNodeLocalForTestingWithInterceptor is like ReplaceNodeLocalForTesting
// but calls interceptRead before each ReadFile call. The interceptor receives
// the basename of the file being read. This can be used to inject blocking or
// delays for specific files, e.g. to block reads of data SSTs during online
// restore testing. Size calls are not intercepted so that Pebble can still
// query external file sizes for disk usage accounting.
func ReplaceNodeLocalForTestingWithInterceptor(
	root string, interceptRead func(context.Context, string),
) func() {
	makeFn := func(ctx context.Context, conf cloud.EarlyBootExternalStorageContext, es cloudpb.ExternalStorage) (cloud.ExternalStorage, error) {
		inner := TestingMakeNodelocalStorage(root, conf.Settings, es)
		return &interceptingStorage{ExternalStorage: inner, interceptRead: interceptRead}, nil
	}
	parserFn := func(uri *url.URL) (cloudpb.ExternalStorage, error) {
		if !buildutil.CrdbTestBuild {
			panic("nodelocal test implementation in non-test build")
		}
		return cloudpb.ExternalStorage{
			Provider: cloudpb.ExternalStorageProvider_nodelocal,
			LocalFileConfig: cloudpb.ExternalStorage_LocalFileConfig{
				Path: uri.Path,
			},
			URI: uri.String(),
		}, nil
	}
	return cloud.ReplaceProviderForTesting(cloudpb.ExternalStorageProvider_nodelocal, cloud.RegisteredProvider{
		EarlyBootConstructFn: makeFn,
		EarlyBootParseFn:     parserFn,
		Schemes:              []string{scheme},
	})
}

// interceptingStorage wraps a cloud.ExternalStorage and calls an interceptor
// before ReadFile operations.
type interceptingStorage struct {
	cloud.ExternalStorage
	interceptRead func(context.Context, string)
}

func (s *interceptingStorage) ReadFile(
	ctx context.Context, basename string, opts cloud.ReadOptions,
) (ioctx.ReadCloserCtx, int64, error) {
	s.interceptRead(ctx, basename)
	return s.ExternalStorage.ReadFile(ctx, basename, opts)
}
