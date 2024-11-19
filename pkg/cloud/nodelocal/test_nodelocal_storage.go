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
	}
}
