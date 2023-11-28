// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nodelocal

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
)

// ReplaceNodeLocalForTesting replaces the implementation of of
// nodelocal with one reads and writes directly from the given
// directory.
//
// The returned func restores the prooduction implemenation.
func ReplaceNodeLocalForTesting(root string) func() {
	makeFn := func(ctx context.Context, conf cloud.ExternalStorageContext, es cloudpb.ExternalStorage) (cloud.ExternalStorage, error) {
		if !buildutil.CrdbTestBuild {
			panic("nodelocal test implementation in non-test build")
		}
		c, err := blobs.NewLocalClient(root)
		if err != nil {
			return nil, err
		}
		lfs := &localFileStorage{
			cfg:        es.LocalFileConfig,
			ioConf:     base.ExternalIODirConfig{},
			base:       es.LocalFileConfig.Path,
			blobClient: c,
			settings:   conf.Settings,
		}
		return lfs, nil
	}

	parserFn := func(_ cloud.ExternalStorageURIContext, uri *url.URL) (cloudpb.ExternalStorage, error) {
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
	return cloud.ReplaceProviderForTesting(cloudpb.ExternalStorageProvider_nodelocal, parserFn, makeFn, cloud.RedactedParams(), scheme)
}
