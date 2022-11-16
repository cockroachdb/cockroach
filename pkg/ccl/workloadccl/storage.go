// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package workloadccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	// Import all the cloud provider storage we care about.
	_ "github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/azure"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/gcp"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
)

const storageError = `failed to create google cloud client ` +
	`(You may need to setup the GCS application default credentials: ` +
	`'gcloud auth application-default login --project=cockroach-shared')`

// GetStorage returns a cloud storage implementation
// The caller is responsible for closing it.
func GetStorage(ctx context.Context, cfg FixtureConfig) (cloud.ExternalStorage, error) {
	switch cfg.StorageProvider {
	case "gs", "s3", "azure":
	default:
		return nil, errors.AssertionFailedf("unsupported external storage provider; valid providers are gs, s3, and azure")
	}

	s, err := cloud.ExternalStorageFromURI(
		ctx,
		cfg.ObjectPathToURI(),
		base.ExternalIODirConfig{},
		clustersettings.MakeClusterSettings(),
		nil, /* blobClientFactory */
		username.SQLUsername{},
		nil,              /* db */
		nil,              /* limiters */
		cloud.NilMetrics, /* metrics */
	)
	if err != nil {
		return nil, errors.Wrap(err, storageError)
	}
	return s, nil
}
