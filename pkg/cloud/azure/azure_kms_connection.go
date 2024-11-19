// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/utils"
	"github.com/cockroachdb/errors"
)

func validateAzureKMSConnectionURI(
	ctx context.Context, execCfg externalconn.ExternalConnEnv, uri string,
) error {
	if err := utils.CheckKMSConnection(ctx, execCfg, uri); err != nil {
		return errors.Wrap(err, "failed to create Azure KMS external connection")
	}

	return nil
}

func init() {
	externalconn.RegisterConnectionDetailsFromURIFactory(
		kmsScheme,
		connectionpb.ConnectionProvider_azure_kms,
		externalconn.SimpleURIFactory,
	)
	externalconn.RegisterDefaultValidation(
		kmsScheme,
		validateAzureKMSConnectionURI,
	)
}
