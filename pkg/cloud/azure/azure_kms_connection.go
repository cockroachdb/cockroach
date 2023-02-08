// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
		scheme,
		connectionpb.ConnectionProvider_azure_kms,
		externalconn.SimpleURIFactory,
	)
	externalconn.RegisterDefaultValidation(
		scheme,
		validateAzureKMSConnectionURI,
	)
}
