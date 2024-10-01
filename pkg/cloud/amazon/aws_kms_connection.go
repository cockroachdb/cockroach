// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package amazon

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/utils"
	"github.com/cockroachdb/errors"
)

func validateAWSKMSConnectionURI(
	ctx context.Context, env externalconn.ExternalConnEnv, uri string,
) error {
	if err := utils.CheckKMSConnection(ctx, env, uri); err != nil {
		return errors.Wrap(err, "failed to create AWS KMS external connection")
	}
	return nil
}

func init() {
	externalconn.RegisterConnectionDetailsFromURIFactory(
		awsKMSScheme,
		connectionpb.ConnectionProvider_aws_kms,
		externalconn.SimpleURIFactory,
	)

	externalconn.RegisterDefaultValidation(awsKMSScheme, validateAWSKMSConnectionURI)
}
