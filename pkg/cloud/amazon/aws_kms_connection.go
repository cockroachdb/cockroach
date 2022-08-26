// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package amazon

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/utils"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/errors"
)

func validateAWSKMSConnectionURI(
	ctx context.Context, execCfg interface{}, user username.SQLUsername, uri string,
) error {
	if err := utils.CheckKMSConnection(ctx, execCfg, user, uri); err != nil {
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
