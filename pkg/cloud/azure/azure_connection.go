// Copyright 2022 The Cockroach Authors.
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

func validateAzureConnectionURI(
	ctx context.Context, env externalconn.ExternalConnEnv, uri string,
) error {
	if err := utils.CheckExternalStorageConnection(ctx, env, uri); err != nil {
		return errors.Wrap(err, "failed to create azure external connection")
	}

	return nil
}

func init() {
	for _, s := range []string{scheme, deprecatedScheme, deprecatedExternalConnectionScheme} {
		externalconn.RegisterConnectionDetailsFromURIFactory(
			s,
			connectionpb.ConnectionProvider_azure_storage,
			externalconn.SimpleURIFactory,
		)

		externalconn.RegisterDefaultValidation(s, validateAzureConnectionURI)
	}
}
