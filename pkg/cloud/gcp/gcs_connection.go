// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcp

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/utils"
	"github.com/cockroachdb/errors"
)

func validateGCSConnectionURI(
	ctx context.Context, env externalconn.ExternalConnEnv, uri string,
) error {
	if err := utils.CheckExternalStorageConnection(ctx, env, uri); err != nil {
		return errors.Wrap(err, "failed to create gs external connection")
	}

	return nil
}

func init() {
	externalconn.RegisterConnectionDetailsFromURIFactory(
		gcsScheme,
		connectionpb.ConnectionProvider_gs,
		externalconn.SimpleURIFactory,
	)

	externalconn.RegisterDefaultValidation(gcsScheme, validateGCSConnectionURI)
}
