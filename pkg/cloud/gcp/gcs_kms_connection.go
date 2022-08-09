// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcp

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
)

func parseAndValidateGCSKMSConnectionURI(
	_ context.Context, _ interface{}, _ username.SQLUsername, uri *url.URL,
) (externalconn.ExternalConnection, error) {
	if err := ValidateKMSURI(*uri); err != nil {
		return nil, err
	}

	connDetails := connectionpb.ConnectionDetails{
		Provider: connectionpb.ConnectionProvider_gs_kms,
		Details: &connectionpb.ConnectionDetails_SimpleURI{
			SimpleURI: &connectionpb.SimpleURI{
				URI: uri.String(),
			},
		},
	}

	return externalconn.NewExternalConnection(connDetails), nil
}

func init() {
	externalconn.RegisterConnectionDetailsFromURIFactory(
		gcsScheme,
		parseAndValidateGCSKMSConnectionURI,
	)
}
