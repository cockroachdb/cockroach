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
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
)

func parseAndValidateS3ConnectionURI(
	_ context.Context, uri *url.URL,
) (externalconn.ExternalConnection, error) {
	// Parse and validate the S3 URL.
	if _, err := parseS3URL(cloud.ExternalStorageURIContext{}, uri); err != nil {
		return nil, err
	}

	connDetails := connectionpb.ConnectionDetails{
		Provider: connectionpb.ConnectionProvider_s3,
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
		scheme,
		parseAndValidateS3ConnectionURI,
	)
}
