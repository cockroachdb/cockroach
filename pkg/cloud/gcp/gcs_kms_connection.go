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

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
)

func parseGCSKMSConnectionURI(
	_ context.Context, uri *url.URL,
) (connectionpb.ConnectionDetails, error) {
	if err := validateKMSURI(*uri); err != nil {
		return connectionpb.ConnectionDetails{}, err
	}

	connDetails := connectionpb.ConnectionDetails{
		Provider: connectionpb.ConnectionProvider_gs_kms,
		Details: &connectionpb.ConnectionDetails_GCSKMS{
			GCSKMS: &connectionpb.GCSKMSConnectionDetails{URI: uri.String()},
		},
	}
	return connDetails, nil
}

type gcsKMSConnectionDetails struct {
	connectionpb.ConnectionDetails
}

// Dial implements the ConnectionDetails interface.
func (g *gcsKMSConnectionDetails) Dial(
	ctx context.Context, connectionCtx externalconn.ConnectionContext, subdir string,
) (externalconn.Connection, error) {
	env := connectionCtx.KMSEnv()
	return cloud.KMSFromURI(ctx, g.GetGCSKMS().URI, env)
}

// ConnectionProto implements the ConnectionDetails interface.
func (g *gcsKMSConnectionDetails) ConnectionProto() *connectionpb.ConnectionDetails {
	return &g.ConnectionDetails
}

// ConnectionType implements the ConnectionDetails interface.
func (g *gcsKMSConnectionDetails) ConnectionType() connectionpb.ConnectionType {
	return g.ConnectionDetails.Type()
}

func makeGCSKMSConnectionDetails(
	_ context.Context, details connectionpb.ConnectionDetails,
) externalconn.ConnectionDetails {
	return &gcsKMSConnectionDetails{ConnectionDetails: details}
}

func init() {
	externalconn.RegisterConnectionDetailsFromURIFactory(
		connectionpb.ConnectionProvider_gs_kms,
		gcsScheme,
		parseGCSKMSConnectionURI,
		makeGCSKMSConnectionDetails,
	)
}
