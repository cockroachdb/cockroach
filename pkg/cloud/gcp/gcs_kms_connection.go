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
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func parseGCSKMSConnectionURI(
	_ context.Context, uri *url.URL,
) (connectionpb.ConnectionDetails, error) {
	kmsURIParams := resolveKMSURIParams(*uri)
	kmsConnDetails := connectionpb.GCSKMSConnectionDetails{
		CMK:           uri.Path,
		Credentials:   kmsURIParams.credentials,
		Auth:          kmsURIParams.auth,
		AssumeRole:    kmsURIParams.assumeRole,
		DelegateRoles: kmsURIParams.delegateRoles,
		BearerToken:   kmsURIParams.bearerToken,
	}
	connDetails := connectionpb.ConnectionDetails{
		Provider: connectionpb.ConnectionProvider_gs_kms,
		Details: &connectionpb.ConnectionDetails_GCSKMS{
			GCSKMS: &kmsConnDetails,
		},
	}
	return connDetails, nil
}

type gcsKMSConnectionDetails struct {
	connectionpb.ConnectionDetails
}

func constructKMSURI(details *connectionpb.GCSKMSConnectionDetails) url.URL {
	uri := url.URL{Scheme: gcsScheme}
	uri.Path = details.CMK
	values := uri.Query()
	if details.Credentials != "" {
		values.Set(CredentialsParam, details.Credentials)
	}
	if details.Auth != "" {
		values.Set(cloud.AuthParam, details.Auth)
	}
	if details.BearerToken != "" {
		values.Set(BearerTokenParam, details.BearerToken)
	}
	roles := cloud.ConstructRoleString(details.AssumeRole, details.DelegateRoles)
	if roles != "" {
		values.Set(AssumeRoleParam, roles)
	}
	uri.RawQuery = values.Encode()
	return uri
}

// Dial implements the ConnectionDetails interface.
func (g *gcsKMSConnectionDetails) Dial(
	ctx context.Context, connectionCtx externalconn.ConnectionContext, subdir string,
) (externalconn.Connection, error) {
	env := connectionCtx.KMSEnv()
	kmsURI := constructKMSURI(g.GetGCSKMS())
	log.Infof(ctx, "dialing URI %s", kmsURI.String())
	return cloud.KMSFromURI(ctx, kmsURI.String(), env)
}

// ConnectionProto implements the ConnectionDetails interface.
func (g *gcsKMSConnectionDetails) ConnectionProto() *connectionpb.ConnectionDetails {
	return &g.ConnectionDetails
}

// ConnectionType implements the ConnectionDetails interface.
func (g *gcsKMSConnectionDetails) ConnectionType() connectionpb.ConnectionType {
	return connectionpb.TypeKMS
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
