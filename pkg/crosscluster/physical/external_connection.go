// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
)

// The canonical PostgreSQL URL scheme is "postgresql", however our
// own client commands also accept "postgres".
func postgresSchemes() [2]string {
	return [2]string{"postgres", "postgresql"}
}

func validatePostgresConnectionURI(
	ctx context.Context, env externalconn.ExternalConnEnv, uri string,
) error {
	parsedURI, err := streamclient.ParseClusterUri(uri)
	if err != nil {
		return err
	}
	conn, err := streamclient.NewPartitionedStreamClient(ctx, parsedURI)
	if err != nil {
		return err
	}
	return conn.Close(ctx)
}

func init() {
	for _, scheme := range postgresSchemes() {
		externalconn.RegisterConnectionDetailsFromURIFactory(
			scheme,
			connectionpb.ConnectionProvider_sql,
			externalconn.SimpleURIFactory,
		)
		cloud.RegisterRedactedParams(cloud.RedactedParams(streamclient.SslInlineURLParam))
		externalconn.RegisterDefaultValidation(scheme, validatePostgresConnectionURI)
	}
}
