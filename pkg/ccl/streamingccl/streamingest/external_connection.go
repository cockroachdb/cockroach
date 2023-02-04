// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"net/url"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
)

// The canonical PostgreSQL URL scheme is "postgresql", however our
// own client commands also accept "postgres".
func postgresSchemes() [2]string {
	return [2]string{"postgres", "postgresql"}
}

func validatePostgresConnectionURI(
	ctx context.Context, env externalconn.ExternalConnEnv, uri string,
) error {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return err
	}
	conn, err := streamclient.NewPartitionedStreamClient(ctx, parsedURI)
	if err != nil {
		return err
	}
	if err = conn.Dial(ctx); err != nil {
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

		externalconn.RegisterDefaultValidation(scheme, validatePostgresConnectionURI)
	}

}
