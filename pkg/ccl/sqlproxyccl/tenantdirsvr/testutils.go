// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantdirsvr

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/tenant"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// SetupTestDirectory returns an instance of the directory cache and the
// in-memory test static directory server. Tenants will need to be added/removed
// manually.
func SetupTestDirectory(
	t *testing.T,
	ctx context.Context,
	stopper *stop.Stopper,
	timeSource timeutil.TimeSource,
	opts ...tenant.DirOption,
) (tenant.DirectoryCache, *TestStaticDirectoryServer) {
	t.Helper()

	// Start an in-memory static directory server.
	directoryServer := NewTestStaticDirectoryServer(stopper, timeSource)
	require.NoError(t, directoryServer.Start(ctx))

	// Dial the test directory server.
	conn, err := grpc.DialContext(
		ctx,
		"",
		grpc.WithContextDialer(directoryServer.DialerFunc),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	stopper.AddCloser(stop.CloserFn(func() {
		_ = conn.Close() // nolint:grpcconnclose
	}))
	client := tenant.NewDirectoryClient(conn)
	directoryCache, err := tenant.NewDirectoryCache(ctx, stopper, client, opts...)
	require.NoError(t, err)

	return directoryCache, directoryServer
}
