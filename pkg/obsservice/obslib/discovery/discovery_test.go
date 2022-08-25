// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package discovery

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestListTargets(t *testing.T) {
	// test setup
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, tc.Server(0).ServingSQLAddr(),
		"TestListTargets", url.User(username.RootUser),
	)
	defer cleanupFunc()

	// test target setup
	config, err := pgx.ParseConfig(pgURL.String())
	require.NoError(t, err)
	config.Database = "defaultdb"
	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer conn.Close(ctx)

	d := MakeGossipTargetDiscovery(conn, Config{})
	targets, err := d.list(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, targets)
	require.Equal(t, tc.Server(0).NodeID(), targets[0].nodeID)
	require.Equal(t, tc.Server(0).SQLAddr(), targets[0].pgUrl)
	require.Equal(t, true, targets[0].active)
}
