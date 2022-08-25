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
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestListTargets(t *testing.T) {
	// test setup
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 2, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Insecure: true,
		},
	})
	defer tc.Stopper().Stop(ctx)

	pgURL, err := pgUrl(tc.Server(0).ServingSQLAddr(), pgurl.TransportNone())
	require.NoError(t, err)

	// test target setup
	config, err := pgx.ParseConfig(pgURL)
	require.NoError(t, err)
	config.Database = "defaultdb"
	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer conn.Close(ctx)

	d := MakeGossipTargetDiscovery(conn, Config{
		InitPGUrl:    pgURL,
		TransportOpt: pgurl.TransportNone(),
	})
	targets, err := d.list(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, targets)
	require.Len(t, targets, 2)

	// Node 1
	require.Equal(t, tc.Server(0).NodeID(), targets[0].NodeID)
	require.Equal(t, tc.Server(0).ServingRPCAddr(), targets[0].RpcAddr)
	pgUrl1, _ := pgUrl(tc.Server(0).ServingSQLAddr(), pgurl.TransportNone())
	require.Equal(t, pgUrl1, targets[0].PGUrl)
	require.Equal(t, true, targets[0].Active)

	// Node 2
	require.Equal(t, tc.Server(1).NodeID(), targets[1].NodeID)
	require.Equal(t, tc.Server(1).ServingRPCAddr(), targets[1].RpcAddr)
	pgUrl2, _ := pgUrl(tc.Server(1).ServingSQLAddr(), pgurl.TransportNone())
	require.Equal(t, pgUrl2, targets[1].PGUrl)
	require.Equal(t, true, targets[1].Active)

	// Try connecting to Node 2 to verify the addr is parsed correctly.
	config2, err := pgx.ParseConfig(targets[1].PGUrl)
	require.NoError(t, err)
	config2.Database = "defaultdb"
	conn2, err := pgx.ConnectConfig(ctx, config2)
	require.NoError(t, err)
	defer conn2.Close(ctx)
}
