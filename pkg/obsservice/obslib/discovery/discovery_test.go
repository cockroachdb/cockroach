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
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, s.ServingSQLAddr(),
		"TestListTargets", url.User(username.RootUser),
	)
	defer cleanupFunc()

	// test target setup
	config, err := pgxpool.ParseConfig(pgURL.String())
	require.NoError(t, err)
	config.ConnConfig.Database = "defaultdb"
	pool, err := pgxpool.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer pool.Close()

	d := NewCRDBTargetDiscovery(pool)
	targets, err := d.List(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, targets)
	require.Equal(t, s.NodeID(), targets[0].nodeID)
	require.Equal(t, s.RPCAddr(), targets[0].address)
	require.Equal(t, true, targets[0].active)
	fmt.Printf("%s", targets[0].PGURLString())
}
