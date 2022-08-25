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
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/pgurl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestTargetRegistryWithGossipTargetDiscovery(t *testing.T) {
	// test setup
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, 4, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Insecure: true,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: stickyEngineRegistry,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	pgURL, _ := pgUrl(tc.Server(1).ServingSQLAddr(), pgurl.TransportNone())

	// test target setup
	config, err := pgx.ParseConfig(pgURL)
	require.NoError(t, err)
	config.Database = "defaultdb"
	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer conn.Close(ctx)

	var targetsAdded []Target
	var targetsRemoved []Target

	d := MakeGossipTargetDiscovery(conn, Config{
		InitPGUrl:    pgURL,
		TransportOpt: pgurl.TransportNone(),
	})
	callbackC := make(chan struct{})
	r := NewTargetRegistry(&d, tc.Stopper(), func(targetsToAdd []Target, targetsToRemove []Target) {
		targetsAdded = targetsToAdd
		targetsRemoved = targetsToRemove
		callbackC <- struct{}{}
	})
	r.pollingInterval = 1000 * time.Millisecond
	r.Start(ctx)

	// Verify that the callback is called.
	testutils.SucceedsSoon(t, func() error {
		<-callbackC
		return nil
	})
	require.NotEmpty(t, r.targetsByNodeID)
	require.Len(t, r.targetsByNodeID, 4)
	require.Len(t, targetsAdded, 4)
	require.Len(t, targetsRemoved, 0)

	// shutdown server
	tc.StopServer(0)

	// Verify that the callback is called.
	testutils.SucceedsSoon(t, func() error {
		<-callbackC
		return nil
	})
	require.NotEmpty(t, r.targetsByNodeID)
	require.Len(t, r.targetsByNodeID, 4)
	require.Len(t, targetsAdded, 0)
	require.Len(t, targetsRemoved, 1)

	// start same server back up
	require.NoError(t, tc.RestartServer(0))

	// Verify that the callback is called.
	testutils.SucceedsSoon(t, func() error {
		<-callbackC
		return nil
	})
	require.NotEmpty(t, r.targetsByNodeID)
	require.Len(t, r.targetsByNodeID, 4)
	require.Len(t, targetsAdded, 1)
	require.Len(t, targetsRemoved, 0)
}
