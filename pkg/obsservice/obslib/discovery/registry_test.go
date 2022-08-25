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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
	"net/url"
	"sync/atomic"
	"testing"
)

func TestTargetRegistryWithCRDBTargetDiscovery(t *testing.T) {
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

	var callbackTriggered atomic.Bool
	var targetsAdded atomic.Int32
	var targetsRemoved atomic.Int32
	d := NewCRDBTargetDiscovery(pool)
	r := NewTargetRegistry(d, s.Stopper(), func(targetsToAdd []Target, targetsToRemove []Target) {
		callbackTriggered.Store(true)
		targetsAdded.Store(int32(len(targetsToAdd)))
		targetsRemoved.Store(int32(len(targetsToRemove)))
	})
	r.Start(ctx)

	testutils.SucceedsSoon(t, func() error {
		if !callbackTriggered.Load() {
			return errors.Errorf("TargetRegistry should trigger callback")
		}
		if targetsAdded.Load() == 0 {
			return errors.Errorf("TargetRegistry should add targets")
		}
		if targetsRemoved.Load() != 0 {
			return errors.Errorf("TargetRegistry should not remove targets")
		}
		return nil
	})

	r.mu.Lock()
	require.NotEmpty(t, r.mu.targetsByID)
	require.Equal(t, 1, len(r.mu.targetsByID))
	r.mu.Unlock()
}
