// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestNodeJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tempDir, cleanup := testutils.TempDir(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	settings := cluster.MakeTestingClusterSettings()
	sql.FeatureTLSAutoJoinEnabled.Override(ctx, &settings.SV, true)
	s, sqldb, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
	})
	defer s.Stopper().Stop(ctx)

	rows, err := sqldb.Query("SELECT crdb_internal.create_join_token();")
	require.NoError(t, err)
	var token string
	for rows.Next() {
		require.NoError(t, rows.Scan(&token))
		require.NotEmpty(t, token)
	}

	oldCfg := *baseCfg
	defer func() {
		*baseCfg = oldCfg
	}()
	sslCertsDir := filepath.Join(tempDir, "certs")
	baseCfg.SSLCertsDir = sslCertsDir
	serverCfg.JoinList = []string{s.ServingRPCAddr()}
	baseCfg.Addr = "127.0.0.1:0"
	baseCfg.AdvertiseAddr = baseCfg.Addr

	err = runConnectJoin(nil, []string{token})
	require.NoError(t, err)

	// Ensure the SSLCertsDir is non-empty.
	f, err := os.Open(sslCertsDir)
	require.NoError(t, err)
	_, err = f.Readdirnames(1)
	// An error is returned if the directory is empty.
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(sslCertsDir, "ca.crt"))
	require.NoError(t, err)
}

func TestNodeJoinBadToken(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tempDir, cleanup := testutils.TempDir(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	settings := cluster.MakeTestingClusterSettings()
	sql.FeatureTLSAutoJoinEnabled.Override(ctx, &settings.SV, true)
	s, sqldb, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: settings,
	})
	defer s.Stopper().Stop(ctx)

	rows, err := sqldb.Query("SELECT crdb_internal.create_join_token();")
	require.NoError(t, err)
	var token string
	for rows.Next() {
		require.NoError(t, rows.Scan(&token))
		require.NotEmpty(t, token)
	}
	// Rewrite token to something else entirely.
	token = "0Zm9vYmFyYmF6"

	oldCfg := *baseCfg
	defer func() {
		*baseCfg = oldCfg
	}()
	sslCertsDir := filepath.Join(tempDir, "certs")
	require.NoError(t, os.MkdirAll(sslCertsDir, 0755))
	baseCfg.SSLCertsDir = sslCertsDir
	serverCfg.JoinList = []string{s.ServingRPCAddr()}
	baseCfg.Addr = "127.0.0.1:0"
	baseCfg.AdvertiseAddr = baseCfg.Addr

	err = runConnectJoin(nil, []string{token})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid join token")

	// Ensure the SSLCertsDir is empty.
	f, err := os.Open(sslCertsDir)
	require.NoError(t, err)
	_, err = f.Readdirnames(1)
	// An error is returned if the directory is empty.
	require.Error(t, err)
}
