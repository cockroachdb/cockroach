// Copyright 2020 The Cockroach Authors.
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
	"os/signal"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var mtStartSQLCmd = &cobra.Command{
	Use:   "start-sql",
	Short: "start a standalone SQL server",
	Long: `
Start a standalone SQL server.

This functionality is **experimental** and for internal use only.

The following certificates are required:

- ca.crt, node.{crt,key}: CA cert and key pair for serving the SQL endpoint.
  Note that under no circumstances should the node certs be shared with those of
  the same name used at the KV layer, as this would pose a severe security risk.
- ca-client-tenant.crt, client-tenant.X.{crt,key}: CA cert and key pair for
  authentication and authorization with the KV layer (as tenant X).
- ca-server-tenant.crt: to authenticate KV layer.

                 ca.crt        ca-client-tenant.crt        ca-server-tenant.crt
user ---------------> sql server ----------------------------> kv
 client.Y.crt    node.crt      client-tenant.X.crt         server-tenant.crt
 client.Y.key    node.key      client-tenant.X.key         server-tenant.key

Note that CA certificates need to be present on the "other" end of the arrow as
well unless it can be verified using a trusted root certificate store. That is,

- ca.crt needs to be passed in the Postgres connection string (sslrootcert) if
  sslmode=verify-ca.
- ca-server-tenant.crt needs to be present on the SQL server.
- ca-client-tenant.crt needs to be present on the KV server.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runStartSQL),
}

func runStartSQL(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	const clusterName = ""

	// Remove the default store, which avoids using it to set up logging.
	// Instead, we'll default to logging to stderr unless --log-dir is
	// specified. This makes sense since the standalone SQL server is
	// at the time of writing stateless and may not be provisioned with
	// suitable storage.
	serverCfg.Stores.Specs = nil

	stopper, err := setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
	if err != nil {
		return err
	}
	defer stopper.Stop(ctx)

	st := serverCfg.BaseConfig.Settings

	// TODO(tbg): this has to be passed in. See the upgrade strategy in:
	// https://github.com/cockroachdb/cockroach/issues/47919
	if err := clusterversion.Initialize(ctx, st.Version.BinaryVersion(), &st.SV); err != nil {
		return err
	}

	tempStorageMaxSizeBytes := int64(base.DefaultInMemTempStorageMaxSizeBytes)
	if err := diskTempStorageSizeValue.Resolve(
		&tempStorageMaxSizeBytes, memoryPercentResolver,
	); err != nil {
		return err
	}

	serverCfg.SQLConfig.TempStorageConfig = base.TempStorageConfigFromEnv(
		ctx,
		st,
		base.StoreSpec{InMemory: true},
		"", // parentDir
		tempStorageMaxSizeBytes,
	)

	sqlServer, addr, httpAddr, err := server.StartTenant(
		ctx,
		stopper,
		clusterName,
		serverCfg.BaseConfig,
		serverCfg.SQLConfig,
	)
	if err != nil {
		return err
	}

	// Start up the diagnostics reporting loop.
	// We don't do this in (*server.SQLServer).preStart() because we don't
	// want this overhead and possible interference in tests.
	if !cluster.TelemetryOptOut() {
		sqlServer.StartDiagnostics(ctx)
	}

	log.Infof(ctx, "SQL server for tenant %s listening at %s, http at %s", serverCfg.SQLConfig.TenantID, addr, httpAddr)

	// TODO(tbg): make the other goodies in `./cockroach start` reusable, such as
	// logging to files, periodic memory output, heap and goroutine dumps, debug
	// server, graceful drain. Then use them here.

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, drainSignals...)
	sig := <-ch
	return errors.Newf("received signal %v", sig)
}
