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
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/sdnotify"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	RunE: clierrorplus.MaybeDecorateError(runStartSQL),
}

func runStartSQL(cmd *cobra.Command, args []string) error {
	tBegin := timeutil.Now()

	// First things first: if the user wants background processing,
	// relinquish the terminal ASAP by forking and exiting.
	//
	// If executing in the background, the function returns ok == true in
	// the parent process (regardless of err) and the parent exits at
	// this point.
	if ok, err := maybeRerunBackground(); ok {
		return err
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, drainSignals...)

	// Set up a cancellable context for the entire start command.
	// The context will be canceled at the end.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The context annotation ensures that server identifiers show up
	// in the logging metadata as soon as they are known.
	ambientCtx := serverCfg.AmbientCtx
	ctx = ambientCtx.AnnotateCtx(ctx)

	const clusterName = ""

	stopper, err := setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
	if err != nil {
		return err
	}
	defer stopper.Stop(ctx)
	stopper.SetTracer(serverCfg.BaseConfig.AmbientCtx.Tracer)

	st := serverCfg.BaseConfig.Settings

	// This value is injected in order to have something populated during startup.
	// In the initial 20.2 release of multi-tenant clusters, no version state was
	// ever populated in the version cluster setting. A value is populated during
	// the activation of 21.1. See the documentation attached to the TenantCluster
	// in migration/migrationcluster for more details on the tenant upgrade flow.
	// Note that a the value of 21.1 is populated when a tenant cluster is created
	// during 21.1 in crdb_internal.create_tenant.
	//
	// Note that the tenant will read the value in the system.settings table
	// before accepting SQL connections.
	if err := clusterversion.Initialize(
		ctx, st.Version.BinaryMinSupportedVersion(), &st.SV,
	); err != nil {
		return err
	}

	if serverCfg.SQLConfig.TempStorageConfig, err = initTempStorageConfig(
		ctx, serverCfg.Settings, stopper, serverCfg.Stores,
	); err != nil {
		return err
	}

	initGEOS(ctx)

	sqlServer, err := server.StartTenant(
		ctx,
		stopper,
		clusterName,
		serverCfg.BaseConfig,
		serverCfg.SQLConfig,
	)
	if err != nil {
		return err
	}
	// If another process was waiting on the PID (e.g. using a FIFO),
	// this is when we can tell them the node has started listening.
	if startCtx.pidFile != "" {
		log.Ops.Infof(ctx, "PID file: %s", startCtx.pidFile)
		if err := ioutil.WriteFile(startCtx.pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644); err != nil {
			log.Ops.Errorf(ctx, "failed writing the PID: %v", err)
		}
	}

	// Ensure the configuration logging is written to disk in case a
	// process is waiting for the sdnotify readiness to read important
	// information from there.
	log.Flush()

	// Signal readiness. This unblocks the process when running with
	// --background or under systemd.
	if err := sdnotify.Ready(); err != nil {
		log.Ops.Errorf(ctx, "failed to signal readiness using systemd protocol: %s", err)
	}

	// Start up the diagnostics reporting loop.
	// We don't do this in (*server.SQLServer).preStart() because we don't
	// want this overhead and possible interference in tests.
	if !cluster.TelemetryOptOut() {
		sqlServer.StartDiagnostics(ctx)
	}

	// Report the server identifiers and other server details
	// in the same format as 'cockroach start'.
	if err := reportServerInfo(ctx, tBegin, &serverCfg, st, false /* isHostNode */, false /* initialStart */); err != nil {
		return err
	}

	// TODO(tbg): make the other goodies in `./cockroach start` reusable, such as
	// logging to files, periodic memory output, heap and goroutine dumps.
	// Then use them here.

	errChan := make(chan error, 1)
	var serverStatusMu serverStatus
	serverStatusMu.started = true

	return waitForShutdown(
		func() serverShutdownInterface { return sqlServer },
		stopper,
		errChan, signalCh,
		&serverStatusMu)
}
