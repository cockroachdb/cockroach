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

	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/cgroups"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/pebble/vfs"
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

	// Change the permission mask for all created files.
	//
	// We're considering everything produced by a cockroach node
	// to potentially contain sensitive information, so it should
	// not be world-readable.
	disableOtherPermissionBits()

	// Set up the signal handlers. This also ensures that any of these
	// signals received beyond this point do not interrupt the startup
	// sequence until the point signals are checked below.
	// We want to set up signal handling before starting logging, because
	// logging uses buffering, and we want to be able to sync
	// the buffers in the signal handler below. If we started capturing
	// signals later, some startup logging might be lost.
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, drainSignals...)

	// Check for stores with full disks and exit with an informative exit
	// code. This needs to happen early during start, before we perform any
	// writes to the filesystem including log rotation. We need to guarantee
	// that the process continues to exit with the Disk Full exit code. A
	// flapping exit code can affect alerting, including the alerting
	// performed within CockroachCloud.
	if err := exitIfDiskFull(vfs.Default, serverCfg.Stores.Specs); err != nil {
		return err
	}

	// If any store has something to say against a server start-up
	// (e.g. previously detected corruption), listen to them now.
	if err := serverCfg.Stores.PriorCriticalAlertError(); err != nil {
		return clierror.NewError(err, exit.FatalError())
	}

	// Set up a cancellable context for the entire start command.
	// The context will be canceled at the end.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The context annotation ensures that server identifiers show up
	// in the logging metadata as soon as they are known.
	ambientCtx := serverCfg.AmbientCtx

	// Annotate the context, and set up a tracing span for the start process.
	//
	// The context annotation ensures that server identifiers show up
	// in the logging metadata as soon as they are known.
	//
	// The tracing span is because we want any logging happening beyond
	// this point to be accounted to this start context, including
	// logging related to the initialization of the logging
	// infrastructure below.  This span concludes when the startup
	// goroutine started below has completed.  TODO(andrei): we don't
	// close the span on the early returns below.
	var startupSpan *tracing.Span
	ctx, startupSpan = ambientCtx.AnnotateCtxWithSpan(ctx, "server start")

	// Set up the logging and profiling output.
	//
	// We want to do this as early as possible, because most of the code
	// in CockroachDB may use logging, and until logging has been
	// initialized log files will be created in $TMPDIR instead of their
	// expected location.
	//
	// This initialization uses the various configuration parameters
	// initialized by flag handling (before runStart was called). Any
	// additional server configuration tweaks for the startup process
	// must be necessarily non-logging-related, as logging parameters
	// cannot be picked up beyond this point.
	stopper, err := setupAndInitializeLoggingAndProfiling(ctx, cmd, false /* isServerCmd */)
	if err != nil {
		return err
	}
	defer stopper.Stop(ctx) // TODO(knz): Move this.
	stopper.SetTracer(serverCfg.BaseConfig.AmbientCtx.Tracer)

	// We don't care about GRPCs fairly verbose logs in most client commands,
	// but when actually starting a server, we enable them.
	grpcutil.LowerSeverity(severity.WARNING)

	// Tweak GOMAXPROCS if we're in a cgroup / container that has cpu limits set.
	// The GO default for GOMAXPROCS is NumCPU(), however this is less
	// than ideal if the cgroup is limited to a number lower than that.
	//
	// TODO(bilal): various global settings have already been initialized based on
	// GOMAXPROCS(0) by now.
	cgroups.AdjustMaxProcs(ctx)

	// Now perform additional configuration tweaks specific to the start
	// command.

	st := serverCfg.BaseConfig.Settings

	// Derive temporary/auxiliary directory specifications.
	if st.ExternalIODir, err = initExternalIODir(ctx, serverCfg.Stores.Specs[0]); err != nil {
		return err
	}

	// This value is injected in order to have something populated during startup.
	// In the initial 20.2 release of multi-tenant clusters, no version state was
	// ever populated in the version cluster setting. A value is populated during
	// the activation of 21.1. See the documentation attached to the TenantCluster
	// in upgrade/upgradecluster for more details on the tenant upgrade flow.
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
		ctx, st, stopper, serverCfg.Stores,
	); err != nil {
		return err
	}

	// Configure the default storage engine.
	// NB: we only support one engine type for now.
	if serverCfg.StorageEngine == enginepb.EngineTypeDefault {
		serverCfg.StorageEngine = enginepb.EngineTypePebble
	}

	// Initialize the node's configuration from startup parameters.
	// This also reads the part of the configuration that comes from
	// environment variables.
	if err := serverCfg.InitSQLServer(ctx); err != nil {
		return err
	}

	// The configuration is now ready to report to the user and the log
	// file. We had to wait after InitNode() so that all configuration
	// environment variables, which are reported too, have been read and
	// registered.
	reportConfiguration(ctx)

	initGEOS(ctx)

	// ReadyFn will be called when the server has started listening on
	// its network sockets, but perhaps before it has done bootstrapping
	serverCfg.ReadyFn = func(_ bool) { reportReadinessExternally(ctx, cmd, false /* waitForInit */) }

	// Beyond this point, the configuration is set and the server is
	// ready to start.
	log.Ops.Info(ctx, "starting cockroach SQL node")

	sqlServer, err := server.StartTenant(
		ctx,
		stopper,
		serverCfg.BaseConfig,
		serverCfg.SQLConfig,
	)
	if err != nil {
		return err
	}

	// When the start up completes, so can the start up span
	// defined above.
	defer startupSpan.Finish()

	// Start up the diagnostics reporting loop.
	// We don't do this in (*server.SQLServer).preStart() because we don't
	// want this overhead and possible interference in tests.
	if !cluster.TelemetryOptOut() {
		sqlServer.StartDiagnostics(ctx)
	}

	tenantClusterID := sqlServer.LogicalClusterID()

	// Report the server identifiers and other server details
	// in the same format as 'cockroach start'.
	if err := reportServerInfo(ctx, tBegin, &serverCfg, st, false /* isHostNode */, false /* initialStart */, tenantClusterID); err != nil {
		return err
	}

	// TODO(tbg): make the other goodies in `./cockroach start` reusable, such as
	// logging to files, periodic memory output, heap and goroutine dumps.
	// Then use them here.

	serverStartupErrC := make(chan error, 1)
	var serverStatusMu serverStatus
	serverStatusMu.started = true

	return waitForShutdown(
		func() serverShutdownInterface { return sqlServer },
		stopper,
		serverStartupErrC, signalCh,
		&serverStatusMu)
}
