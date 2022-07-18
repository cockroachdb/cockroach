// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/clientsecopts"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil/addr"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

// StartInMemoryAppTenant starts in-memory servers for the app tenant.
// TODO(knz): Replace this logic by a dynamic registry.
// See: https://github.com/cockroachdb/cockroach/issues/84604
func (s *Server) StartInMemoryAppTenant(ctx context.Context) error {
	// What is the ID of the app tenant?
	// TODO(knz): use a flag or capability to decide whether to start in-memory.
	ie := s.sqlServer.internalExecutor
	datums, err := ie.QueryRow(ctx, "get-app-tenant-id", nil, /* txn */
		// TODO(knz): This might benefit from an index?
		`
SELECT id
  FROM system.tenants
 WHERE name = 'app'
 LIMIT 1
`)
	if err != nil {
		return err
	}
	if datums == nil {
		log.Ops.Infof(ctx, "no app tenant found")
		// Nothing else to do.
		return nil
	}

	// Start the app tenant if found.
	appTenantID := uint64(tree.MustBeDInt(datums[0]))
	_, _, err = s.startInMemoryTenantServerInternal(ctx, roachpb.MakeTenantID(appTenantID), 1)
	return err
}

// startInMemoryTenantServerInternal starts an in-memory server for the given target tenant ID.
// The resulting stopper should be closed in any case, even when an error is returned.
func (s *Server) startInMemoryTenantServerInternal(
	ctx context.Context, tenantID roachpb.TenantID, index int,
) (stopper *stop.Stopper, tenantServer *SQLServerWrapper, err error) {
	stopper = stop.NewStopper()

	// Can we even start a server for this tenant ID?
	if err := checkTenantExistsAndIsActive(ctx, s.sqlServer.execCfg, tenantID); err != nil {
		return stopper, nil, err
	}
	// Create a configuration for the new tenant.
	// TODO(knz): Maybe enforce the SQL Instance ID to be equal to the KV node ID?
	// See: https://github.com/cockroachdb/cockroach/issues/84602
	parentCfg := s.cfg
	baseCfg, sqlCfg, err := makeInMemoryTenantServerConfig(ctx, tenantID, index, parentCfg, stopper)
	if err != nil {
		return stopper, nil, err
	}

	// Create a child stopper for this tenant's server.
	ambientCtx := baseCfg.AmbientCtx
	stopper.SetTracer(ambientCtx.Tracer)

	// The parent stopper stops the tenant's stopper.
	parentStopper := s.stopper
	parentStopper.AddCloser(stop.CloserFn(func() { stopper.Stop(context.Background()) }))

	// New context, since we're using a separate tracer.
	startCtx := ambientCtx.AnnotateCtx(context.Background())
	startCtx = logtags.AddTags(startCtx, logtags.FromContext(ctx))

	// Now start the tenant proper.
	tenantServer, err = NewTenantServer(startCtx, stopper, baseCfg, sqlCfg)
	if err != nil {
		return stopper, tenantServer, err
	}
	if err := tenantServer.Start(startCtx); err != nil {
		return stopper, tenantServer, err
	}

	// Start up the diagnostics reporting loop.
	if !cluster.TelemetryOptOut() {
		tenantServer.StartDiagnostics(startCtx)
	}

	// Show the tenant details in logs.
	// TODO(knz): Pull out this logic to cli/start.go so it also
	// can be displayed on the terminal, conditional on --backround.
	if err := reportTenantInfo(startCtx, baseCfg, sqlCfg); err != nil {
		return stopper, tenantServer, err
	}

	return stopper, tenantServer, nil
}

func checkTenantExistsAndIsActive(
	ctx context.Context, execCfg *sql.ExecutorConfig, tenantID roachpb.TenantID,
) error {
	// First check that the tenant actually exists.
	ie := execCfg.InternalExecutor
	rowCount, err := ie.Exec(
		ctx, "check-tenant-active", nil,
		"SELECT 1 FROM system.tenants WHERE id=$1 AND active=true",
		tenantID.ToUint64(),
	)
	if err != nil {
		return err
	}
	if rowCount == 0 {
		return errors.Newf("tenant %v not found or not active", tenantID)
	}
	return nil
}

func makeInMemoryTenantServerConfig(
	ctx context.Context,
	tenantID roachpb.TenantID,
	index int,
	kvServerCfg Config,
	stopper *stop.Stopper,
) (baseCfg BaseConfig, sqlCfg SQLConfig, err error) {
	st := cluster.MakeClusterSettings()

	// This version initialization is copied from cli/mt_start_sql.go.
	// TODO(knz): Why is this even useful? The comment refers to v21.1 compatibility,
	// yet if we don't do this, the server panics with "version not initialized".
	// This might be related to: https://github.com/cockroachdb/cockroach/issues/84587
	if err := clusterversion.Initialize(
		ctx, st.Version.BinaryMinSupportedVersion(), &st.SV,
	); err != nil {
		return baseCfg, sqlCfg, err
	}

	tr := tracing.NewTracerWithOpt(ctx, tracing.WithClusterSettings(&st.SV))

	// Find a suitable store directory.
	// TODO(knz): Derive these parameters more intelligently.
	storeDir := ""
	for _, storeSpec := range kvServerCfg.Stores.Specs {
		if storeSpec.InMemory {
			continue
		}
		storeDir = filepath.Join(storeSpec.Path, "app-data")
		break
	}
	if storeDir == "" {
		storeDir = "app-data"
	}
	if err := os.MkdirAll(storeDir, 0700); err != nil {
		return baseCfg, sqlCfg, err
	}

	storeSpec, err := base.NewStoreSpec(storeDir)
	if err != nil {
		return baseCfg, sqlCfg, errors.Wrap(err, "cannot create store spec")
	}
	baseCfg = MakeBaseConfig(st, tr, storeSpec)

	// Uncontroversial inherited values.
	baseCfg.Config.Insecure = kvServerCfg.Config.Insecure
	baseCfg.Config.User = kvServerCfg.Config.User
	baseCfg.Config.DisableTLSForHTTP = kvServerCfg.Config.DisableTLSForHTTP
	baseCfg.Config.AcceptSQLWithoutTLS = kvServerCfg.Config.AcceptSQLWithoutTLS
	baseCfg.Config.RPCHeartbeatIntervalAndHalfTimeout = kvServerCfg.Config.RPCHeartbeatIntervalAndHalfTimeout
	baseCfg.Config.ClockDevicePath = kvServerCfg.Config.ClockDevicePath
	baseCfg.Config.ClusterName = kvServerCfg.Config.ClusterName
	baseCfg.Config.DisableClusterNameVerification = kvServerCfg.Config.DisableClusterNameVerification

	baseCfg.MaxOffset = kvServerCfg.BaseConfig.MaxOffset
	baseCfg.StorageEngine = kvServerCfg.BaseConfig.StorageEngine
	baseCfg.EnableWebSessionAuthentication = kvServerCfg.BaseConfig.EnableWebSessionAuthentication
	baseCfg.Locality = kvServerCfg.BaseConfig.Locality
	baseCfg.SpanConfigsDisabled = kvServerCfg.BaseConfig.SpanConfigsDisabled
	baseCfg.EnableDemoLoginEndpoint = kvServerCfg.BaseConfig.EnableDemoLoginEndpoint

	// TODO(knz): use a single network interface for all tenant servers.
	// See: https://github.com/cockroachdb/cockroach/issues/84585
	portOffset := kvServerCfg.Config.SecondaryTenantPortOffset
	var err1, err2, err3, err4, err5, err6 error
	baseCfg.Addr, err1 = rederivePort(index, kvServerCfg.Config.Addr, "", portOffset)
	baseCfg.AdvertiseAddr, err2 = rederivePort(index, kvServerCfg.Config.AdvertiseAddr, baseCfg.Addr, portOffset)
	baseCfg.HTTPAddr, err3 = rederivePort(index, kvServerCfg.Config.HTTPAddr, "", portOffset)
	baseCfg.HTTPAdvertiseAddr, err4 = rederivePort(index, kvServerCfg.Config.HTTPAdvertiseAddr, baseCfg.HTTPAddr, portOffset)
	baseCfg.SQLAddr, err5 = rederivePort(index, kvServerCfg.Config.SQLAddr, "", portOffset)
	baseCfg.SQLAdvertiseAddr, err6 = rederivePort(index, kvServerCfg.Config.SQLAdvertiseAddr, baseCfg.SQLAddr, portOffset)
	if err := errors.CombineErrors(err1,
		errors.CombineErrors(err2,
			errors.CombineErrors(err3,
				errors.CombineErrors(err4,
					errors.CombineErrors(err5, err6))))); err != nil {
		return baseCfg, sqlCfg, err
	}
	// Define the unix socket intelligently.
	// See: https://github.com/cockroachdb/cockroach/issues/84585
	baseCfg.SocketFile = ""

	baseCfg.SplitListenSQL = false

	// TODO(knz): Make the TLS config separate per tenant.
	// See https://cockroachlabs.atlassian.net/browse/CRDB-14539.
	baseCfg.SSLCertsDir = kvServerCfg.BaseConfig.SSLCertsDir
	baseCfg.SSLCAKey = kvServerCfg.BaseConfig.SSLCAKey

	// TODO(knz): startSampleEnvironment() should not be part of startTenantInternal. For now,
	// disable the mechanism manually.
	// See: https://github.com/cockroachdb/cockroach/issues/84589
	baseCfg.GoroutineDumpDirName = ""
	baseCfg.HeapProfileDirName = ""
	baseCfg.CPUProfileDirName = ""
	baseCfg.InflightTraceDirName = ""

	// TODO(knz): Define a meaningful storage config for each tenant,
	// see: https://github.com/cockroachdb/cockroach/issues/84588.
	useStore := kvServerCfg.SQLConfig.TempStorageConfig.Spec
	tempStorageCfg := base.TempStorageConfigFromEnv(
		ctx, st, useStore, "" /* parentDir */, kvServerCfg.SQLConfig.TempStorageConfig.Mon.MaximumBytes())
	// TODO(knz): Make tempDir configurable.
	tempDir := useStore.Path
	if tempStorageCfg.Path, err = fs.CreateTempDir(tempDir, TempDirPrefix, stopper); err != nil {
		return baseCfg, sqlCfg, errors.Wrap(err, "could not create temporary directory for temp storage")
	}
	if useStore.Path != "" {
		recordPath := filepath.Join(useStore.Path, TempDirsRecordFilename)
		if err := fs.RecordTempDir(recordPath, tempStorageCfg.Path); err != nil {
			return baseCfg, sqlCfg, errors.Wrap(err, "could not record temp dir")
		}
	}

	sqlCfg = MakeSQLConfig(tenantID, tempStorageCfg)

	// Split for each tenant, see https://github.com/cockroachdb/cockroach/issues/84588.
	sqlCfg.ExternalIODirConfig = kvServerCfg.SQLConfig.ExternalIODirConfig

	// Use the internal connector instead of the network.
	// See: https://github.com/cockroachdb/cockroach/issues/84591
	sqlCfg.TenantKVAddrs = []string{kvServerCfg.BaseConfig.Config.AdvertiseAddr}

	// Use the same memory budget for each secondary tenant. The assumption
	// here is that we use max 2 tenants, and that under common loads one
	// of them will be mostly idle.
	// We might want to reconsider this if we use more than 1 in-memory tenant at a time.
	sqlCfg.MemoryPoolSize = kvServerCfg.SQLConfig.MemoryPoolSize
	sqlCfg.TableStatCacheSize = kvServerCfg.SQLConfig.TableStatCacheSize
	sqlCfg.QueryCacheSize = kvServerCfg.SQLConfig.QueryCacheSize

	return baseCfg, sqlCfg, nil
}

// rederivePort computes a host:port pair for a secondary tenant.
// TODO(knz): All this can be removed once we implement a single
// network listener.
// See https://github.com/cockroachdb/cockroach/issues/84604.
func rederivePort(index int, addrToChange string, prevAddr string, portOffset int) (string, error) {
	h, port, err := addr.SplitHostPort(addrToChange, "0")
	if err != nil {
		return "", errors.Wrapf(err, "%d: %q", index, addrToChange)
	}

	if portOffset == 0 {
		// Shortcut: random selection for base address.
		return net.JoinHostPort(h, "0"), nil
	}

	var pnum int
	if port != "" {
		pnum, err = strconv.Atoi(port)
		if err != nil {
			return "", errors.Wrapf(err, "%d: %q", index, addrToChange)
		}
	}

	if prevAddr != "" && pnum == 0 {
		// Try harder to find a port number, by taking one from
		// the previously computed addr.
		_, port2, err := addr.SplitHostPort(prevAddr, "0")
		if err != nil {
			return "", errors.Wrapf(err, "%d: %q", index, prevAddr)
		}
		pnum, err = strconv.Atoi(port2)
		if err != nil {
			return "", errors.Wrapf(err, "%d: %q", index, prevAddr)
		}
	}

	// Do we have a base port to go with now?
	if pnum == 0 {
		// No, bail.
		return "", errors.Newf("%d: no base port available for computation in %q / %q", index, addrToChange, prevAddr)
	}
	port = strconv.Itoa(pnum + portOffset + index)
	return net.JoinHostPort(h, port), nil
}

func reportTenantInfo(ctx context.Context, baseCfg BaseConfig, sqlCfg SQLConfig) error {
	var buf redact.StringBuilder
	buf.Printf("started tenant SQL server at %s\n", timeutil.Now())
	buf.Printf("webui:\t%s\n", baseCfg.AdminURL())
	clientConnOptions, serverParams := MakeServerOptionsForURL(baseCfg.Config)
	pgURL, err := clientsecopts.MakeURLForServer(clientConnOptions, serverParams, url.User(username.RootUser))
	if err != nil {
		log.Errorf(ctx, "failed computing the URL: %v", err)
	} else {
		buf.Printf("sql:\t%s\n", pgURL.ToPQ())
		buf.Printf("sql (JDBC):\t%s\n", pgURL.ToJDBC())
	}
	if baseCfg.SocketFile != "" {
		buf.Printf("socket:\t%s\n", baseCfg.SocketFile)
	}
	if tmpDir := sqlCfg.TempStorageConfig.Path; tmpDir != "" {
		buf.Printf("temp dir:\t%s\n", tmpDir)
	}
	buf.Printf("clusterID:\t%s\n", baseCfg.ClusterIDContainer.Get())
	buf.Printf("tenantID:\t%s\n", sqlCfg.TenantID)
	buf.Printf("instanceID:\t%d\n", baseCfg.IDContainer.Get())
	// Collect the formatted string and show it to the user.
	msg, err := util.ExpandTabsInRedactableBytes(buf.RedactableBytes())
	if err != nil {
		return err
	}
	msgS := msg.ToString()
	log.Ops.Infof(ctx, "tenant startup completed:\n%s", msgS)
	return nil
}
