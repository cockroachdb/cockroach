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
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// StartInMemoryTenantServer starts an in-memory server for the given target tenant ID.
// The resulting stopper must be closed in any case, even when an error is returned.
func (s *Server) StartInMemoryTenantServer(
	ctx context.Context, tenantID roachpb.TenantID,
) (stopper *stop.Stopper, sqlServer *SQLServerWrapper, err error) {
	stopper = stop.NewStopper()

	// Can we even start a server for this tenant ID?
	if err := checkTenantExists(ctx, s.sqlServer.internalExecutor, tenantID); err != nil {
		return stopper, nil, err
	}
	// Create a configuration for the new tenant.
	// TODO(knz): Maybe enforce the SQL Instance ID to be equal to the KV node ID?
	// See: https://github.com/cockroachdb/cockroach/issues/84602
	parentCfg := s.cfg
	baseCfg, sqlCfg, err := makeInMemoryTenantServerConfig(ctx, tenantID, parentCfg, stopper)
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
	sqlServer, err = StartTenant(startCtx, stopper, parentCfg.BaseConfig.Config.ClusterName, baseCfg, sqlCfg)
	if err != nil {
		return stopper, sqlServer, err
	}

	// Start up the diagnostics reporting loop.
	if !cluster.TelemetryOptOut() {
		sqlServer.StartDiagnostics(startCtx)
	}
	return stopper, sqlServer, nil
}

func checkTenantExists(
	ctx context.Context, ie *sql.InternalExecutor, tenantID roachpb.TenantID,
) error {
	// First check that the tenant actually exists.
	rowCount, err := ie.Exec(
		ctx, "check-tenant-active", nil,
		"SELECT 1 FROM system.tenants WHERE id=$1 AND active=true",
		tenantID.ToUint64(),
	)
	if err != nil {
		return err
	}
	if rowCount == 0 {
		return errors.Newf("tenant %d not found", tenantID)
	}
	return nil
}

func makeInMemoryTenantServerConfig(
	ctx context.Context, tenantID roachpb.TenantID, kvServerCfg Config, stopper *stop.Stopper,
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
	baseCfg = MakeBaseConfig(st, tr)

	// Uncontroversial inherited values.
	baseCfg.Config.Insecure = kvServerCfg.Config.Insecure
	baseCfg.Config.User = kvServerCfg.Config.User
	baseCfg.Config.DisableTLSForHTTP = kvServerCfg.Config.DisableTLSForHTTP
	baseCfg.Config.AcceptSQLWithoutTLS = kvServerCfg.Config.AcceptSQLWithoutTLS
	baseCfg.Config.RPCHeartbeatInterval = kvServerCfg.Config.RPCHeartbeatInterval
	baseCfg.Config.ClockDevicePath = kvServerCfg.Config.ClockDevicePath
	baseCfg.Config.ClusterName = kvServerCfg.Config.ClusterName
	baseCfg.Config.DisableClusterNameVerification = kvServerCfg.Config.DisableClusterNameVerification

	baseCfg.MaxOffset = kvServerCfg.BaseConfig.MaxOffset
	baseCfg.StorageEngine = kvServerCfg.BaseConfig.StorageEngine
	baseCfg.EnableWebSessionAuthentication = kvServerCfg.BaseConfig.EnableWebSessionAuthentication
	baseCfg.Locality = kvServerCfg.BaseConfig.Locality
	baseCfg.SpanConfigsDisabled = kvServerCfg.BaseConfig.SpanConfigsDisabled
	baseCfg.EnableDemoLoginEndpoint = kvServerCfg.BaseConfig.EnableDemoLoginEndpoint

	// TODO(knz): Make the server addresses configurable, pending
	// resolution of https://github.com/cockroachdb/cockroach/issues/84585
	baseCfg.Addr = "127.0.0.1:26258" // FIXME
	baseCfg.AdvertiseAddr = baseCfg.Addr
	baseCfg.HTTPAddr = "127.0.0.1:8081" // FIXME
	baseCfg.HTTPAdvertiseAddr = baseCfg.HTTPAddr
	baseCfg.SplitListenSQL = false
	baseCfg.SQLAddr = baseCfg.Addr
	baseCfg.SQLAdvertiseAddr = baseCfg.SQLAddr

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

	// Define the unix socket intelligently.
	// See: https://github.com/cockroachdb/cockroach/issues/84585
	sqlCfg.SocketFile = ""

	// Use the same memory budget for each secondary tenant. The assumption
	// here is that we use max 2 tenants, and that under common loads one
	// of them will be mostly idle.
	// We might want to reconsider this if we use more than 1 in-memory tenant at a time.
	sqlCfg.MemoryPoolSize = kvServerCfg.SQLConfig.MemoryPoolSize
	sqlCfg.TableStatCacheSize = kvServerCfg.SQLConfig.TableStatCacheSize
	sqlCfg.QueryCacheSize = kvServerCfg.SQLConfig.QueryCacheSize

	return baseCfg, sqlCfg, nil
}
