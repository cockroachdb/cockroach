// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package operations

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/changefeeds"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
)

// RegisterOperations registers all operations to the Registry. This powers `roachtest run-operations`.
func RegisterOperations(r registry.Registry) {
	registerAddColumn(r)
	registerAddDatabase(r)
	registerAddIndex(r)
	registerAddRLSPolicy(r)
	registerShowTables(r)
	registerGrantRole(r)
	registerGrantRevoke(r)
	registerNetworkPartition(r)
	registerDiskStall(r)
	//registerNodeKill(r)
	registerClusterSettings(r)
	registerCreateSQLOperations(r)
	registerBackupRestore(r)
	registerManualCompaction(r)
	registerResize(r)
	registerPauseJob(r)
	registerCancelJob(r)
	registerProbeRanges(r)
	registerLicenseThrottle(r)
	registerSessionVariables(r)
	registerDebugZip(r)
	changefeeds.RegisterChangefeeds(r)
	registerInspect(r)
}
