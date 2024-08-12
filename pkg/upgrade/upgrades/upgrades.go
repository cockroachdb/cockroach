// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package upgrades contains the implementation of upgrades. It is imported
// by the server library.
//
// This package registers the upgrades with the upgrade package.
package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
)

// SettingsDefaultOverrides documents the effect of several migrations that add
// an explicit value for a setting, effectively changing the "default value"
// from what was defined in code.
var SettingsDefaultOverrides = map[settings.InternalKey]string{
	"diagnostics.reporting.enabled": "true",
	"cluster.secret":                "<random>",
}

// GetUpgrade returns the upgrade corresponding to this version if
// one exists.
func GetUpgrade(key roachpb.Version) (upgradebase.Upgrade, bool) {
	m, ok := registry[key]
	if !ok {
		return nil, false
	}
	return m, ok
}

// NoTenantUpgradeFunc is a TenantUpgradeFunc that doesn't do anything.
func NoTenantUpgradeFunc(context.Context, clusterversion.ClusterVersion, upgrade.TenantDeps) error {
	return nil
}

// registry defines the global mapping between a cluster version and the
// associated upgrade. The upgrade is only executed after a cluster-wide
// bump of the corresponding version gate.
var registry = make(map[roachpb.Version]upgradebase.Upgrade)

var upgrades = []upgradebase.Upgrade{
	upgrade.NewSystemUpgrade("intitialize the system cluster",
		clusterversion.VBootstrapSystem.Version(),
		bootstrapSystem,
		upgrade.RestoreActionNotRequired("initialization runs before restore")),

	upgrade.NewTenantUpgrade("intitialize the cluster",
		clusterversion.VBootstrapTenant.Version(),
		upgrade.NoPrecondition,
		bootstrapCluster,
		upgrade.RestoreActionNotRequired("initialization runs before restore")),

	newFirstUpgrade(clusterversion.V24_2Start.Version()),

	upgrade.NewTenantUpgrade(
		"add the redacted column to system.statement_diagnostics_requests table",
		clusterversion.V24_2_StmtDiagRedacted.Version(),
		upgrade.NoPrecondition,
		stmtDiagRedactedMigration,
		upgrade.RestoreActionNotRequired("cluster restore does not restore this table"),
	),

	upgrade.NewTenantUpgrade(
		"create all missing system tables in app tenants",
		clusterversion.V24_2_TenantSystemTables.Version(),
		upgrade.NoPrecondition,
		createTenantSystemTables,
		upgrade.RestoreActionNotRequired("cluster restore does not restore these tables"),
	),

	upgrade.NewTenantUpgrade(
		"add new columns to the system.tenant_usage table to store tenant consumption rates",
		clusterversion.V24_2_TenantRates.Version(),
		upgrade.NoPrecondition,
		tenantRatesMigration,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the new field"),
	),

	upgrade.NewTenantUpgrade(
		"delete version row in system.tenant_settings",
		clusterversion.V24_2_DeleteTenantSettingsVersion.Version(),
		upgrade.NoPrecondition,
		deleteVersionTenantSettings,
		upgrade.RestoreActionImplemented("bad row skipped when restoring system.tenant_settings"),
	),

	upgrade.NewTenantUpgrade(
		"add new table for listen/notify queue",
		clusterversion.V24_3_ListenNotifyQueue.Version(),
		upgrade.NoPrecondition,
		createListenNotifyQueyeTables,
		upgrade.RestoreActionNotRequired("idk lol"),
	),

	// Note: when starting a new release version, the first upgrade (for
	// Vxy_zStart) must be a newFirstUpgrade. Keep this comment at the bottom.
}

func init() {
	for _, m := range upgrades {
		registry[m.Version()] = m
	}
}
