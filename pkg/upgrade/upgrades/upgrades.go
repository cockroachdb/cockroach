// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	newFirstUpgrade(clusterversion.V25_1_Start.Version()),

	upgrade.NewTenantUpgrade(
		"add new jobs tables",
		clusterversion.V25_1_AddJobsTables.Version(),
		upgrade.NoPrecondition,
		addJobsTables,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the new field"),
	),

	upgrade.NewTenantUpgrade(
		"create prepared_transactions table",
		clusterversion.V25_1_PreparedTransactionsTable.Version(),
		upgrade.NoPrecondition,
		createPreparedTransactionsTable,
		upgrade.RestoreActionNotRequired("cluster restore does not restore this table"),
	),
	upgrade.NewTenantUpgrade(
		"add new jobs tables",
		clusterversion.V25_1_AddJobsColumns.Version(),
		upgrade.NoPrecondition,
		addJobsColumns,
		upgrade.RestoreActionNotRequired("cluster restore does not restore the new field"),
	),
	upgrade.NewTenantUpgrade(
		"backfill new jobs tables",
		clusterversion.V25_1_JobsBackfill.Version(),
		upgrade.NoPrecondition,
		backfillJobsTablesAndColumns,
		upgrade.RestoreActionNotRequired("cluster restore does not restore jobs tables"),
	),

	newFirstUpgrade(clusterversion.V25_2_Start.Version()),

	upgrade.NewTenantUpgrade(
		"add new sql activity flush job",
		clusterversion.V25_2_AddSqlActivityFlushJob.Version(),
		upgrade.NoPrecondition,
		addSqlActivityFlushJob,
		upgrade.RestoreActionNotRequired("cluster restore does not restore this job"),
	),

	// Note: when starting a new release version, the first upgrade (for
	// Vxy_zStart) must be a newFirstUpgrade. Keep this comment at the bottom.
}

func init() {
	for _, m := range upgrades {
		registry[m.Version()] = m
	}
}
