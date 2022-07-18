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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// GetUpgrade returns the upgrade corresponding to this version if
// one exists.
func GetUpgrade(key clusterversion.ClusterVersion) (upgrade.Upgrade, bool) {
	m, ok := registry[key]
	return m, ok
}

// NoPrecondition is a PreconditionFunc that doesn't check anything.
func NoPrecondition(context.Context, clusterversion.ClusterVersion, upgrade.TenantDeps) error {
	return nil
}

// NoTenantUpgradeFunc is a TenantUpgradeFunc that doesn't do anything.
func NoTenantUpgradeFunc(
	context.Context, clusterversion.ClusterVersion, upgrade.TenantDeps, *jobs.Job,
) error {
	return nil
}

// registry defines the global mapping between a cluster version and the
// associated upgrade. The upgrade is only executed after a cluster-wide
// bump of the corresponding version gate.
var registry = make(map[clusterversion.ClusterVersion]upgrade.Upgrade)

var upgrades = []upgrade.Upgrade{
	upgrade.NewTenantUpgrade(
		"ensure preconditions are met before starting upgrading to v22.1",
		toCV(clusterversion.Start22_1),
		preconditionBeforeStartingAnUpgrade,
		NoTenantUpgradeFunc,
	),
	upgrade.NewTenantUpgrade(
		"add column target to system.protected_ts_records",
		toCV(clusterversion.AlterSystemProtectedTimestampAddColumn),
		NoPrecondition,
		alterTableProtectedTimestampRecords,
	),
	upgrade.NewTenantUpgrade("update synthetic public schemas to be backed by a descriptor",
		toCV(clusterversion.PublicSchemasWithDescriptors),
		NoPrecondition,
		publicSchemaMigration,
	),
	upgrade.NewTenantUpgrade(
		"enable span configs infrastructure",
		toCV(clusterversion.EnsureSpanConfigReconciliation),
		NoPrecondition,
		ensureSpanConfigReconciliation,
	),
	upgrade.NewSystemUpgrade(
		"enable span configs infrastructure",
		toCV(clusterversion.EnsureSpanConfigSubscription),
		ensureSpanConfigSubscription,
	),
	upgrade.NewTenantUpgrade(
		"remove grant privilege from users",
		toCV(clusterversion.RemoveGrantPrivilege),
		NoPrecondition,
		removeGrantMigration,
	),
	upgrade.NewTenantUpgrade(
		"delete comments that belong to dropped indexes",
		toCV(clusterversion.DeleteCommentsWithDroppedIndexes),
		NoPrecondition,
		ensureCommentsHaveNonDroppedIndexes,
	),
	upgrade.NewSystemUpgrade(
		"populate RangeAppliedState.RaftAppliedIndexTerm for all ranges",
		toCV(clusterversion.AddRaftAppliedIndexTermMigration),
		raftAppliedIndexTermMigration,
	),
	upgrade.NewSystemUpgrade(
		"purge all replicas not populating RangeAppliedState.RaftAppliedIndexTerm",
		toCV(clusterversion.PostAddRaftAppliedIndexTermMigration),
		postRaftAppliedIndexTermMigration,
	),
	upgrade.NewTenantUpgrade(
		"add the system.span_count table",
		toCV(clusterversion.SpanCountTable),
		NoPrecondition,
		spanCountTableMigration,
	),
	upgrade.NewTenantUpgrade(
		"seed system.span_count with span count for existing tenants",
		toCV(clusterversion.SeedSpanCountTable),
		NoPrecondition,
		seedSpanCountTableMigration,
	),
	upgrade.NewTenantUpgrade(
		"upgrade sequences to be referenced by ID",
		toCV(clusterversion.UpgradeSequenceToBeReferencedByID),
		NoPrecondition,
		upgradeSequenceToBeReferencedByID,
	),
	upgrade.NewTenantUpgrade(
		"update system.statement_diagnostics_requests to support sampling probabilities",
		toCV(clusterversion.SampledStmtDiagReqs),
		NoPrecondition,
		sampledStmtDiagReqsMigration,
	),
	upgrade.NewTenantUpgrade(
		"add the system.privileges table",
		toCV(clusterversion.SystemPrivilegesTable),
		NoPrecondition,
		systemPrivilegesTableMigration,
	),
	upgrade.NewTenantUpgrade(
		"add column locality to table system.sql_instances",
		toCV(clusterversion.AlterSystemSQLInstancesAddLocality),
		NoPrecondition,
		alterSystemSQLInstancesAddLocality,
	),
	upgrade.NewTenantUpgrade(
		"add the system.external_connections table",
		toCV(clusterversion.SystemExternalConnectionsTable),
		NoPrecondition,
		systemExternalConnectionsTableMigration,
	),
	upgrade.NewTenantUpgrade(
		"add column index_recommendations to table system.statement_statistics",
		toCV(clusterversion.AlterSystemStatementStatisticsAddIndexRecommendations),
		NoPrecondition,
		alterSystemStatementStatisticsAddIndexRecommendations,
	),
}

func init() {
	for _, m := range upgrades {
		if _, exists := registry[m.ClusterVersion()]; exists {
			panic(errors.AssertionFailedf("duplicate upgrade registration for %v", m.ClusterVersion()))
		}
		registry[m.ClusterVersion()] = m
	}
}

func toCV(key clusterversion.Key) clusterversion.ClusterVersion {
	return clusterversion.ClusterVersion{
		Version: clusterversion.ByKey(key),
	}
}
