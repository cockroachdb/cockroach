// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestCreateComputedIndexesOnSystemSQLStatistics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1AddSQLStatsComputedIndexes - 1),
				},
			},
		},
	}

	var (
		ctx = context.Background()

		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	var (
		validationSchemas = []upgrades.Schema{
			{Name: "execution_count", ValidationFn: upgrades.HasColumn},
			{Name: "execution_count_idx", ValidationFn: upgrades.HasIndex},
			{Name: "service_latency", ValidationFn: upgrades.HasColumn},
			{Name: "service_latency_idx", ValidationFn: upgrades.HasIndex},
			{Name: "cpu_sql_nanos", ValidationFn: upgrades.HasColumn},
			{Name: "cpu_sql_nanos_idx", ValidationFn: upgrades.HasIndex},
			{Name: "contention_time", ValidationFn: upgrades.HasColumn},
			{Name: "contention_time_idx", ValidationFn: upgrades.HasIndex},
			{Name: "total_estimated_execution_time", ValidationFn: upgrades.HasColumn},
			{Name: "total_estimated_execution_time_idx", ValidationFn: upgrades.HasIndex},
			{Name: "p99_latency", ValidationFn: upgrades.HasColumn},
			{Name: "p99_latency_idx", ValidationFn: upgrades.HasIndex},
			{Name: "primary", ValidationFn: upgrades.OnlyHasColumnFamily},
		}
	)

	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_1AddSQLStatsComputedIndexes,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the tables have new schemas.
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.StatementStatisticsTableID,
		systemschema.StatementStatisticsTable,
		[]string{},
		validationSchemas,
		true, /* expectExists */
	)
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.TransactionStatisticsTableID,
		systemschema.TransactionStatisticsTable,
		[]string{},
		validationSchemas,
		true, /* expectExists */
	)
}
