// Copyright 2022 The Cockroach Authors.
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

func TestAlterSystemSqlInstancesTableAddSqlAddr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BootstrapVersionKeyOverride:    clusterversion.V22_2,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1AlterSystemSQLInstancesAddSQLAddr - 1),
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
			{Name: "sql_addr", ValidationFn: upgrades.HasColumn},
		}
	)

	sqlInstancesTable := systemschema.SQLInstancesTable()
	// Validate that the table sql_instances has the old schema.
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.SQLInstancesTableID,
		systemschema.SQLInstancesTable(),
		[]string{},
		validationSchemas,
		false, /* expectExists */
	)
	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_1AlterSystemSQLInstancesAddSQLAddr,
		nil,   /* done */
		false, /* expectError */
	)
	// Validate that the table has new schema.
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.SQLInstancesTableID,
		sqlInstancesTable,
		[]string{},
		validationSchemas,
		true, /* expectExists */
	)
}
