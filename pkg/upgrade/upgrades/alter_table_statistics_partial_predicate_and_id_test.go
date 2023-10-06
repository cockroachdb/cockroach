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
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestAlterSystemTableStatisticsAddPartialPredicateAndID(t *testing.T) {
	skip.WithIssue(t, 111768, "bump minBinary to 23.1")
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BootstrapVersionKeyOverride:    clusterversion.V22_2,
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1AddPartialStatisticsColumns - 1),
				},
			},
		},
	}

	var (
		ctx   = context.Background()
		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		s     = tc.Server(0)
		sqlDB = tc.ServerConn(0)
	)
	defer tc.Stopper().Stop(ctx)

	var (
		validationSchemas = []upgrades.Schema{
			{Name: "partialPredicate", ValidationFn: upgrades.HasColumn},
			{Name: "fullStatisticID", ValidationFn: upgrades.HasColumn},
		}
	)

	// Validate that the table statistics table has the old schema.
	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.TableStatisticsTableID,
		systemschema.TableStatisticsTable,
		[]string{},
		validationSchemas,
		false, /* expectExists */
	)

	// Run the upgrade.
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V23_1AddPartialStatisticsColumns,
		nil,   /* done */
		false, /* expectError */
	)

	upgrades.ValidateSchemaExists(
		ctx,
		t,
		s,
		sqlDB,
		keys.TableStatisticsTableID,
		systemschema.TableStatisticsTable,
		[]string{},
		validationSchemas,
		true, /* expectExists */
	)
}
