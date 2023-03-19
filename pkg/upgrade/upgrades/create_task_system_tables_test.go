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
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestTaskTablesMigration(t *testing.T) {
	skip.UnderStressRace(t)
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_1_TaskSystemTables - 1),
				},
			},
		},
	}

	tc := testcluster.StartTestCluster(t, 1, clusterArgs)

	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	defer db.Close()

	upgrades.Upgrade(
		t,
		db,
		clusterversion.V23_1_TaskSystemTables,
		nil,
		false,
	)

	_, err := db.Exec("SELECT * FROM system.task_payloads")
	assert.NoError(t, err, "system.task_payloads exists")

	_, err = db.Exec("SELECT * FROM system.tenant_tasks")
	assert.NoError(t, err, "system.tenant_tasks exists")
}
