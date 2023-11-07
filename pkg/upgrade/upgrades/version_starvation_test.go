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
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestLeasingClusterVersionStarvation validates that setting
// the cluster version is done with a high priority txn and cannot
// be pushed out. Previously, this would be normal priority and
// get pushed by the leasing code. Note: This test just confirms
// its not normal priority by checking if it can push other txns.
func TestLeasingClusterVersionStarvation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	routineChan := make(chan error)
	startRoutineOnce := atomic.Bool{}
	var bgRoutine func()
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				UpgradeManager: &upgradebase.TestingKnobs{
					ListBetweenOverride: func(from, to roachpb.Version) []roachpb.Version {
						// Using this to detect when the upgrade actually starts.
						if startRoutineOnce.Swap(false) {
							go bgRoutine()
						}
						return clusterversion.ListBetween(from, to)
					},
				},
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_2),
				},
			},
		},
	}

	tc := testcluster.StartTestCluster(t, 1, clusterArgs)

	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	defer db.Close()

	proceedWithCommit := make(chan struct{})
	// Start a background transaction that will have an intent
	// on the version key inside the settings table, with a
	// normal priority (which should get pushed by the upgrade).
	bgRoutine = func() {
		tx, err := db.Begin()
		if err != nil {
			routineChan <- err
			return
		}
		_, err = tx.Exec("SELECT name from system.settings where name='version' FOR UPDATE")
		if err != nil {
			routineChan <- err
			return
		}
		<-proceedWithCommit
		routineChan <- tx.Commit()
	}
	startRoutineOnce.Swap(true)

	upgrades.Upgrade(
		t,
		db,
		clusterversion.V24_1,
		nil,
		false,
	)

	// Our txn should have been pushed by the upgrade,
	// which has a higher txn priority.
	close(proceedWithCommit)
	require.ErrorContainsf(t, <-routineChan, "pq: restart transaction: TransactionRetryWithProtoRefreshError:",
		"upgrade was not able to push transaction")
}
