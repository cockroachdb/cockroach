// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	clustersettings "github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestLeasingClusterVersionStarvation validates that setting
// the cluster version is done with a high priority txn and cannot
// be pushed out. Previously, this would be normal priority and
// get pushed by the leasing code, leading to starvation
// when leases were acquired with sufficiently high frequency
// Note: This test just confirms its not normal priority by checking
// if it can push other txns.
func TestLeasingClusterVersionStarvation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	routineChan := make(chan error)
	waitToStartBump := make(chan struct{})
	resumeBump := make(chan struct{})
	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				UpgradeManager: &upgradebase.TestingKnobs{
					InterlockPausePoint:               upgradebase.AfterVersionBumpRPC,
					InterlockReachedPausePointChannel: &waitToStartBump,
					InterlockResumeChannel:            &resumeBump,
				},
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         clusterversion.MinSupported.Version(),
				},
			},
		},
	}

	st := clustersettings.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.MinSupported.Version(),
		false)
	clusterArgs.ServerArgs.Settings = st
	// Encourage continuous lease renewals intentionally, so that we validate
	// no deadlock risk exists with the settings table. By setting a higher number
	// the expiration will be less than the renewal interval.g
	lease.LeaseRenewalDuration.Override(ctx, &st.SV, time.Hour)
	tc := testcluster.StartTestCluster(t, 1, clusterArgs)

	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	defer db.Close()

	proceedWithCommit := make(chan struct{})
	// Start a background transaction that will have an intent
	// on the version key inside the settings table, with a
	// normal priority (which should get pushed by the upgrade).
	go func() {
		<-waitToStartBump
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
		resumeBump <- struct{}{}
		for retry := retry.Start(retry.Options{}); retry.Next(); {
			_, err = tx.Exec("SELECT name from system.settings where name='version' FOR UPDATE")
			if err != nil {
				rollbackErr := tx.Rollback()
				routineChan <- errors.WithSecondaryError(err, rollbackErr)
				return
			}
		}
	}()

	upgrades.Upgrade(
		t,
		db,
		clusterversion.Latest,
		nil,
		false,
	)

	// Our txn should have been pushed by the upgrade,
	// which has a higher txn priority.
	close(proceedWithCommit)
	require.ErrorContainsf(t, <-routineChan, "pq: restart transaction: TransactionRetryWithProtoRefreshError:",
		"upgrade was not able to push transaction")
}
