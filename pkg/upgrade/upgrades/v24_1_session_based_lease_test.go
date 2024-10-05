// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

// TestSessionBasedLeaseUpgrade validates that the leasing format
// can be upgraded while queries are running.
func TestSessionBasedLeaseUpgrade(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          (clusterversion.V24_1_SessionBasedLeasingDualWrite - 1).Version(),
				},
			},
		},
	}

	var (
		ctx = context.Background()

		tc    = testcluster.StartTestCluster(t, 1, clusterArgs)
		sqlDB = tc.ServerConn(0)
	)
	runner := sqlutils.MakeSQLRunner(sqlDB)
	defer tc.Stopper().Stop(ctx)

	grp := ctxgroup.WithContext(ctx)
	_, err := sqlDB.Exec("CREATE TABLE t1(n int)")
	require.NoError(t, err)
	startUpgradeChan := make(chan struct{})
	startAlterChan := make(chan struct{})
	waitForCommit := make(chan struct{})
	grp.GoCtx(func(ctx context.Context) error {
		defer close(startAlterChan)
		txn, err := sqlDB.Begin()
		if err != nil {
			return err
		}
		if _, err := txn.Exec("SELECT * FROM t1"); err != nil {
			return err
		}
		startAlterChan <- struct{}{}
		if _, err := txn.Exec("INSERT INTO t1 VALUES(5)"); err != nil {
			return err
		}
		<-waitForCommit
		return txn.Commit()
	})
	grp.GoCtx(func(ctx context.Context) error {
		defer close(startUpgradeChan)
		<-startAlterChan
		txn, err := sqlDB.Begin()
		if err != nil {
			return err
		}
		if _, err := txn.Exec("ALTER TABLE t1 ADD COLUMN j INT DEFAULT 32"); err != nil {
			return err
		}
		startUpgradeChan <- struct{}{}
		return txn.Commit()
	})
	<-startUpgradeChan
	upgrades.Upgrade(
		t,
		sqlDB,
		clusterversion.V24_1_SessionBasedLeasingUpgradeDescriptor,
		nil,
		false,
	)
	waitForCommit <- struct{}{}
	require.NoError(t, grp.Wait())

	// Validate the lease table was converted properly.
	rowsWithSessionID := runner.QueryRow(t, "SELECT count(*) from system.lease WHERE session_id IS NOT NULL")
	rowsWithoutSessionID := runner.QueryRow(t, "SELECT count(*) from system.lease WHERE session_id IS NULL")
	var count int
	rowsWithSessionID.Scan(&count)
	require.Greaterf(t, count, 0, "expected leases to have session ID")
	rowsWithoutSessionID.Scan(&count)
	require.Zerof(t, count, "No leases be missing session ID")
	// Confirm the schema change completed.
	runner.CheckQueryResults(t, "SELECT * FROM t1",
		[][]string{{"5", "32"}})
	// Validate the final descriptor matches what we expect from a brand new one.
	cf := tc.Servers[0].CollectionFactory().(*descs.CollectionFactory)
	descs := cf.NewCollection(ctx)
	defer descs.ReleaseAll(ctx)
	txn := tc.Servers[0].DB().NewTxn(ctx, "query-desc")
	leaseTableDesc, err := descs.ByID(txn).Get().Table(ctx, keys.LeaseTableID)
	require.NoError(t, err)
	// Copy over the version and modification time.
	expectedDesc := systemschema.LeaseTable().TableDesc()
	expectedDesc.ModificationTime = leaseTableDesc.TableDesc().ModificationTime
	expectedDesc.Version = leaseTableDesc.GetVersion()

	// The V24_1_SystemDatabaseSurvivability sets this flag to true, but this
	// test only upgrades up to V24_1_SessionBasedLeasingUpgradeDescriptor, so
	// the flag should still be false.
	expectedDesc.ExcludeDataFromBackup = false

	if !leaseTableDesc.TableDesc().Equal(expectedDesc) {
		diff := strings.Join(pretty.Diff(leaseTableDesc.TableDesc(), expectedDesc), "\n")
		t.Errorf("system.lease table descriptor from upgrade does not match:\n%s", diff)
	}
}
