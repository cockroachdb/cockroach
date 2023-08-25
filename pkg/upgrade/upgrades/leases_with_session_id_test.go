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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestLeaseSessionIDMigration(t *testing.T) {
	skip.UnderStressRace(t)
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	mu := syncutil.Mutex{}
	stmtChan := make(map[string]chan struct{})

	releaseSome := func(final bool) {
		mu.Lock()
		for _, nextChan := range stmtChan {
			nextChan <- struct{}{}
		}
		if final {
			stmtChan = nil
		} else {
			stmtChan = make(map[string]chan struct{})
		}
		mu.Unlock()
	}

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLDeclarativeSchemaChanger: &scexec.TestingKnobs{
					BeforeStage: func(p scplan.Plan, stageIdx int) error {
						if p.Params.ExecutionPhase == scop.PostCommitNonRevertiblePhase ||
							p.Params.ExecutionPhase == scop.PostCommitPhase {
							var waitChan chan struct{}
							mu.Lock()
							if stmtChan != nil {
								waitChan = make(chan struct{})
								stmtChan[p.Statements[0].Statement] = waitChan
							}
							mu.Unlock()
							if waitChan != nil {
								<-waitChan
							}
						}
						return nil
					},
				},
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride: clusterversion.ByKey(
						clusterversion.V23_2_LeaseToSessionCreation - 1),
				},
			},
		},
	}
	// Force frequent lease renewals, so that refreshes are exercised
	// until the cut off.
	clusterArgs.ServerArgs.Settings = cluster.MakeClusterSettings()
	lease.LeaseDuration.Override(ctx, &clusterArgs.ServerArgs.Settings.SV, time.Second)

	tc := testcluster.StartTestCluster(t, 1, clusterArgs)

	defer tc.Stopper().Stop(ctx)
	db := tc.ServerConn(0)
	defer db.Close()

	grp := ctxgroup.WithContext(ctx)
	_, err := db.Exec("CREATE TABLE t_select()")
	require.NoError(t, err)
	// Start a transaction due to the life span, leases
	// should migrate through this.
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	// Run a select statement against a table, with a txn holding it.
	runSelect := func() {
		_, err = tx.Exec("SELECT * from t_select")
		require.NoError(t, err)
	}
	// Start a new DDL and release any waiters.
	startAndRelease := func(name string) {
		runSelect()
		releaseSome(false /*not final */)
		grp.Go(func() error {
			_, err := db.Exec(fmt.Sprintf("CREATE TABLE %s()", name))
			if err != nil {
				return err
			}
			_, err = db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN j int", name))
			return err
		})
		runSelect()
		releaseSome(false)
	}
	startAndRelease("t0" /* not final */)
	upgrades.Upgrade(
		t,
		db,
		clusterversion.V23_2_LeaseToSessionCreation,
		nil,
		false,
	)
	startAndRelease("t2")
	upgrades.Upgrade(
		t,
		db,
		clusterversion.V23_2_LeaseWillOnlyHaveSessions,
		nil,
		false,
	)
	startAndRelease("t3")
	upgrades.Upgrade(
		t,
		db,
		clusterversion.V23_2_LeasesTableWithNewDesc,
		nil,
		false,
	)
	startAndRelease("t4")
	// Release everything else.
	releaseSome(true /* release everything */)
	require.NoError(t, grp.Wait())

	// New column should be visible now.
	_, err = db.Exec(`SELECT "sessionID" FROM system.lease`)
	require.NoError(t, err, "system.leases has session id")
	// Transactions shouldn't be interrupted.
	require.NoError(t, tx.Commit(), "txn should not be interrupted")

}
