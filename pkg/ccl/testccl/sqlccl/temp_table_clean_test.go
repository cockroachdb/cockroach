// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlccl_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func TestTenantTempTableCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "May cause a conn executor race fixed by #114783")

	ctx := context.Background()
	t.Helper()
	// Knob state is used to track when temporary object clean up
	// is executed.
	var (
		knobState struct {
			syncutil.Mutex
			paused   bool
			finishCh chan struct{}
		}
		knobStateLocked = func(f func()) {
			knobState.Lock()
			defer knobState.Unlock()
			f()
		}
		// Enables pausing of temp object cleanups
		// until signaled.
		pause = func() {
			knobStateLocked(func() {
				knobState.paused, knobState.finishCh = true, make(chan struct{})
			})
		}
		// Disables pausing of temp object cleanups.
		unpause = func() {
			knobStateLocked(func() {
				close(knobState.finishCh)
				knobState.paused, knobState.finishCh = false, nil
			})
		}
		getPaused = func() (paused bool) {
			knobStateLocked(func() { paused = knobState.paused })
			return paused
		}
		getFinishCh = func() (finishCh chan struct{}) {
			knobStateLocked(func() { finishCh = knobState.finishCh })
			return finishCh
		}
		// Waits for temp object cleanup, we intentionally
		// wait for two cycles which will guarantee that any
		// temp object is destroyed.
		waitForCleanup = func() {
			getFinishCh() <- struct{}{}
			getFinishCh() <- struct{}{}
		}
	)
	tenantTempKnobSettings := base.TestingKnobs{
		SQLExecutor: &sql.ExecutorTestingKnobs{
			OnTempObjectsCleanupDone: func() {
				// If pausing is disabled, we don't need
				// to inform anyone.
				if !getPaused() {
					return
				}
				// Indicate that a cleanup has occurred.
				<-getFinishCh()
			},
		},
	}
	tc := serverutils.StartCluster(
		t, 3 /* numNodes */, base.TestClusterArgs{ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
			},
		},
	)
	tenantStoppers := []*stop.Stopper{stop.NewStopper(), stop.NewStopper()}

	tenantSettings := func() *cluster.Settings {
		st := cluster.MakeTestingClusterSettings()
		sql.TempObjectCleanupInterval.Override(ctx, &st.SV, time.Second)
		sql.TempObjectWaitInterval.Override(ctx, &st.SV, time.Second*0)
		// Set up sessions to expire within 5 seconds of a
		// nodes death.
		slbase.DefaultTTL.Override(ctx, &st.SV, 5*time.Second)
		slbase.DefaultHeartBeat.Override(ctx, &st.SV, time.Second)
		return st
	}

	_, tenantPrimaryDB := serverutils.StartTenant(t, tc.Server(0),
		base.TestTenantArgs{
			TenantID:     serverutils.TestTenantID(),
			Settings:     tenantSettings(),
			TestingKnobs: tenantTempKnobSettings,
			Stopper:      tenantStoppers[0],
		})
	tenantSQL := sqlutils.MakeSQLRunner(tenantPrimaryDB)
	pause()

	// Sanity: Temporary table clean up works fine, we are going
	// to start two nodes and close the kill one of
	// them.
	tenantSQL.Exec(t, "SET experimental_enable_temp_tables = 'on'")
	tenantSQL.Exec(t, "set cluster setting sql.temp_object_cleaner.cleanup_interval='1 seconds'")
	tenantSQL.Exec(t, "CREATE TEMP TABLE temp_table (x INT PRIMARY KEY, y INT);")

	_, tenantSecondDB := serverutils.StartTenant(t, tc.Server(1),
		base.TestTenantArgs{
			TenantID: serverutils.TestTenantID(),
			Settings: tenantSettings(),
			Stopper:  tenantStoppers[1],
		})
	tenantSecondSQL := sqlutils.MakeSQLRunner(tenantSecondDB)
	tenantSecondSQL.CheckQueryResults(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{
			{"temp_table"},
		})
	tenantSecondSQL.Exec(t, "SET experimental_enable_temp_tables = 'on'")
	tenantSecondSQL.Exec(t, "CREATE TEMP TABLE temp_table2 (x INT PRIMARY KEY, y INT);")
	waitForCleanup()
	tenantSecondSQL.CheckQueryResults(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{
			{"temp_table"},
			{"temp_table2"},
		})
	// Stop the second node, we should one be left with a single temp table.
	tenantStoppers[1].Stop(ctx)
	// Session should expire in 5 seconds, wait for
	// two clean up cycles just in case, so that we have
	// stable timing.
	waitForCleanup()
	tenantSQL.CheckQueryResultsRetry(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{
			{"temp_table"},
		})
	// Disable our hook to allow the database to be
	// brought down.
	unpause()
	// Close the primary DB, there should be no temporary
	// tables left.
	tenantStoppers[0].Stop(ctx)
	// Enable our hook to allow the database to be
	// brought up.
	pause()
	// Once we restart the tenant, no sessions should exist
	// so all temporary tables should be cleaned up.
	tenantStoppers[0] = stop.NewStopper()
	_, tenantPrimaryDB = serverutils.StartTenant(t, tc.Server(0),
		base.TestTenantArgs{
			TenantID:     serverutils.TestTenantID(),
			Settings:     tenantSettings(),
			TestingKnobs: tenantTempKnobSettings,
			Stopper:      tenantStoppers[0]})
	tenantSQL = sqlutils.MakeSQLRunner(tenantPrimaryDB)
	waitForCleanup()
	tenantSQL.CheckQueryResultsRetry(t, "SELECT table_name FROM [SHOW TABLES]",
		[][]string{})
	// Disable our hook to allow the database to be
	// brought stopped.
	unpause()
	// Bring down the tenants.
	tenantStoppers[0].Stop(ctx)
	tc.Stopper().Stop(ctx)
}
