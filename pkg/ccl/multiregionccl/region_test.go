// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSettingPrimaryRegionAmidstDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	numServers := 2
	serverArgs := make(map[int]base.TestServerArgs)

	regionNames := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		// "us-east1", "us-east2"...
		regionNames[i] = fmt.Sprintf("us-east%d", i+1)
	}

	var mu syncutil.Mutex
	dropRegionStarted := make(chan struct{})
	waitForPrimaryRegionSwitch := make(chan struct{})
	dropRegionFinished := make(chan struct{})
	for i := 0; i < numServers; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
					RunBeforeEnumMemberPromotion: func() {
						mu.Lock()
						defer mu.Unlock()
						if dropRegionStarted != nil {
							close(dropRegionStarted)
							<-waitForPrimaryRegionSwitch
							dropRegionStarted = nil
						}
					},
				},
			},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: regionNames[i]}},
			},
		}
	}

	tc := serverutils.StartNewTestCluster(t, numServers, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})

	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	sqlDB := tc.ServerConn(0)

	// Setup the test.
	_, err := sqlDB.Exec(`CREATE DATABASE db WITH PRIMARY REGION "us-east1" REGIONS "us-east2"`)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		if _, err := sqlDB.Exec(`ALTER DATABASE db DROP REGION "us-east2"`); err != nil {
			t.Error(err)
		}
		close(dropRegionFinished)
	}()

	// Wait for the drop region to start and move the enum member "us-east2" in
	// read-only state.
	<-dropRegionStarted

	_, err = sqlDB.Exec(`ALTER DATABASE db PRIMARY REGION "us-east2"`)

	if err == nil {
		t.Fatalf("expected error, found nil")
	}
	if !testutils.IsError(err, `"us-east2" has not been added to the database`) {
		t.Fatalf(`expected err, got %v`, err)
	}

	close(waitForPrimaryRegionSwitch)
	<-dropRegionFinished

	// We expect the async region drop job to succeed soon.
	testutils.SucceedsSoon(t, func() error {
		rows, err := sqlDB.Query("SELECT region FROM [SHOW REGIONS FROM DATABASE db]")
		if err != nil {
			return err
		}
		defer rows.Close()

		const expectedRegion = "us-east1"
		var region string
		rows.Next()
		if err := rows.Scan(&region); err != nil {
			return err
		}

		if region != expectedRegion {
			return errors.Newf("expected region to be: %q, got %q", expectedRegion, region)
		}

		if rows.Next() {
			return errors.New("unexpected number of rows returned")
		}
		return nil
	})
}

// TestDroppingPrimaryRegionAsyncJobFailure drops the primary region of the
// database, which results in dropping the multi-region type descriptor. Then,
// it errors out the async job associated with the type descriptor cleanup and
// ensures the namespace entry is reclaimed back despite the injected error.
// We rely on this behavior to be able to add multi-region capability in the
// future.
func TestDroppingPrimaryRegionAsyncJobFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Decrease the adopt loop interval so that retries happen quickly.
	defer sqltestutils.SetTestJobsAdoptInterval()()

	params, _ := tests.CreateTestServerParams()
	params.Locality.Tiers = []roachpb.Tier{
		{Key: "region", Value: "us-east-1"},
	}

	// Protects expectedCleanupRuns
	var mu syncutil.Mutex
	// We need to cleanup 2 times, once for the multi-region type descriptor and
	// once for the array alias of the multi-region type descriptor.
	haveWePerformedFirstRoundOfCleanup := false
	cleanupFinished := make(chan struct{})
	params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
		RunBeforeExec: func() error {
			return errors.New("yikes")
		},
		RunAfterOnFailOrCancel: func() error {
			mu.Lock()
			defer mu.Unlock()
			if haveWePerformedFirstRoundOfCleanup {
				close(cleanupFinished)
			}
			haveWePerformedFirstRoundOfCleanup = true
			return nil
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Setup the test.
	_, err := sqlDB.Exec(`
CREATE DATABASE db WITH PRIMARY REGION "us-east-1"; 
CREATE TABLE db.t(k INT) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;
`)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`ALTER DATABASE db DROP REGION "us-east-1"`)
	testutils.IsError(err, "yikes")

	<-cleanupFinished

	rows := sqlDB.QueryRow(`SELECT count(*) FROM system.namespace WHERE name = 'crdb_internal_region'`)
	var count int
	err = rows.Scan(&count)
	require.NoError(t, err)
	if count != 0 {
		t.Fatal("expected crdb_internal_region not to be present in system.namespace")
	}

	_, err = sqlDB.Exec(`ALTER DATABASE db PRIMARY REGION "us-east-1"`)
	require.NoError(t, err)

	rows = sqlDB.QueryRow(`SELECT count(*) FROM system.namespace WHERE name = 'crdb_internal_region'`)
	err = rows.Scan(&count)
	require.NoError(t, err)
	if count != 1 {
		t.Fatal("expected crdb_internal_region to be present in system.namespace")
	}
}

// TODO(arul): Currently, database level zone configurations aren't rolled back.
// When we fix this limitation we should also update this test to check for that.
func TestRollbackDuringAddRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Decrease the adopt loop interval so that retries happen quickly.
	defer sqltestutils.SetTestJobsAdoptInterval()()

	numServers := 2
	serverArgs := make(map[int]base.TestServerArgs)

	regionNames := make([]string, numServers)
	for i := 0; i < numServers; i++ {
		// "us-east1", "us-east2"...
		regionNames[i] = fmt.Sprintf("us-east%d", i+1)
	}

	for i := 0; i < numServers; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLTypeSchemaChanger: &sql.TypeSchemaChangerTestingKnobs{
					RunBeforeExec: func() error {
						return errors.New("boom")
					},
				},
			},
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: regionNames[i]}},
			},
		}
	}

	tc := serverutils.StartNewTestCluster(t, numServers, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})

	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	sqlDB := tc.ServerConn(0)

	_, err := sqlDB.Exec(`CREATE DATABASE db WITH PRIMARY REGION "us-east1"`)
	require.NoError(t, err)
	if err != nil {
		t.Error(err)
	}

	_, err = sqlDB.Exec(`ALTER DATABASE db ADD REGION "us-east2"`)
	require.Error(t, err, "boom")

	var jobStatus string
	var jobErr string
	row := sqlDB.QueryRow("SELECT status, error FROM [SHOW JOBS] WHERE job_type = 'TYPEDESC SCHEMA CHANGE'")
	require.NoError(t, row.Scan(&jobStatus, &jobErr))
	require.Regexp(t, "boom", jobErr)
	require.Regexp(t, "failed", jobStatus)
}
