// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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
