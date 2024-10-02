// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigkvsubscriberccl

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestSpanConfigUpdatesApplyInCorrectOrder ensures the KVSubscriber applies
// updates to its in-memory store in correct order. In particular, when there
// are both deletions and additions at the same timestamp, the deletions should
// be applied before the additions. This scenario is created by altering from a
// regional by row table to a regional table -- doing so creates overlapping
// updates with both deletions and additions with the same timestamo.
//
// Regression test for https://github.com/cockroachdb/cockroach/issues/110908.
func TestSpanConfigUpdatesApplyInCorrectOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc, _, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(t, 3, base.TestingKnobs{
		Store: &kvserver.StoreTestingKnobs{
			DisableMergeQueue: true, // we don't want the partitions to merge away
		},
		JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(), // speed up schema changes.
	}, multiregionccltestutils.WithScanInterval(50*time.Millisecond))
	defer cleanup()
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0 /* idx */))

	// Speed up the test.
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '50ms'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '50ms'`)

	sqlDB.Exec(t, `CREATE DATABASE mr PRIMARY REGION "us-east1" REGIONS "us-east2", "us-east3"; USE mr`)
	sqlDB.Exec(t, `CREATE TABLE t() LOCALITY REGIONAL BY ROW`)

	testutils.SucceedsSoon(t, func() error {
		var count int
		sqlDB.QueryRow(t,
			"SELECT count(distinct lease_holder) from [show ranges from table t with details]",
		).Scan(&count)
		if count == 3 {
			return nil
		}
		return fmt.Errorf("waiting for each region to pick up leaseholders; count %d", count)
	})

	sqlDB.Exec(t, `ALTER TABLE t SET LOCALITY REGIONAL BY TABLE`)
	testutils.SucceedsSoon(t, func() error {
		var count int
		sqlDB.QueryRow(t,
			"SELECT count(distinct lease_holder) from [show ranges from table t with details]",
		).Scan(&count)
		if count == 1 {
			return nil
		}
		return fmt.Errorf(
			"waiting for all partition leases to move to the primary region; number of regions %d", count,
		)
	})
}
