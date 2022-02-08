// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package partitionccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

func subzoneExists(cfg *zonepb.ZoneConfig, index uint32, partition string) bool {
	for _, s := range cfg.Subzones {
		if s.IndexID == index && s.PartitionName == partition {
			return true
		}
	}
	return false
}

func TestDropIndexWithZoneConfigCCL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numRows = 100

	defer gcjob.SetSmallMaxGCIntervalForTest()()

	asyncNotification := make(chan struct{})

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		GCJob: &sql.GCJobTestingKnobs{
			RunBeforeResume: func(_ jobspb.JobID) error {
				<-asyncNotification
				return nil
			},
		},
	}
	s, sqlDBRaw, kvDB := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	defer s.Stopper().Stop(context.Background())

	// Create a test table with a partitioned secondary index.
	if err := tests.CreateKVTable(sqlDBRaw, "kv", numRows); err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, `CREATE INDEX i ON t.kv (v) PARTITION BY LIST (v) (
		PARTITION p1 VALUES IN (1),
		PARTITION p2 VALUES IN (2)
	)`)
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	index, err := tableDesc.FindIndexWithName("i")
	if err != nil {
		t.Fatal(err)
	}
	indexSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, index.GetID())
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)

	// Set zone configs on the primary index, secondary index, and one partition
	// of the secondary index.
	ttlYaml := "gc: {ttlseconds: 1}"
	sqlutils.SetZoneConfig(t, sqlDB, "INDEX t.kv@kv_pkey", "")
	sqlutils.SetZoneConfig(t, sqlDB, "INDEX t.kv@i", ttlYaml)
	sqlutils.SetZoneConfig(t, sqlDB, "PARTITION p2 OF INDEX t.kv@i", ttlYaml)

	// Drop the index and verify that the zone config for the secondary index and
	// its partition are removed but the zone config for the primary index
	// remains.
	sqlDB.Exec(t, `DROP INDEX t.kv@i`)
	// All zone configs should still exist.
	var buf []byte
	cfg := &zonepb.ZoneConfig{}
	sqlDB.QueryRow(t, "SELECT config FROM system.zones WHERE id = $1", tableDesc.GetID()).Scan(&buf)
	if err := protoutil.Unmarshal(buf, cfg); err != nil {
		t.Fatal(err)
	}

	subzones := []struct {
		index     uint32
		partition string
	}{
		{1, ""},
		{4, ""},
		{4, "p2"},
	}
	for _, target := range subzones {
		if exists := subzoneExists(cfg, target.index, target.partition); !exists {
			t.Fatalf(`zone config for %v does not exist`, target)
		}
	}
	// Dropped indexes waiting for GC shouldn't have their zone configs be visible
	// using SHOW ZONE CONFIGURATIONS ..., but still need to exist in system.zones.
	for _, target := range []string{"t.kv@i", "t.kv.p2"} {
		if exists := sqlutils.ZoneConfigExists(t, sqlDB, target); exists {
			t.Fatalf(`zone config for %s still exists`, target)
		}
	}
	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if _, err := tableDesc.FindIndexWithName("i"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
	close(asyncNotification)

	// Wait for index drop to complete so zone configs are updated.
	testutils.SucceedsSoon(t, func() error {
		if kvs, err := kvDB.Scan(context.Background(), indexSpan.Key, indexSpan.EndKey, 0); err != nil {
			return err
		} else if l := 0; len(kvs) != l {
			return errors.Errorf("expected %d key value pairs, but got %d", l, len(kvs))
		}
		sqlDB.QueryRow(t, "SELECT config FROM system.zones WHERE id = $1", tableDesc.GetID()).Scan(&buf)
		if err := protoutil.Unmarshal(buf, cfg); err != nil {
			return err
		}
		if exists := subzoneExists(cfg, 1, ""); !exists {
			return errors.New("zone config for primary index removed after dropping secondary index")
		}
		for _, target := range subzones[1:] {
			if exists := subzoneExists(cfg, target.index, target.partition); exists {
				return fmt.Errorf(`zone config for %v still exists`, target)
			}
		}
		return nil
	})
}

// TestDropIndexPartitionedByUserDefinedTypeCCL is a regression test to ensure
// that dropping an index partitioned by a user-defined types is safe.
func TestDropIndexPartitionedByUserDefinedTypeCCL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	waitForJobDone := func(t *testing.T, tdb *sqlutils.SQLRunner, description string) {
		t.Helper()
		var id int
		tdb.QueryRow(t, `
SELECT job_id
  FROM crdb_internal.jobs
 WHERE description LIKE $1
`, description).Scan(&id)
		testutils.SucceedsSoon(t, func() error {
			var status string
			tdb.QueryRow(t,
				`SELECT status FROM [SHOW JOB $1]`,
				id,
			).Scan(&status)
			if status != string(jobs.StatusSucceeded) {
				return errors.Errorf("expected %q, got %q", jobs.StatusSucceeded, status)
			}
			return nil
		})
	}

	// This is a regression test for a bug which was caused by not using hydrated
	// descriptors in the index gc job to re-write zone config subzone spans.
	// This test ensures that subzone spans can be re-written by creating a
	// table partitioned by user-defined types and then dropping an index and
	// ensuring that the drop job for the index completes successfully.
	t.Run("drop index, type-partitioned table", func(t *testing.T) {
		// Sketch of the test:
		//
		//  * Set up a partitioned table partitioned by an enum.
		//  * Create an index.
		//  * Set a short GC TTL on the index.
		//  * Drop the index.
		//  * Wait for the index to be cleaned up, which would have crashed before the
		//    this fix

		defer log.Scope(t).Close(t)
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
		defer tc.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

		tdb.Exec(t, `
	  CREATE TYPE typ AS ENUM ('a', 'b', 'c');
	  CREATE TABLE t (e typ PRIMARY KEY) PARTITION BY LIST (e) (
	              PARTITION a VALUES IN ('a'),
	              PARTITION b VALUES IN ('b'),
	              PARTITION c VALUES IN ('c')
	  );
	  CREATE INDEX idx ON t (e);
	  ALTER PARTITION a OF TABLE t CONFIGURE ZONE USING range_min_bytes = 123456, range_max_bytes = 654321;
	  ALTER INDEX t@idx CONFIGURE ZONE USING gc.ttlseconds = 1;
	  DROP INDEX t@idx;
	  `)

		waitForJobDone(t, tdb, "GC for DROP INDEX%idx")
	})

	// This is a regression test for a hazardous scenario whereby a drop index gc
	// job may attempt to rewrite subzone spans for a dropped table which used types
	// which no longer exist.
	t.Run("drop table and type", func(t *testing.T) {

		// Sketch of the test:
		//
		//  * Set up a partitioned table and index which are partitioned by an enum.
		//  * Set a short GC TTL on the index.
		//  * Drop the index.
		//  * Drop the table.
		//  * Drop the type.
		//  * Wait for the index to be cleaned up, which would have crashed before the
		//    this fix.
		//  * Set a short GC TTL on everything.
		//  * Wait for the table to be cleaned up.
		//

		defer log.Scope(t).Close(t)
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
		defer tc.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

		tdb.Exec(t, `
	  CREATE TYPE typ AS ENUM ('a', 'b', 'c');
	  CREATE TABLE t (e typ PRIMARY KEY) PARTITION BY LIST (e) (
	              PARTITION a VALUES IN ('a'),
	              PARTITION b VALUES IN ('b'),
	              PARTITION c VALUES IN ('c')
	  );
	  CREATE INDEX idx
	      ON t (e)
	      PARTITION BY LIST (e)
	          (
	              PARTITION ai VALUES IN ('a'),
	              PARTITION bi VALUES IN ('b'),
	              PARTITION ci VALUES IN ('c')
	          );
	  ALTER PARTITION ai OF INDEX t@idx CONFIGURE ZONE USING range_min_bytes = 123456, range_max_bytes = 654321;
	  ALTER PARTITION a OF TABLE t CONFIGURE ZONE USING range_min_bytes = 123456, range_max_bytes = 654321;
	  ALTER INDEX t@idx CONFIGURE ZONE USING gc.ttlseconds = 1;
	  DROP INDEX t@idx;
	  DROP TABLE t;
	  DROP TYPE typ;
	  `)

		waitForJobDone(t, tdb, "GC for DROP INDEX%idx")
		tdb.Exec(t, `ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 1`)
		waitForJobDone(t, tdb, "GC for dropping descriptor %")
	})
}
