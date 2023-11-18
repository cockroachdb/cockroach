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
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRemovePartitioningExpiredLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase:       "d",
		DefaultTestTenant: base.TODOTestTenantDisabled,
	})
	defer s.Stopper().Stop(ctx)

	// Create a partitioned table and index.
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE t (a INT PRIMARY KEY) PARTITION BY LIST (a) (
		PARTITION	p1 VALUES IN (1)
	)`)
	sqlDB.Exec(t, `CREATE INDEX i ON t (a) PARTITION BY RANGE (a) (
		PARTITION p34 VALUES FROM (3) TO (4)
	)`)
	sqlDB.Exec(t, `ALTER PARTITION p1 OF TABLE t CONFIGURE ZONE USING DEFAULT`)
	sqlDB.Exec(t, `ALTER PARTITION p34 OF INDEX t@i CONFIGURE ZONE USING DEFAULT`)
	sqlDB.Exec(t, `ALTER INDEX t@t_pkey CONFIGURE ZONE USING DEFAULT`)
	sqlDB.Exec(t, `ALTER INDEX t@i CONFIGURE ZONE USING DEFAULT`)

	// Remove the enterprise license.
	defer utilccl.TestingDisableEnterprise()()

	const partitionErr = "use of partitions requires an enterprise license"
	const zoneErr = "use of replication zones on indexes or partitions requires an enterprise license"
	expectErr := func(q string, expErr string) {
		t.Helper()
		sqlDB.ExpectErr(t, expErr, q)
	}

	// Partitions and zone configs cannot be modified without a valid license.
	expectErr(`ALTER TABLE t PARTITION BY LIST (a) (PARTITION p2 VALUES IN (2))`, partitionErr)
	expectErr(`ALTER INDEX t@i PARTITION BY RANGE (a) (PARTITION p45 VALUES FROM (4) TO (5))`, partitionErr)
	expectErr(`ALTER PARTITION p1 OF TABLE t CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER PARTITION p34 OF INDEX t@i CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER INDEX t@t_pkey CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER INDEX t@i CONFIGURE ZONE USING DEFAULT`, zoneErr)

	// But they can be removed.
	sqlDB.Exec(t, `ALTER TABLE t PARTITION BY NOTHING`)
	sqlDB.Exec(t, `ALTER INDEX t@i PARTITION BY NOTHING`)
	sqlDB.Exec(t, `ALTER INDEX t@t_pkey CONFIGURE ZONE DISCARD`)
	sqlDB.Exec(t, `ALTER INDEX t@i CONFIGURE ZONE DISCARD`)

	// Once removed, they cannot be added back.
	expectErr(`ALTER TABLE t PARTITION BY LIST (a) (PARTITION p2 VALUES IN (2))`, partitionErr)
	expectErr(`ALTER INDEX t@i PARTITION BY RANGE (a) (PARTITION p45 VALUES FROM (4) TO (5))`, partitionErr)
	expectErr(`ALTER INDEX t@t_pkey CONFIGURE ZONE USING DEFAULT`, zoneErr)
	expectErr(`ALTER INDEX t@i CONFIGURE ZONE USING DEFAULT`, zoneErr)
}

// Test that dropping an enum value fails if there's a concurrent index drop
// for an index partitioned by that enum value. The reason is that it
// would be bad if we rolled back the dropping of the index.
func TestDropEnumValueWithConcurrentPartitionedIndexDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var s serverutils.TestServerInterface
	var sqlDB *gosql.DB

	// Use the dropCh to block any DROP INDEX job until the channel is closed.
	dropCh := make(chan chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	s, sqlDB, _ = serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeResume: func(jobID jobspb.JobID) error {
					var isDropJob bool
					if err := sqlDB.QueryRow(`
SELECT count(*) > 0
  FROM [SHOW JOB $1]
 WHERE description LIKE 'DROP INDEX %'
`, jobID).Scan(&isDropJob); err != nil {
						return err
					}
					if !isDropJob {
						return nil
					}
					ch := make(chan struct{})
					select {
					case dropCh <- ch:
					case <-ctx.Done():
						return ctx.Err()
					}
					select {
					case <-ch:
					case <-ctx.Done():
						return ctx.Err()
					}
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())
	defer cancel()
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Set up the table to have an index which is partitioned by the enum value
	// we're going to drop.
	for _, stmt := range []string{
		`CREATE TYPE t AS ENUM ('a', 'b', 'c')`,
		`CREATE TABLE tbl (
    i INT8, k t,
    PRIMARY KEY (i, k),
    INDEX idx (k)
        PARTITION BY RANGE (k)
            (PARTITION a VALUES FROM ('a') TO ('b'))
)`,
	} {
		tdb.Exec(t, stmt)
	}
	// Run a transaction to drop the index and the enum value.
	errCh := make(chan error)
	go func() {
		errCh <- crdb.ExecuteTx(ctx, sqlDB, nil, func(tx *gosql.Tx) error {
			if _, err := tx.Exec("drop index tbl@idx;"); err != nil {
				return err
			}
			_, err := tx.Exec("alter type t drop value 'a';")
			return err
		})
	}()
	// Wait until the dropping of the enum value has finished.
	ch := <-dropCh
	testutils.SucceedsSoon(t, func() error {
		var done bool
		tdb.QueryRow(t, `
SELECT bool_and(done)
  FROM (
        SELECT status NOT IN `+jobs.NonTerminalStatusTupleString+` AS done
          FROM [SHOW JOBS]
         WHERE job_type = 'TYPEDESC SCHEMA CHANGE'
       );`).
			Scan(&done)
		if done {
			return nil
		}
		return errors.Errorf("not done")
	})
	// Allow the dropping of the index to proceed.
	close(ch)
	// Ensure we got the right error.
	require.Regexp(t,
		`could not remove enum value "a" as it is being used in the partitioning of index tbl@idx`,
		<-errCh)
}
