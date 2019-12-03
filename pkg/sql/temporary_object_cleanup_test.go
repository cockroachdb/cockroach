// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTempObjectsDeletionJob(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ch := make(chan time.Time)
	knobs := base.TestingKnobs{
		SQLExecutor: &ExecutorTestingKnobs{
			DisableTempObjectsCleanupOnSessionExit: true,
			TempObjectsCleanupCh:                   ch,
		},
	}
	tc := serverutils.StartTestCluster(t, 2, /* numNodes */
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				UseDatabase: "defaultdb",
				Knobs:       knobs,
			},
		})
	defer tc.Stopper().Stop(context.TODO())

	db := tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET experimental_enable_temp_tables=true`)
	sqlDB.Exec(t, `CREATE TEMP TABLE t (x INT)`)

	// Close the client connection. Normally the temporary data would immediately
	// be cleaned up on session exit, but this is disabled via the
	// DisableTempObjectsCleanupOnSessionExit testing knob.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Sanity check: there should still be one temporary schema present.
	db = tc.ServerConn(1)
	sqlDB = sqlutils.MakeSQLRunner(db)
	tempSchemaQuery := `SELECT count(*) FROM system.namespace WHERE name LIKE 'pg_temp%'`
	var tempSchemaCount int
	sqlDB.QueryRow(t, tempSchemaQuery).Scan(&tempSchemaCount)
	require.Equal(t, tempSchemaCount, 1)

	// Now force a cleanup run (by default, it is every 30mins).
	ch <- time.Now()

	// Verify that the asynchronous cleanup job kicks in and removes the temporary
	// data.
	testutils.SucceedsSoon(t, func() error {
		sqlDB.QueryRow(t, tempSchemaQuery).Scan(&tempSchemaCount)
		if tempSchemaCount > 0 {
			return errors.Errorf("expected 0 temp schemas, found %d", tempSchemaCount)
		}
		return nil
	})
}
