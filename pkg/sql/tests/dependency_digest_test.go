// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestDependencyDigestOptimization validates that dependency digest information
// is properly invalidated in the face of modifications for prepared queries.
func TestDependencyDigestOptimization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := serverutils.StartCluster(t, 3, base.TestClusterArgs{})

	changeRawConn := c.ServerConn(0)
	prepareRawConn := c.ServerConn(1)

	changeConn := sqlutils.MakeSQLRunner(changeRawConn)
	prepareConn := sqlutils.MakeSQLRunner(prepareRawConn)

	// Setup a database with multiple schemas.
	changeConn.ExecMultiple(t,
		"CREATE USER bob",
		"CREATE DATABASE db1",
		"CREATE DATABASE db2",
		"CREATE SCHEMA db1.sc1",
		"CREATE SCHEMA db1.sc2",
		"CREATE SCHEMA db1.sc3",
		"CREATE SCHEMA db2.sc3",
		"CREATE TABLE db2.sc3.t1(m int)",
		"CREATE TABLE db1.sc3.t1(n int)",
		"CREATE TYPE IF NOT EXISTS db1.sc3.status AS ENUM ('open', 'closed', 'inactive');",
		"USE db1",
		`CREATE FUNCTION db1.sc3.f1 (input INT) RETURNS INT8
                                LANGUAGE SQL
                                AS $$
                                SELECT 1;
                                $$;`,
		"USE db2",
		`CREATE FUNCTION db2.sc3.f1 (input INT) RETURNS INT8
                                LANGUAGE SQL
                                AS $$
                                SELECT 32;
                                $$;`,
		"INSERT INTO db1.sc3.t1 VALUES(1)",
		"INSERT INTO db2.sc3.t1 VALUES(32)",
	)

	// Set up the search_path and a simple select against t1.
	prepareConn.Exec(t, "USE db1")
	prepareConn.Exec(t, "SET search_path=sc1,sc2,sc4,sc3")
	query, err := prepareRawConn.Prepare("SELECT * FROM t1")
	require.NoError(t, err)

	// Confirm the table from sc3 is being selected.
	validateColumnsOnQuery := func(exp int) {
		row := query.QueryRow()
		require.NoError(t, row.Err())
		var val int
		require.NoError(t, row.Scan(&val))
		require.Equal(t, exp, val)
		// Validate the function query next.
		tRow := prepareConn.QueryRow(t, "SELECT f1(32)")
		tRow.Scan(&val)
		require.Equal(t, exp, val)
	}
	// Sanity: Ensure that sc3.t1 is picked in the original prepare.
	validateColumnsOnQuery(1)
	// Sanity: Ensure the type resolution works.
	prepareConn.Exec(t, "SELECT 'open'::STATUS")
	// Set up a new t1/f1 in sc2, which should be picked earlier in our search_path.
	changeConn.Exec(t, "CREATE TABLE db1.sc2.t1(k int)")
	changeConn.Exec(t, "INSERT INTO db1.sc2.t1 VALUES(2)")
	changeConn.Exec(t, `
USE db1;
CREATE FUNCTION db1.sc2.f1 (input INT) RETURNS INT8
	LANGUAGE SQL
	AS $$
	SELECT 2;
	$$;`)
	// Confirm the table from sc2 is being selected next.
	validateColumnsOnQuery(2)
	// Setup a new type that will not have open.
	changeConn.Exec(t, "CREATE TYPE IF NOT EXISTS db1.sc2.status AS ENUM ('closed', 'inactive');")
	prepareConn.ExpectErr(t, "invalid input value for enum status: \"open\"",

		"SELECT 'open'::STATUS")
	// Alter table db1.sc2.t1 to only have a virtual column
	changeConn.Exec(t, "ALTER TABLE db1.sc2.t1 DROP COLUMN k, ADD COLUMN n INT AS (5) VIRTUAL")
	// Replace the function to have a new value
	changeConn.Exec(t, `
	CREATE OR REPLACE FUNCTION db1.sc2.f1 (input INT) RETURNS INT8
	LANGUAGE SQL
	AS $$
	SELECT 5;
	$$;`)
	validateColumnsOnQuery(5)
	// Modify the search_path so that sc3 is first, and validate the prepared
	// query is properly updated.
	prepareConn.Exec(t, "SET search_path=sc3,sc2,sc4,sc1")
	validateColumnsOnQuery(1)
	// Change our database path next, we should select db2.sc3.t1.
	prepareConn.Exec(t, "USE db2")
	validateColumnsOnQuery(32)
	// Confirm that statistics changes have the same behavior.
	prepareConn.Exec(t, "PREPARE p1 AS (SELECT * FROM t1)")
	explainRows := prepareConn.QueryStr(t, "EXPLAIN ANALYZE EXECUTE p1")
	foundReusedPlan := false
	for _, row := range explainRows {
		// Same as the prepare above, so we don't expect any changes.
		if row[0] == "plan type: generic, reused" {
			foundReusedPlan = true
			break
		}
	}
	require.Truef(t, foundReusedPlan, "did not find reused plan")
	changeConn.Exec(t, "CREATE STATISTICS s1 FROM db2.sc3.t1")
	// Statistics cache is updated via a range feed, so propagation may take
	// time.
	testutils.SucceedsSoon(t, func() error {
		explainRows = prepareConn.QueryStr(t, "EXPLAIN ANALYZE EXECUTE p1")
		foundReOptimizedPlan := false
		for _, row := range explainRows {
			// New stats were generated so the plan should get re-optimized.
			if row[0] == "plan type: generic, re-optimized" {
				foundReOptimizedPlan = true
				break
			}
		}
		if !foundReOptimizedPlan {
			return errors.AssertionFailedf("did not find re-optimized plan")
		}
		return nil
	})

	// Confirm changing the role will have trouble.
	prepareConn.Exec(t, "SET ROLE bob")
	row := query.QueryRow()
	require.Errorf(t, row.Err(), "user bob does not have USAGE privilege on schema sc3")
	defer c.Stopper().Stop(context.Background())

}
