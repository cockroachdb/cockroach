// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// TestInjectDescriptors verifies that descriptors can be extracted from a
// cluster and then injected into another using InjectDescriptors.
func TestInjectDescriptors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// A smattering of schema features.
	setupStmts := []string{
		`CREATE DATABASE db`,
		`CREATE TABLE db.foo (i INT PRIMARY KEY)`,
		`USE db`,
		`CREATE SCHEMA sc`,
		`CREATE TABLE sc.foo (i INT PRIMARY KEY, j INT NOT NULL UNIQUE REFERENCES db.foo(i))`,
		`CREATE TYPE dogs AS ENUM ('jake', 'carl')`,
		`CREATE TYPE cats AS ENUM ('cake', 'jumbotron')`,
		`CREATE TABLE sc.cats_and_dogs (dog dogs, cat cats, PRIMARY KEY (dog, cat))`,
	}
	afterStmts := []string{
		`SELECT * FROM db.foo`,
		`SELECT * FROM db.sc.foo`,
		`SELECT * FROM db.sc.cats_and_dogs WHERE dog = 'jake' AND cat = 'cake'`,
	}

	var descriptors []*descpb.Descriptor
	{
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(context.Background())
		tdb := sqlutils.MakeSQLRunner(db)
		for _, stmt := range setupStmts {
			tdb.Exec(t, stmt)
		}
		var dbID int
		tdb.QueryRow(t, `SELECT id FROM system.namespace WHERE "parentID" = 0 AND name = 'db'`).Scan(&dbID)
		rows := tdb.Query(t, `
  SELECT descriptor
    FROM system.descriptor
   WHERE id >= $1
ORDER BY gen_random_uuid(); -- to mix it up
`, dbID)
		for rows.Next() {
			var encoded []byte
			require.NoError(t, rows.Scan(&encoded))
			var decoded descpb.Descriptor
			require.NoError(t, protoutil.Unmarshal(encoded, &decoded))
			descriptors = append(descriptors, &decoded)
		}
		// Sanity check the afterStmts.
		for _, stmt := range afterStmts {
			tdb.Exec(t, stmt)
		}
	}
	{
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(context.Background())

		require.NoError(t, sqlutils.InjectDescriptors(
			context.Background(), db, descriptors, true, /* force */
		))

		tdb := sqlutils.MakeSQLRunner(db)
		for _, stmt := range afterStmts {
			tdb.Exec(t, stmt)
		}
	}
}
