// Copyright 2018 The Cockroach Authors.
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
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestXiang(t *testing.T) {
	params, _ := tests.CreateTestServerParams()
	params.Settings = cluster.MakeTestingClusterSettingsWithVersions(clusterversion.ByKey(clusterversion.V23_1),
		clusterversion.ByKey(clusterversion.V22_2),
		false, /* initializeVersion */
	)
	params.Knobs.Server = &server.TestingKnobs{
		DisableAutomaticVersionUpgrade: make(chan struct{}),
		BinaryVersionOverride:          clusterversion.ByKey(clusterversion.V22_2),
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(t, "set use_declarative_schema_changer = on;")
	tdb.Exec(t, "create table t (i int primary key);")
	tdb.Exec(t, "alter table t add column j int not null;")
	//tdb.Exec(t, "alter table t add column k int unique not null")
	result := tdb.QueryStr(t, "show create table t")
	fmt.Printf("Xiang: result = \n%v\n", result)
}

func TestXiang2(t *testing.T) {
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(t, "set use_declarative_schema_changer = on;")
	tdb.Exec(t, "CREATE TABLE t1 (id1 INT PRIMARY KEY, id2 INT, id3 INT NOT NULL)")
	tdb.Exec(t, "CREATE TABLE t2 (id1 INT PRIMARY KEY)")
	tdb.Exec(t, "ALTER TABLE t1 ADD FOREIGN KEY (id2) REFERENCES t1(id1);")
	tdb.Exec(t, "ALTER TABLE t1 ADD FOREIGN KEY (id3) REFERENCES t2(id1);")
	tdb.Exec(t, "drop table t1;")
}

func TestXiang3(t *testing.T) {
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(t, "set use_declarative_schema_changer = on;")
	tdb.Exec(t, "CREATE TABLE t (i int primary key)")
	tdb.Exec(t, "ALTER TABLE t ADD COLUMN j INT NOT NULL")
	tdb.Exec(t, "alter table t drop column j")
}

func TestXiang4(t *testing.T) {
	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(t, "set use_declarative_schema_changer = on;")
	tdb.Exec(t, "CREATE TABLE t (i int primary key)")
	tdb.Exec(t, "ALTER TABLE t ADD COLUMN j INT GENERATED ALWAYS AS IDENTITY;")
	tdb.Exec(t, "alter table t drop column j")
}

func TestCommentOnColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	runCommentOnTests(t, func(db *gosql.DB) {
		if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (c1 INT, c2 INT, c3 INT);
	`); err != nil {
			t.Fatal(err)
		}

		testCases := []struct {
			exec   string
			query  string
			expect gosql.NullString
		}{
			{
				`COMMENT ON COLUMN t.c1 IS 'foo'`,
				`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c1'`,
				gosql.NullString{String: `foo`, Valid: true},
			},
			{
				`TRUNCATE t`,
				`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c1'`,
				gosql.NullString{String: `foo`, Valid: true},
			},
			{
				`ALTER TABLE t RENAME COLUMN c1 TO c1_1`,
				`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c1_1'`,
				gosql.NullString{String: `foo`, Valid: true},
			},
			{
				`COMMENT ON COLUMN t.c1_1 IS NULL`,
				`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c1_1'`,
				gosql.NullString{Valid: false},
			},
			{
				`COMMENT ON COLUMN t.c3 IS 'foo'`,
				`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c3'`,
				gosql.NullString{String: `foo`, Valid: true},
			},
			{
				`ALTER TABLE t DROP COLUMN c2`,
				`SELECT col_description(attrelid, attnum) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c3'`,
				gosql.NullString{String: `foo`, Valid: true},
			},
		}

		for _, tc := range testCases {
			if _, err := db.Exec(tc.exec); err != nil {
				t.Fatal(err)
			}

			row := db.QueryRow(tc.query)
			var comment gosql.NullString
			if err := row.Scan(&comment); err != nil {
				t.Fatal(err)
			}
			if tc.expect != comment {
				t.Fatalf("expected comment %v, got %v", tc.expect, comment)
			}
		}
	})
}

func TestCommentOnColumnTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	runCommentOnTests(t, func(db *gosql.DB) {
		if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (c INT);
		BEGIN;
		ALTER TABLE t ADD COLUMN x INT;
		COMMENT ON COLUMN t.x IS 'foo';
		COMMIT;
	`); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCommentOnColumnWhenDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	runCommentOnTests(t, func(db *gosql.DB) {
		if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (c INT);
	`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`COMMENT ON COLUMN t.c IS 'foo'`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`DROP TABLE t`); err != nil {
			t.Fatal(err)
		}

		row := db.QueryRow(`SELECT comment FROM system.comments LIMIT 1`)
		var comment string
		err := row.Scan(&comment)
		if !errors.Is(err, gosql.ErrNoRows) {
			if err != nil {
				t.Fatal(err)
			}

			t.Fatal("comment remain")
		}
	})
}

func TestCommentOnColumnWhenDropColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	runCommentOnTests(t, func(db *gosql.DB) {
		if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (c INT);
	`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`COMMENT ON COLUMN t.c IS 'foo'`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`ALTER TABLE t DROP COLUMN c`); err != nil {
			t.Fatal(err)
		}

		row := db.QueryRow(`SELECT comment FROM system.comments LIMIT 1`)
		var comment string
		err := row.Scan(&comment)
		if !errors.Is(err, gosql.ErrNoRows) {
			if err != nil {
				t.Fatal(err)
			}

			t.Fatal("comment remaining in system.comments despite drop")
		}
	})
}

func TestCommentOnAlteredColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	runCommentOnTests(t, func(db *gosql.DB) {
		expectedComment := "expected comment"

		if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		SET enable_experimental_alter_column_type_general = true;
		CREATE TABLE t (c INT);
	`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`COMMENT ON COLUMN t.c IS 'first comment'`); err != nil {
			t.Fatal(err)
		}

		if _, err := db.Exec(`ALTER TABLE t ALTER COLUMN c TYPE character varying;`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			fmt.Sprintf(`COMMENT ON COLUMN t.c IS '%s'`, expectedComment)); err != nil {
			t.Fatal(err)
		}
		row := db.QueryRow(`SELECT comment FROM system.comments LIMIT 1`)
		var comment string
		if err := row.Scan(&comment); err != nil {
			t.Fatal(err)
		}

		if expectedComment != comment {
			t.Fatalf("expected comment %v, got %v", expectedComment, comment)
		}
	})
}

func runCommentOnTests(t *testing.T, testFunc func(db *gosql.DB)) {
	for _, setupQuery := range []string{
		`SET use_declarative_schema_changer = 'on'`,
		`SET use_declarative_schema_changer = 'off'`,
	} {
		func() {
			params, _ := tests.CreateTestServerParams()
			s, db, _ := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(context.Background())
			_, err := db.Exec(setupQuery)
			require.NoError(t, err)
			testFunc(db)
		}()
	}
}
