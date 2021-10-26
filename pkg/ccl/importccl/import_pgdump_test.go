// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestParseDDLStatementsFromDumpFile tests that DDL statements in a dump file
// are correctly parsed, and the database name for fully-qualified objects is
// replaced with the temporary database being imported into.
func TestParseDDLStatementsFromDumpFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)

	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	execCtx, cleanup := sql.MakeJobExecContext("TestParseDDLStatementsFromDumpFile", security.RootUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer cleanup()

	var data string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()

	tests := []struct {
		name          string
		data          string
		error         string
		expectedStmts []string
	}{
		{
			name: "multiple CREATE DATABASE",
			data: `
CREATE DATABASE db;
CREATE TABLE db.schema.table (id INT);
CREATE DATABASE db2;
`,
			error: "encountered more than one `CREATE DATABASE` statement when importing PGDUMP file",
		},
		{
			name: "unqualified create statements",
			data: `
		CREATE DATABASE db;
		CREATE SCHEMA s;
		CREATE TABLE t (id INT);
		CREATE SEQUENCE seq;
		CREATE INDEX idx ON t (id);
		`,
			expectedStmts: []string{"CREATE SCHEMA s",
				"CREATE TABLE t (id INT8)",
				"CREATE SEQUENCE seq",
				"CREATE INDEX idx ON t (id)"},
		},
		{
			name: "qualified create statement",
			data: `
		CREATE DATABASE db;
		CREATE SCHEMA db.s;
		CREATE TABLE db.s.t (id INT);
		CREATE TABLE s.t2 (id INT);
		CREATE SEQUENCE db.public.seq;
		CREATE INDEX idx ON db.s.t (id);
		`,
			expectedStmts: []string{"CREATE SCHEMA crdb_temp_pgdump_import.s",
				"CREATE TABLE crdb_temp_pgdump_import.s.t (id INT8)",
				"CREATE TABLE s.t2 (id INT8)",
				"CREATE SEQUENCE crdb_temp_pgdump_import.public.seq",
				"CREATE INDEX idx ON crdb_temp_pgdump_import.s.t (id)"},
		},
		{
			name: "mismatched catalog name",
			data: `
		CREATE DATABASE db;
		CREATE SCHEMA db2.s;
		`,
			error: "catalog name db2 does not match dump target database name db",
		},
		{
			name: "unqualified alter statements",
			data: `
CREATE DATABASE db;
ALTER TABLE t ADD COLUMN col STRING;
ALTER SCHEMA s RENAME TO s2;
		`,
			expectedStmts: []string{"ALTER TABLE t ADD COLUMN col STRING",
				"ALTER SCHEMA s RENAME TO s2"},
		},
		{
			name: "qualified alter statements",
			data: `
CREATE DATABASE db;
ALTER TABLE db.s.t ADD COLUMN col STRING;
ALTER SCHEMA db.s RENAME TO s2;
ALTER TABLE db.s.t ADD CONSTRAINT users_fk FOREIGN KEY (id) REFERENCES db.s.t2 (id) ON DELETE CASCADE;
		`,
			expectedStmts: []string{"ALTER TABLE crdb_temp_pgdump_import.s.t ADD COLUMN col STRING",
				"ALTER SCHEMA crdb_temp_pgdump_import.s RENAME TO s2",
				"ALTER TABLE crdb_temp_pgdump_import.s.t ADD CONSTRAINT users_fk FOREIGN KEY (id) REFERENCES crdb_temp_pgdump_import.s.t2 (id) ON DELETE CASCADE"},
		},
		{
			name: "shp2pg.sql select statement",
			data: `
CREATE DATABASE db;
CREATE TABLE "nyc_census_blocks" (gid serial,
"blkid" varchar(15),
"popn_total" float8,
"popn_white" float8,
"popn_black" float8,
"popn_nativ" float8,
"popn_asian" float8,
"popn_other" float8,
"boroname" varchar(32)) WITH (fillfactor = 2, autovacuum_enabled = false);
ALTER TABLE "nyc_census_blocks" ADD PRIMARY KEY (gid);
SELECT AddGeometryColumn('', 'nyc_census_blocks','geom','26918','MULTIPOLYGON',2);
`,
			expectedStmts: []string{"CREATE TABLE nyc_census_blocks (gid SERIAL8, blkid VARCHAR(15), popn_total FLOAT8, popn_white FLOAT8, popn_black FLOAT8, popn_nativ FLOAT8, popn_asian FLOAT8, popn_other FLOAT8, boroname VARCHAR(32))",
				"ALTER TABLE nyc_census_blocks ADD PRIMARY KEY (gid)",
				"ALTER TABLE nyc_census_blocks ADD COLUMN geom GEOMETRY(MULTIPOLYGON,26918)"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data = test.data
			h, err := parseDDLStatementsFromDumpFile(ctx, &evalCtx, execCtx, srv.URL,
				roachpb.IOFileFormat{PgDump: roachpb.PgDumpOptions{
					MaxRowSize: defaultScanBuffer,
				}}, defaultScanBuffer, 0)
			if test.error != "" {
				require.True(t, testutils.IsError(err, test.error))
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedStmts, h.bufferedDDLStmts)
			}
		})
	}
}

func TestProcessDDLStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)

	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	execCtx, cleanup := sql.MakeJobExecContext("TestParseDDLStatementsFromDumpFile", security.NodeUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer cleanup()

	conn := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(conn)

	var data string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()

	tests := []struct {
		name            string
		data            string
		error           string
		expectedTables  []string
		expectedSchemas []string
	}{
		{
			name: "simple object creation",
			data: `
CREATE DATABASE db;
CREATE SCHEMA s;
CREATE TABLE db.s.t (id INT);
CREATE SCHEMA s2;
CREATE SEQUENCE s2.seq;
CREATE INDEX foo ON db.s.t (id);
`,
			expectedTables:  []string{"t", "seq"},
			expectedSchemas: []string{"s", "s2"},
		},
		{
			name: "simple alter statements",
			data: `
CREATE DATABASE db;
CREATE SCHEMA s;
CREATE TABLE db.s.t (id INT, INDEX foo(id));
CREATE TABLE db.s.z (id INT PRIMARY KEY, id2 INT);

ALTER TABLE db.s.t ADD COLUMN id2 INT;
ALTER TABLE db.s.t ALTER COLUMN id2 SET NOT NULL;
ALTER TABLE db.s.z ALTER COLUMN id2 SET DEFAULT 0;
`,
			expectedTables:  []string{"t", "z"},
			expectedSchemas: []string{"s"},
		},
	}

	for _, test := range tests {
		data = test.data
		tables, schemas, err := processDDLStatements(ctx, &evalCtx, execCtx, srv.URL,
			roachpb.IOFileFormat{PgDump: roachpb.PgDumpOptions{
				MaxRowSize: defaultScanBuffer,
			}}, defaultScanBuffer, 0)
		require.NoError(t, err)

		require.Equal(t, len(test.expectedTables), len(tables))
		require.Equal(t, len(test.expectedSchemas), len(schemas))

		var tempImportDBID descpb.ID
		tdb.QueryRow(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name='%s'`, importTempPgdumpDB)).Scan(&tempImportDBID)

		for i, table := range tables {
			require.Equal(t, table.GetName(), test.expectedTables[i])
			require.True(t, table.Offline())
			require.Equal(t, tempImportDBID, table.GetParentID())
		}

		for i, schema := range schemas {
			require.Equal(t, schema.GetName(), test.expectedSchemas[i])
			require.True(t, schema.Offline())
			require.Equal(t, tempImportDBID, schema.GetParentID())
		}

		// For test purposes move all objects to PUBLIC so that we can drop the
		// database.
		_, _, err = moveObjectsInTempDatabaseToState(ctx, execCtx, tempImportDBID, descpb.DescriptorState_PUBLIC)
		require.NoError(t, err)
		tdb.Exec(t, fmt.Sprintf(`DROP DATABASE %s CASCADE`, importTempPgdumpDB))
	}
}
