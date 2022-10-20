// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package importer_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type userWithTablePrivilege struct {
	grantee     string
	catalogName string
	schemaName  string
	tableName   string
	privilege   string
}

// TestParseDDLStatementsFromDumpFile tests that DDL statements in a dump file
// are correctly parsed, and the database name for fully-qualified objects is
// replaced with the temporary database being imported into.
func TestParseDDLStatementsFromDumpFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)

	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	kvDB := execCfg.DB
	p, cleanup := sql.NewInternalPlanner("importPgdump",
		kvDB.NewTxn(ctx, "TestParseDDLStatementsFromDumpFile"),
		username.RootUserName(), &sql.MemoryMetrics{}, &execCfg,
		sessiondatapb.SessionData{
			Database:   importer.ImportTempPgdumpDB,
			SearchPath: sessiondata.DefaultSearchPath.GetPathArray(),
		})
	defer cleanup()
	execCtx := p.(sql.JobExecContext)

	var queryBundle string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(queryBundle))
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
		// SELECT stmt that triggers schema change.
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
			expectedStmts: []string{"CREATE TABLE nyc_census_blocks (gid SERIAL8, blkid VARCHAR(15), popn_total FLOAT8, popn_white FLOAT8, popn_black FLOAT8, popn_nativ FLOAT8, popn_asian FLOAT8, popn_other FLOAT8, boroname VARCHAR(32)) WITH (fillfactor = 2, autovacuum_enabled = false)",
				"ALTER TABLE nyc_census_blocks ADD PRIMARY KEY (gid)",
				"ALTER TABLE nyc_census_blocks ADD COLUMN geom GEOMETRY(MULTIPOLYGON,26918)"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// The flow of the test data: test.data -> queryBundle -> srv -> ParseDDLStatementsFromDumpFile.
			queryBundle = test.data
			h, err := importer.ParseDDLStatementsFromDumpFile(ctx, &evalCtx, execCtx, srv.URL,
				roachpb.IOFileFormat{PgDump: roachpb.PgDumpOptions{
					MaxRowSize: importer.DefaultScanBuffer,
				}}, importer.DefaultScanBuffer, 0)
			if test.error != "" {
				require.True(t, testutils.IsError(err, test.error))
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedStmts, h.BufferedDDLStmts)
			}
		})
	}
}

// TestPrivilegeOnTempDB tests that the temporary database created to store the
// newly created schemas is only accessible to the node user, and not visible
// to any superusers.
func TestPrivilegeOnTempDB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)

	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	kvDB := execCfg.DB
	p, cleanup := sql.NewInternalPlanner("importPgdump",
		kvDB.NewTxn(ctx, "TestParseDDLStatementsFromDumpFile"),
		username.RootUserName(), &sql.MemoryMetrics{}, &execCfg,
		sessiondatapb.SessionData{
			Database:   importer.ImportTempPgdumpDB,
			SearchPath: sessiondata.DefaultSearchPath.GetPathArray(),
		})
	defer cleanup()
	execCtx := p.(sql.JobExecContext)

	data := `
		CREATE DATABASE jojo;
    CREATE TABLE yaroyaro (x int);
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()

	_, _, err := importer.ProcessDDLStatements(ctx, &evalCtx, execCtx, srv.URL,
		roachpb.IOFileFormat{PgDump: roachpb.PgDumpOptions{
			MaxRowSize: importer.DefaultScanBuffer,
		}}, importer.DefaultScanBuffer,
		0, /* parentID*/
	)

	require.NoError(t, err)

	ief := execCfg.InternalExecutorFactory
	err = ief.DescsTxnWithExecutor(
		ctx,
		execCfg.DB,
		nil, /* sessionData */
		func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, ie sqlutil.InternalExecutor) error {
			previlegesOnTempDBFromNode, err := ie.QueryBufferedEx(
				ctx,
				"check privilege on temp db",
				txn,
				sessiondata.InternalExecutorOverride{
					User:     username.NodeUserName(),
					Database: importer.ImportTempPgdumpDB,
				},
				fmt.Sprintf(`SHOW GRANTS ON DATABASE %s`, importer.ImportTempPgdumpDB),
			)
			if err != nil {
				return err
			}

			// Only public has the CONNECT privilege.
			require.Equal(t, 1, len(previlegesOnTempDBFromNode))
			require.Equal(t, `('crdb_temp_pgdump_import', 'public', 'CONNECT', false)`, previlegesOnTempDBFromNode[0].String())

			// Remove the database at the end of the test.
			_, err = ie.ExecEx(
				ctx,
				"remove temp db from import pgdump",
				txn,
				sessiondata.InternalExecutorOverride{User: username.NodeUserName()},
				fmt.Sprintf(`DROP DATABASE %s CASCADE`, importer.ImportTempPgdumpDB),
			)
			return err
		})
	require.NoError(t, err)

}

// TestIngestionIntoTable tests that the newly created tables can be inserted.
func TestIngestionIntoTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)

	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	kvDB := execCfg.DB
	p, cleanup := sql.NewInternalPlanner("importPgdump",
		kvDB.NewTxn(ctx, "TestParseDDLStatementsFromDumpFile"),
		username.RootUserName(), &sql.MemoryMetrics{}, &execCfg,
		sessiondatapb.SessionData{
			Database:   importer.ImportTempPgdumpDB,
			SearchPath: sessiondata.DefaultSearchPath.GetPathArray(),
		})
	defer cleanup()
	execCtx := p.(sql.JobExecContext)

	var ddlStmts string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(ddlStmts))
		}
	}))
	defer srv.Close()

	tests := []struct {
		name        string
		ddlStmts    string
		insertStmts []string
		queryStmt   string
		queryResult string
	}{
		{
			name: "simple insert",
			ddlStmts: `
		CREATE DATABASE jojo;
    CREATE TABLE yaroyaro (x int);
`,
			insertStmts: []string{`INSERT INTO yaroyaro VALUES (1), (2), (3)`},
			queryStmt:   `SELECT * FROM yaroyaro`,
			queryResult: `[[1] [2] [3]]`,
		},
	}

	for _, test := range tests {
		ddlStmts = test.ddlStmts
		_, _, err := importer.ProcessDDLStatements(ctx, &evalCtx, execCtx, srv.URL,
			roachpb.IOFileFormat{PgDump: roachpb.PgDumpOptions{
				MaxRowSize: importer.DefaultScanBuffer,
			}}, importer.DefaultScanBuffer,
			0, /* parentID*/
		)

		require.NoError(t, err)

		ief := execCfg.InternalExecutorFactory
		err = ief.DescsTxnWithExecutor(
			ctx,
			execCfg.DB,
			nil, /* sessionData */
			func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, ie sqlutil.InternalExecutor) error {
				var res []tree.Datums
				var err error

				for _, insertStmt := range test.insertStmts {
					_, err = ie.ExecEx(
						ctx,
						"check privilege on temp db",
						txn,
						sessiondata.InternalExecutorOverride{
							User:     username.NodeUserName(),
							Database: importer.ImportTempPgdumpDB,
						},
						insertStmt,
					)
					if err != nil {
						return errors.Wrapf(err, "running %q", insertStmt)
					}
				}

				res, err = ie.QueryBufferedEx(
					ctx,
					"check privilege on temp db",
					txn,
					sessiondata.InternalExecutorOverride{
						User:     username.NodeUserName(),
						Database: importer.ImportTempPgdumpDB,
					},
					test.queryStmt,
				)
				if err != nil {
					return err
				}

				require.Less(t, 0, len(res))
				require.Equal(t, test.queryResult, fmt.Sprintf("%s", res))

				// Remove the database at the end of the test.
				_, err = ie.ExecEx(
					ctx,
					"remove temp db from import pgdump",
					txn,
					sessiondata.InternalExecutorOverride{User: username.NodeUserName()},
					fmt.Sprintf(`DROP DATABASE %s CASCADE`, importer.ImportTempPgdumpDB),
				)
				return err
			})
		require.NoError(t, err)

	}

}

func TestProcessDDLStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)

	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	kvDB := execCfg.DB
	p, cleanup := sql.NewInternalPlanner("importPgdump",
		kvDB.NewTxn(ctx, "TestParseDDLStatementsFromDumpFile"),
		username.RootUserName(), &sql.MemoryMetrics{}, &execCfg,
		sessiondatapb.SessionData{
			Database:   importer.ImportTempPgdumpDB,
			SearchPath: sessiondata.DefaultSearchPath.GetPathArray(),
		})
	defer cleanup()
	execCtx := p.(sql.JobExecContext)

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
		name               string
		data               string
		error              string
		expectedTables     []string
		expectedSchemas    []string
		expectedPrivileges []userWithTablePrivilege
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
		{
			name: "disallow db.table form",
			data: `
				CREATE DATABASE jojo;
				CREATE SCHEMA jojo.jojo7;
				CREATE TABLE jojo.sbr (id INT, horse STRING);
		`,
			error: "running DDL statements from dump file: executing CREATE TABLE jojo.sbr (id INT8, horse STRING): " +
				"import-pgdump-ddl: cannot create \"jojo.sbr\" " +
				"because the target database or schema does not exist",
		},
		{
			name: "database and schema with the same name",
			data: `
				CREATE DATABASE jojo;
				CREATE SCHEMA jojo.jojo;
				CREATE TABLE jojo.sbr (id INT, horse STRING);
		`,
			expectedTables:  []string{"sbr"},
			expectedSchemas: []string{"jojo"},
		},
		{
			name: "create role and grant",
			data: `
		CREATE DATABASE jojo;
		CREATE ROLE johnny;
		CREATE TABLE sbr (id INT, horse STRING);
    GRANT ALL ON TABLE sbr TO johnny;
`,
			expectedTables: []string{"sbr"},
			expectedPrivileges: []userWithTablePrivilege{
				{
					grantee:     `'johnny'`,
					catalogName: importer.ImportTempPgdumpDB,
					schemaName:  "public",
					tableName:   "sbr",
					privilege:   "ALL",
				},
			},
		},
	}

	for _, test := range tests {
		data = test.data

		var testPrivilegeKnobs []func(ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn)
		for _, ep := range test.expectedPrivileges {
			testPrivilegeKnobs = append(testPrivilegeKnobs, func(ctx context.Context, ie sqlutil.InternalExecutor, txn *kv.Txn) {
				query := fmt.Sprintf(
					`SELECT grantee FROM information_schema.table_privileges where 
                table_catalog='%s' AND
                table_schema='%s' AND
                table_name='%s' AND 
                privilege_type='%s'
                `,
					ep.catalogName,
					ep.schemaName,
					ep.tableName,
					ep.privilege,
				)
				res, err := ie.QueryBufferedEx(
					ctx,
					fmt.Sprintf("%s, check table privilege", test.name),
					txn,
					sessiondata.InternalExecutorOverride{
						User:     username.NodeUserName(),
						Database: importer.ImportTempPgdumpDB,
					},
					query,
				)
				require.NoError(t, err)
				require.Equal(t, 1, len(res))
				require.Equal(t, ep.grantee, res[0][0].String())
			})
		}

		tables, schemas, err := importer.ProcessDDLStatements(ctx, &evalCtx, execCtx, srv.URL,
			roachpb.IOFileFormat{PgDump: roachpb.PgDumpOptions{
				MaxRowSize: importer.DefaultScanBuffer,
			}}, importer.DefaultScanBuffer,
			0, /* parentID*/
		)

		if test.error != "" {
			require.Equal(t, test.error, err.Error())
		} else {
			require.NoError(t, err)
		}

		require.Equal(t, len(test.expectedTables), len(tables))
		require.Equal(t, len(test.expectedSchemas), len(schemas))

		var tempImportDBID descpb.ID
		tdb.QueryRow(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name='%s'`, importer.ImportTempPgdumpDB)).Scan(&tempImportDBID)

		for i, table := range tables {
			require.Equal(t, table.GetName(), test.expectedTables[i])
			require.Equal(t, tempImportDBID, table.GetParentID())
		}

		for i, schema := range schemas {
			require.Equal(t, schema.GetName(), test.expectedSchemas[i])
			require.Equal(t, tempImportDBID, schema.GetParentID())
		}

		ief := execCfg.InternalExecutorFactory
		err = ief.DescsTxnWithExecutor(
			ctx,
			execCfg.DB,
			nil, /* sessionData */
			func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection, ie sqlutil.InternalExecutor) error {
				for _, testPrivilegeKnob := range testPrivilegeKnobs {
					testPrivilegeKnob(ctx, ie, txn)
				}
				// Remove the database at the end of the test.
				_, err := ie.ExecEx(
					ctx,
					"remove temp db from import pgdump",
					txn,
					sessiondata.InternalExecutorOverride{User: username.NodeUserName()},
					fmt.Sprintf(`DROP DATABASE %s CASCADE`, importer.ImportTempPgdumpDB),
				)
				return err
			})
		require.NoError(t, err)
	}
}
