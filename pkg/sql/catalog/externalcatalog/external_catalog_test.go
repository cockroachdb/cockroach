// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package externalcatalog

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/externalcatalog/externalpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// TestExtractIngestExternalCatalog extracts some tables from a cluster and
// ingests them into another cluster into a different database and schema.
func TestExtractIngestExternalCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	// Ensure gc job runs quickly drop DropExternalCatalog.
	sysDB := sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t))
	sysDB.Exec(t, "SET CLUSTER SETTING sql.virtual_cluster.feature_access.zone_configs.enabled = true;")
	sysDB.Exec(t, "SET CLUSTER SETTING sql.virtual_cluster.feature_access.zone_configs_unrestricted.enabled = true;")

	rng, _ := randutil.NewTestRand()
	fastGC := rng.Float64() < 0.5
	if fastGC {
		sysDB.Exec(t, "SET CLUSTER SETTING sql.gc_job.wait_for_gc.interval = '250ms'")
	}

	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "CREATE DATABASE db1")
	sqlDB.Exec(t, "CREATE SCHEMA db1.sc1")
	sqlDB.Exec(t, "CREATE TABLE db1.sc1.tab1 (a INT PRIMARY KEY)")
	sqlDB.Exec(t, "CREATE INDEX idx ON db1.sc1.tab1 (a)")
	sqlDB.Exec(t, "CREATE TABLE db1.sc1.tab2 (a INT PRIMARY KEY)")

	sqlUser, err := username.MakeSQLUsernameFromUserInput("root", username.PurposeValidation)
	require.NoError(t, err)

	extractCatalog := func(tableNames ...string) (catalog externalpb.ExternalCatalog, err error) {
		err = sqltestutils.TestingDescsTxn(ctx, srv, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			opName := redact.SafeString("extractCatalog")
			planner, close := sql.NewInternalPlanner(
				opName,
				txn.KV(),
				sqlUser,
				&sql.MemoryMetrics{},
				&execCfg,
				sql.NewInternalSessionData(ctx, execCfg.Settings, opName),
			)
			defer close()

			catalog, err = ExtractExternalCatalog(ctx, planner.(resolver.SchemaResolver), txn, col, false, tableNames...)
			return err
		})
		return catalog, err
	}

	getDatabaseSchemaIDs := func(database string) (descpb.ID, descpb.ID) {
		var parentID descpb.ID
		var schemaID descpb.ID
		require.NoError(t, sqltestutils.TestingDescsTxn(ctx, srv, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			dbDesc, err := col.ByNameWithLeased(txn.KV()).Get().Database(ctx, database)
			require.NoError(t, err)
			parentID = dbDesc.GetID()
			schemaID, err = col.LookupSchemaID(ctx, txn.KV(), parentID, catconstants.PublicSchemaName)
			return err
		}))
		return parentID, schemaID
	}

	defaultdbID, defaultdbpublicID := getDatabaseSchemaIDs("defaultdb")

	t.Run("basic", func(t *testing.T) {
		ingestableCatalog, err := extractCatalog("db1.sc1.tab1", "db1.sc1.tab2")
		require.NoError(t, err)
		require.Equal(t, 2, len(ingestableCatalog.Tables))

		var written externalpb.ExternalCatalog
		require.NoError(t, sqltestutils.TestingDescsTxn(ctx, srv, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			written, err = IngestExternalCatalog(ctx, &execCfg, sqlUser, ingestableCatalog, txn, col, defaultdbID, defaultdbpublicID, false)
			return err
		}))
		require.Equal(t, 2, len(written.Tables))
		sqlDB.CheckQueryResults(t, "SELECT schema_name,table_name FROM [SHOW TABLES]", [][]string{{"public", "tab1"}, {"public", "tab2"}})

		require.NoError(t, sqltestutils.TestingDescsTxn(ctx, srv, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			return DropIngestedExternalCatalog(ctx, &execCfg, sqlUser, written, txn, srv.JobRegistry().(*jobs.Registry), col, "test gc")
		}))
		var res int
		sqlDB.QueryRow(t, "SELECT count(*) FROM [SHOW TABLES]").Scan(&res)
		require.Zero(t, res)
		if fastGC {
			// With fast gc, the dropped tables may disappear before
			// crdb_internal.tables is read.
			sqlDB.CheckQueryResultsRetry(
				t,
				"SELECT status FROM [SHOW JOBS] WHERE description = 'test gc'",
				[][]string{{"succeeded"}},
			)
		} else {
			sqlDB.CheckQueryResults(t, "SELECT name FROM crdb_internal.tables WHERE state = 'DROP'", [][]string{{"tab1"}, {"tab2"}})
		}
	})

	t.Run("udt", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE TYPE db1.udt AS ENUM ('a', 'b', 'c')")
		sqlDB.Exec(t, "CREATE TABLE db1.data (pk INT PRIMARY KEY, val1 db1.udt)")
		udtCatalog, err := extractCatalog("db1.data")
		require.NoError(t, err)
		require.Equal(t, "udt", udtCatalog.Types[0].Name)
		require.Equal(t, "_udt", udtCatalog.Types[1].Name)

		// The ingest API will not allow cross database references to be written,
		// since these are deprecated. So, any attempt to write these should hit
		// an error.
		require.ErrorContains(t,
			sqltestutils.TestingDescsTxn(ctx, srv, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
				_, err = IngestExternalCatalog(ctx, &execCfg, sqlUser, udtCatalog, txn, col, defaultdbID, defaultdbpublicID, false)
				return err
			}),
			"cross database type references are not supported",
		)
		var res int
		sqlDB.QueryRow(t, "SELECT count(*) FROM [SHOW TABLES]").Scan(&res)
		require.Zero(t, res)
	})

	t.Run("fk", func(t *testing.T) {
		sqlDB.Exec(t, "CREATE TABLE db1.sc1.tab3 (a INT PRIMARY KEY, b INT REFERENCES db1.sc1.tab2(a))")
		sadCatalog, err := extractCatalog("db1.sc1.tab3")
		require.NoError(t, err)
		require.ErrorContains(t, sqltestutils.TestingDescsTxn(ctx, srv, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			_, err := IngestExternalCatalog(ctx, &execCfg, sqlUser, sadCatalog, txn, col, defaultdbID, defaultdbpublicID, false)
			return err
		}), "invalid outbound foreign key")

		anotherSadCatalog, err := extractCatalog("db1.sc1.tab2")
		require.NoError(t, err)
		require.ErrorContains(t, sqltestutils.TestingDescsTxn(ctx, srv, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			_, err := IngestExternalCatalog(ctx, &execCfg, sqlUser, anotherSadCatalog, txn, col, defaultdbID, defaultdbpublicID, false)
			return err
		}), "invalid inbound foreign key")
	})
}
