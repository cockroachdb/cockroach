// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package externalcatalog

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/externalcatalog/externalpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// TestExtractIngestExternalCatalog extracts some tables from a cluster and
// ingests them into another cluster into a different database and schema.
func TestExtractIngestExternalCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	sqlDB := sqlutils.MakeSQLRunner(conn)
	sqlDB.Exec(t, "CREATE DATABASE db1")
	sqlDB.Exec(t, "CREATE SCHEMA db1.sc1")
	sqlDB.Exec(t, "CREATE TABLE db1.sc1.tab1 (a INT PRIMARY KEY)")
	sqlDB.Exec(t, "CREATE INDEX idx ON db1.sc1.tab1 (a)")
	sqlDB.Exec(t, "CREATE TABLE db1.sc1.tab2 (a INT PRIMARY KEY)")

	sqlUser, err := username.MakeSQLUsernameFromUserInput("root", username.PurposeValidation)
	require.NoError(t, err)

	extractCatalog := func() externalpb.ExternalCatalog {
		opName := redact.SafeString("extractCatalog")
		planner, close := sql.NewInternalPlanner(
			opName,
			kv.NewTxn(ctx, kvDB, 0),
			sqlUser,
			&sql.MemoryMetrics{},
			&execCfg,
			sql.NewInternalSessionData(ctx, execCfg.Settings, opName),
		)
		defer close()

		catalog, err := ExtractExternalCatalog(ctx, planner.(resolver.SchemaResolver), "db1.sc1.tab1", "db1.sc1.tab2")
		require.NoError(t, err)
		return catalog
	}

	ingestableCatalog := extractCatalog()
	require.NoError(t, err)
	require.Equal(t, 2, len(ingestableCatalog.Tables))

	// Extract the same tables but note that tab1 now has a back reference.
	sqlDB.Exec(t, "CREATE TABLE db1.sc1.tab3 (a INT PRIMARY KEY, b INT REFERENCES db1.sc1.tab2(a))")
	sadCatalog := extractCatalog()

	srv2, conn2, _ := serverutils.StartServer(t, base.TestServerArgs{})
	execCfg2 := srv2.ExecutorConfig().(sql.ExecutorConfig)
	defer srv2.Stopper().Stop(ctx)

	var parentID descpb.ID
	var schemaID descpb.ID

	require.NoError(t, sql.TestingDescsTxn(ctx, srv2, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		dbDesc, err := col.ByNameWithLeased(txn.KV()).Get().Database(ctx, "defaultdb")
		require.NoError(t, err)
		parentID = dbDesc.GetID()
		schemaID, err = col.LookupSchemaID(ctx, txn.KV(), parentID, catconstants.PublicSchemaName)
		return err
	}))

	require.ErrorContains(t, sql.TestingDescsTxn(ctx, srv2, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		return IngestExternalCatalog(ctx, &execCfg2, sqlUser, sadCatalog, txn, col, parentID, schemaID, false)
	}), "invalid foreign key backreference")

	require.NoError(t, sql.TestingDescsTxn(ctx, srv2, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		return IngestExternalCatalog(ctx, &execCfg2, sqlUser, ingestableCatalog, txn, col, parentID, schemaID, false)
	}))

	sqlDB2 := sqlutils.MakeSQLRunner(conn2)
	sqlDB2.CheckQueryResults(t, "SELECT schema_name,table_name FROM [SHOW TABLES]", [][]string{{"public", "tab1"}, {"public", "tab2"}})
}
