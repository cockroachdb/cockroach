// Copyright 2015 The Cockroach Authors.
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
	"math/rand"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
)

// Returns an error if a zone config for the specified table or
// database ID doesn't match the expected parameter. If expected
// is nil, then we verify no zone config exists.
func zoneExists(sqlDB *gosql.DB, expected *zonepb.ZoneConfig, id sqlbase.ID) error {
	rows, err := sqlDB.Query(`SELECT * FROM system.zones WHERE id = $1`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	if exists := (expected != nil); exists != rows.Next() {
		return errors.Errorf("zone config exists = %v", exists)
	}
	if expected != nil {
		// Ensure that the zone config matches.
		var storedID sqlbase.ID
		var val []byte
		if err := rows.Scan(&storedID, &val); err != nil {
			return errors.Errorf("row scan failed: %s", err)
		}
		if storedID != id {
			return errors.Errorf("e = %d, v = %d", id, storedID)
		}
		var cfg zonepb.ZoneConfig
		if err := protoutil.Unmarshal(val, &cfg); err != nil {
			return err
		}
		if !expected.Equal(cfg) {
			return errors.Errorf("e = %v, v = %v", expected, cfg)
		}
	}
	return nil
}

// Returns an error if a descriptor "exists" for the table id.
func descExists(sqlDB *gosql.DB, exists bool, id sqlbase.ID) error {
	rows, err := sqlDB.Query(`SELECT * FROM system.descriptor WHERE id = $1`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	if exists != rows.Next() {
		return errors.Errorf("descriptor exists = %v", exists)
	}
	return nil
}

func TestDropDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	dbNameKey := sqlbase.NewDatabaseKey("t").Key(keys.SystemSQLCodec)
	r, err := kvDB.Get(ctx, dbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Exists() {
		t.Fatalf(`database "t" does not exist`)
	}
	dbDescKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, sqlbase.ID(r.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(ctx, dbDescKey, desc); err != nil {
		t.Fatal(err)
	}
	dbDesc := sqlbase.NewImmutableDatabaseDescriptor(*desc.GetDatabase())
	tbNameKey := sqlbase.NewPublicTableKey(dbDesc.GetID(), "kv").Key(keys.SystemSQLCodec)
	gr, err := kvDB.Get(ctx, tbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf(`table "kv" does not exist`)
	}
	tbDescKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, sqlbase.ID(gr.ValueInt()))
	ts, err := kvDB.GetProtoTs(ctx, tbDescKey, desc)
	if err != nil {
		t.Fatal(err)
	}
	tbDesc := desc.Table(ts)

	// Add a zone config for both the table and database.
	cfg := zonepb.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, dbDesc.GetID(), buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tbDesc.ID); err != nil {
		t.Fatal(err)
	}
	if err := zoneExists(sqlDB, &cfg, dbDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	tableSpan := tbDesc.TableSpan(keys.SystemSQLCodec)
	tests.CheckKeyCount(t, kvDB, tableSpan, 6)

	if _, err := sqlDB.Exec(`DROP DATABASE t RESTRICT`); !testutils.IsError(err,
		`database "t" is not empty`) {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`DROP DATABASE t CASCADE`); err != nil {
		t.Fatal(err)
	}

	// Data is not deleted.
	tests.CheckKeyCount(t, kvDB, tableSpan, 6)

	if err := descExists(sqlDB, true, tbDesc.ID); err != nil {
		t.Fatal(err)
	}

	if gr, err := kvDB.Get(ctx, tbNameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table descriptor key still exists after database is dropped")
	}

	if err := descExists(sqlDB, false, dbDesc.GetID()); err != nil {
		t.Fatal(err)
	}
	// Database zone config is removed once all table data and zone configs are removed.
	if err := zoneExists(sqlDB, &cfg, dbDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	if gr, err := kvDB.Get(ctx, dbNameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("database descriptor key still exists after database is dropped")
	}

	if err := zoneExists(sqlDB, &cfg, tbDesc.ID); err != nil {
		t.Fatal(err)
	}

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	// There are no more namespace entries referencing this database as its
	// parent.
	namespaceQuery := fmt.Sprintf(`SELECT * FROM system.namespace WHERE "parentID"  = %d`, dbDesc.GetID())
	sqlRun.CheckQueryResults(t, namespaceQuery, [][]string{})

	// Job still running, waiting for GC.
	// TODO (lucy): Maybe this test API should use an offset starting
	// from the most recent job instead.
	if err := jobutils.VerifySystemJob(t, sqlRun, 0, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUser,
		Description: "DROP DATABASE t CASCADE",
		DescriptorIDs: sqlbase.IDs{
			tbDesc.ID,
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// Test that an empty, dropped database's zone config gets deleted immediately.
func TestDropDatabaseEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
`); err != nil {
		t.Fatal(err)
	}

	dKey := sqlbase.NewDatabaseKey("t")
	r, err := kvDB.Get(ctx, dKey.Key(keys.SystemSQLCodec))
	if err != nil {
		t.Fatal(err)
	}
	if !r.Exists() {
		t.Fatalf(`database "t" does not exist`)
	}
	dbID := sqlbase.ID(r.ValueInt())

	if cfg, err := sqltestutils.AddDefaultZoneConfig(sqlDB, dbID); err != nil {
		t.Fatal(err)
	} else if err := zoneExists(sqlDB, &cfg, dbID); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`DROP DATABASE t`); err != nil {
		t.Fatal(err)
	}

	if err := descExists(sqlDB, false, dbID); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, nil, dbID); err != nil {
		t.Fatal(err)
	}
}

// Test that a dropped database's data gets deleted properly.
func TestDropDatabaseDeleteData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer gcjob.SetSmallMaxGCIntervalForTest()()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
CREATE TABLE t.kv2 (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.kv2 VALUES ('c', 'd'), ('a', 'b'), ('e', 'a');
`); err != nil {
		t.Fatal(err)
	}

	dKey := sqlbase.NewDatabaseKey("t")
	r, err := kvDB.Get(ctx, dKey.Key(keys.SystemSQLCodec))
	if err != nil {
		t.Fatal(err)
	}
	if !r.Exists() {
		t.Fatalf(`database "t" does not exist`)
	}
	dbDescKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, sqlbase.ID(r.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(ctx, dbDescKey, desc); err != nil {
		t.Fatal(err)
	}
	dbDesc := sqlbase.NewImmutableDatabaseDescriptor(*desc.GetDatabase())

	tKey := sqlbase.NewPublicTableKey(dbDesc.GetID(), "kv")
	gr, err := kvDB.Get(ctx, tKey.Key(keys.SystemSQLCodec))
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf(`table "kv" does not exist`)
	}
	tbDescKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, sqlbase.ID(gr.ValueInt()))
	ts, err := kvDB.GetProtoTs(ctx, tbDescKey, desc)
	if err != nil {
		t.Fatal(err)
	}
	tbDesc := desc.Table(ts)

	t2Key := sqlbase.NewPublicTableKey(dbDesc.GetID(), "kv2")
	gr2, err := kvDB.Get(ctx, t2Key.Key(keys.SystemSQLCodec))
	if err != nil {
		t.Fatal(err)
	}
	if !gr2.Exists() {
		t.Fatalf(`table "kv2" does not exist`)
	}
	tb2DescKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, sqlbase.ID(gr2.ValueInt()))
	ts, err = kvDB.GetProtoTs(ctx, tb2DescKey, desc)
	if err != nil {
		t.Fatal(err)
	}
	tb2Desc := desc.Table(ts)

	tableSpan := tbDesc.TableSpan(keys.SystemSQLCodec)
	table2Span := tb2Desc.TableSpan(keys.SystemSQLCodec)
	tests.CheckKeyCount(t, kvDB, tableSpan, 6)
	tests.CheckKeyCount(t, kvDB, table2Span, 6)

	if _, err := sqltestutils.AddDefaultZoneConfig(sqlDB, dbDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`DROP DATABASE t RESTRICT`); !testutils.IsError(err,
		`database "t" is not empty`) {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`DROP DATABASE t CASCADE`); err != nil {
		t.Fatal(err)
	}

	tests.CheckKeyCount(t, kvDB, tableSpan, 6)
	tests.CheckKeyCount(t, kvDB, table2Span, 6)

	// TODO (lucy): Maybe this test API should use an offset starting
	// from the most recent job instead.
	const migrationJobOffset = 0
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	if err := jobutils.VerifySystemJob(t, sqlRun, migrationJobOffset, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUser,
		Description: "DROP DATABASE t CASCADE",
		DescriptorIDs: sqlbase.IDs{
			tbDesc.ID, tb2Desc.ID,
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Push a new zone config for the table with TTL=0 so the data is
	// deleted immediately.
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tbDesc.ID); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		if err := descExists(sqlDB, false, tbDesc.ID); err != nil {
			return err
		}

		return zoneExists(sqlDB, nil, tbDesc.ID)
	})

	// Table 1 data is deleted.
	tests.CheckKeyCount(t, kvDB, tableSpan, 0)
	tests.CheckKeyCount(t, kvDB, table2Span, 6)

	def := zonepb.DefaultZoneConfig()
	if err := zoneExists(sqlDB, &def, dbDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		return jobutils.VerifySystemJob(t, sqlRun, 0, jobspb.TypeSchemaChangeGC, jobs.StatusRunning, jobs.Record{
			Username:    security.RootUser,
			Description: "GC for DROP DATABASE t CASCADE",
			DescriptorIDs: sqlbase.IDs{
				tbDesc.ID, tb2Desc.ID,
			},
		})
	})

	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tb2Desc.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, dbDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		if err := descExists(sqlDB, false, tb2Desc.ID); err != nil {
			return err
		}

		return zoneExists(sqlDB, nil, tb2Desc.ID)
	})

	// Table 2 data is deleted.
	tests.CheckKeyCount(t, kvDB, table2Span, 0)

	if err := jobutils.VerifySystemJob(t, sqlRun, migrationJobOffset, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUser,
		Description: "DROP DATABASE t CASCADE",
		DescriptorIDs: sqlbase.IDs{
			tbDesc.ID, tb2Desc.ID,
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Database zone config is removed once all table data and zone configs are removed.
	if err := zoneExists(sqlDB, nil, dbDesc.GetID()); err != nil {
		t.Fatal(err)
	}
}

// Tests that SHOW TABLES works correctly when a database is recreated
// during the time the underlying tables are still being deleted.
func TestShowTablesAfterRecreateDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	// Turn off the application of schema changes so that tables do not
	// get completely dropped.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SchemaChangeJobNoOp: func() bool {
				return true
			},
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
DROP DATABASE t CASCADE;
CREATE DATABASE t;
`); err != nil {
		t.Fatal(err)
	}

	rows, err := sqlDB.Query(`
SET DATABASE=t;
SHOW TABLES;
`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("table should be invisible through SHOW TABLES")
	}
}

func TestDropIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const chunkSize = 200
	params, _ := tests.CreateTestServerParams()
	emptySpan := true
	clearIndexAttempt := false
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if clearIndexAttempt && (sp.Key != nil || sp.EndKey != nil) {
					emptySpan = false
				}
				return nil
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	numRows := 2*chunkSize + 1
	if err := tests.CreateKVTable(sqlDB, "kv", numRows); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	tests.CheckKeyCount(t, kvDB, tableDesc.TableSpan(keys.SystemSQLCodec), 3*numRows)
	idx, _, err := tableDesc.FindIndexByName("foo")
	if err != nil {
		t.Fatal(err)
	}
	indexSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, idx.ID)
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)
	if _, err := sqlDB.Exec(`DROP INDEX t.kv@foo`); err != nil {
		t.Fatal(err)
	}

	tableDesc = sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if _, _, err := tableDesc.FindIndexByName("foo"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
	// Index data hasn't been deleted.
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)
	tests.CheckKeyCount(t, kvDB, tableDesc.TableSpan(keys.SystemSQLCodec), 3*numRows)

	// TODO (lucy): Maybe this test API should use an offset starting
	// from the most recent job instead.
	const migrationJobOffset = 0
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	if err := jobutils.VerifySystemJob(t, sqlRun, migrationJobOffset+1, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUser,
		Description: `DROP INDEX t.public.kv@foo`,
		DescriptorIDs: sqlbase.IDs{
			tableDesc.ID,
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`CREATE INDEX foo on t.kv (v);`); err != nil {
		t.Fatal(err)
	}

	tableDesc = sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	newIdx, _, err := tableDesc.FindIndexByName("foo")
	if err != nil {
		t.Fatal(err)
	}
	newIdxSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, newIdx.ID)
	tests.CheckKeyCount(t, kvDB, newIdxSpan, numRows)
	tests.CheckKeyCount(t, kvDB, tableDesc.TableSpan(keys.SystemSQLCodec), 4*numRows)

	clearIndexAttempt = true
	// Add a zone config for the table.
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		return jobutils.VerifySystemJob(t, sqlRun, migrationJobOffset+1, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
			Username:    security.RootUser,
			Description: `DROP INDEX t.public.kv@foo`,
			DescriptorIDs: sqlbase.IDs{
				tableDesc.ID,
			},
		})
	})

	testutils.SucceedsSoon(t, func() error {
		return jobutils.VerifySystemJob(t, sqlRun, 0, jobspb.TypeSchemaChangeGC, jobs.StatusSucceeded, jobs.Record{
			Username:    security.RootUser,
			Description: `GC for DROP INDEX t.public.kv@foo`,
			DescriptorIDs: sqlbase.IDs{
				tableDesc.ID,
			},
		})
	})

	if !emptySpan {
		t.Fatalf("tried to clear index with non-empty resume span")
	}

	tests.CheckKeyCount(t, kvDB, newIdxSpan, numRows)
	tests.CheckKeyCount(t, kvDB, indexSpan, 0)
	tests.CheckKeyCount(t, kvDB, tableDesc.TableSpan(keys.SystemSQLCodec), 3*numRows)
}

func TestDropIndexWithZoneConfigOSS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const chunkSize = 200
	const numRows = 2*chunkSize + 1

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{BackfillChunkSize: chunkSize},
	}
	s, sqlDBRaw, kvDB := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	defer s.Stopper().Stop(context.Background())

	// Create a test table with a secondary index.
	if err := tests.CreateKVTable(sqlDBRaw, "kv", numRows); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	indexDesc, _, err := tableDesc.FindIndexByName("foo")
	if err != nil {
		t.Fatal(err)
	}
	indexSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, indexDesc.ID)
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)

	// Hack in zone configs for the primary and secondary indexes. (You need a CCL
	// binary to do this properly.) Dropping the index will thus require
	// regenerating the zone config's SubzoneSpans, which will fail with a "CCL
	// required" error.
	zoneConfig := zonepb.ZoneConfig{
		Subzones: []zonepb.Subzone{
			{IndexID: uint32(tableDesc.PrimaryIndex.ID), Config: s.(*server.TestServer).Cfg.DefaultZoneConfig},
			{IndexID: uint32(indexDesc.ID), Config: s.(*server.TestServer).Cfg.DefaultZoneConfig},
		},
	}
	zoneConfigBytes, err := protoutil.Marshal(&zoneConfig)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, `INSERT INTO system.zones VALUES ($1, $2)`, tableDesc.ID, zoneConfigBytes)
	if !sqlutils.ZoneConfigExists(t, sqlDB, "INDEX t.public.kv@foo") {
		t.Fatal("zone config for index does not exist")
	}

	// Verify that dropping the index works.
	_, err = sqlDBRaw.Exec(`DROP INDEX t.kv@foo`)
	require.NoError(t, err)

	// Verify that the index and its zone config still exist.
	if sqlutils.ZoneConfigExists(t, sqlDB, "INDEX t.public.kv@foo") {
		t.Fatal("zone config for index still exists")
	}
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)
	// TODO(benesch): Run scrub here. It can't currently handle the way t.kv
	// declares column families.

	tableDesc = sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if _, _, err := tableDesc.FindIndexByName("foo"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
}

func TestDropIndexInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const chunkSize = 200
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	numRows := 2*chunkSize + 1
	tests.CreateKVInterleavedTable(t, sqlDB, numRows)

	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)

	tests.CheckKeyCount(t, kvDB, tableSpan, 3*numRows)

	if _, err := sqlDB.Exec(`DROP INDEX t.intlv@intlv_idx`); err != nil {
		t.Fatal(err)
	}
	tests.CheckKeyCount(t, kvDB, tableSpan, 2*numRows)

	// Ensure that index is not active.
	tableDesc = sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "intlv")
	if _, _, err := tableDesc.FindIndexByName("intlv_idx"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
}

// Tests DROP TABLE and also checks that the table data is not deleted
// via the synchronous path.
func TestDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	numRows := 2*sql.TableTruncateChunkSize + 1
	if err := tests.CreateKVTable(sqlDB, "kv", numRows); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	nameKey := sqlbase.NewPublicTableKey(keys.MinNonPredefinedUserDescID, "kv").Key(keys.SystemSQLCodec)
	gr, err := kvDB.Get(ctx, nameKey)

	if err != nil {
		t.Fatal(err)
	}

	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}

	// Add a zone config for the table.
	cfg, err := sqltestutils.AddDefaultZoneConfig(sqlDB, tableDesc.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)
	tests.CheckKeyCount(t, kvDB, tableSpan, 3*numRows)
	if _, err := sqlDB.Exec(`DROP TABLE t.kv`); err != nil {
		t.Fatal(err)
	}

	// Test that deleted table cannot be used. This prevents
	// regressions where name -> descriptor ID caches might make
	// this statement erronously work.
	if _, err := sqlDB.Exec(
		`SELECT * FROM t.kv`,
	); !testutils.IsError(err, `relation "t.kv" does not exist`) {
		t.Fatalf("different error than expected: %+v", err)
	}

	if gr, err := kvDB.Get(ctx, nameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table namekey still exists")
	}

	// Job still running, waiting for GC.
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	if err := jobutils.VerifySystemJob(t, sqlRun, 1, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUser,
		Description: `DROP TABLE t.public.kv`,
		DescriptorIDs: sqlbase.IDs{
			tableDesc.ID,
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Can create a table with the same name.
	if err := tests.CreateKVTable(sqlDB, "kv", numRows); err != nil {
		t.Fatal(err)
	}

	// A lot of garbage has been left behind to be cleaned up by the
	// asynchronous path.
	tests.CheckKeyCount(t, kvDB, tableSpan, 3*numRows)

	if err := descExists(sqlDB, true, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tableDesc.ID); err != nil {
		t.Fatal(err)
	}
}

// Test that after a DROP TABLE the table eventually gets deleted.
func TestDropTableDeleteData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()

	defer gcjob.SetSmallMaxGCIntervalForTest()()

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	const numRows = 2*sql.TableTruncateChunkSize + 1
	const numKeys = 3 * numRows
	const numTables = 5
	var descs []*sqlbase.TableDescriptor
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("test%d", i)
		if err := tests.CreateKVTable(sqlDB, tableName, numRows); err != nil {
			t.Fatal(err)
		}

		descs = append(descs, sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", tableName))

		nameKey := sqlbase.NewPublicTableKey(keys.MinNonPredefinedUserDescID, tableName).Key(keys.SystemSQLCodec)
		gr, err := kvDB.Get(ctx, nameKey)
		if err != nil {
			t.Fatal(err)
		}
		if !gr.Exists() {
			t.Fatalf("Name entry %q does not exist", nameKey)
		}

		tableSpan := descs[i].TableSpan(keys.SystemSQLCodec)
		tests.CheckKeyCount(t, kvDB, tableSpan, numKeys)

		if _, err := sqlDB.Exec(fmt.Sprintf(`DROP TABLE t.%s`, tableName)); err != nil {
			t.Fatal(err)
		}
	}

	// TODO (lucy): Maybe this test API should use an offset starting
	// from the most recent job instead.
	const migrationJobOffset = 0

	// Data hasn't been GC-ed.
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	for i := 0; i < numTables; i++ {
		if err := descExists(sqlDB, true, descs[i].ID); err != nil {
			t.Fatal(err)
		}
		tableSpan := descs[i].TableSpan(keys.SystemSQLCodec)
		tests.CheckKeyCount(t, kvDB, tableSpan, numKeys)

		if err := jobutils.VerifySystemJob(t, sqlRun, 2*i+1+migrationJobOffset, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
			Username:    security.RootUser,
			Description: fmt.Sprintf(`DROP TABLE t.public.%s`, descs[i].GetName()),
			DescriptorIDs: sqlbase.IDs{
				descs[i].ID,
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// The closure pushes a zone config reducing the TTL to 0 for descriptor i.
	pushZoneCfg := func(i int) {
		if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, descs[i].ID); err != nil {
			t.Fatal(err)
		}
	}

	checkTableGCed := func(i int) {
		testutils.SucceedsSoon(t, func() error {
			if err := descExists(sqlDB, false, descs[i].ID); err != nil {
				return err
			}

			return zoneExists(sqlDB, nil, descs[i].ID)
		})
		tableSpan := descs[i].TableSpan(keys.SystemSQLCodec)
		tests.CheckKeyCount(t, kvDB, tableSpan, 0)

		// Ensure that the job is marked as succeeded.
		if err := jobutils.VerifySystemJob(t, sqlRun, 2*i+1+migrationJobOffset, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
			Username:    security.RootUser,
			Description: fmt.Sprintf(`DROP TABLE t.public.%s`, descs[i].GetName()),
			DescriptorIDs: sqlbase.IDs{
				descs[i].ID,
			},
		}); err != nil {
			t.Fatal(err)
		}

		// Ensure that the job is marked as succeeded.
		testutils.SucceedsSoon(t, func() error {
			return jobutils.VerifySystemJob(t, sqlRun, i, jobspb.TypeSchemaChangeGC, jobs.StatusSucceeded, jobs.Record{
				Username:    security.RootUser,
				Description: fmt.Sprintf(`GC for DROP TABLE t.public.%s`, descs[i].GetName()),
				DescriptorIDs: sqlbase.IDs{
					descs[i].ID,
				},
			})
		})
	}

	// Push a new zone config for a few tables with TTL=0 so the data
	// is deleted immediately.
	barrier := rand.Intn(numTables)
	for i := 0; i < barrier; i++ {
		pushZoneCfg(i)
	}

	// Check GC worked!
	for i := 0; i < numTables; i++ {
		if i < barrier {
			checkTableGCed(i)
		} else {
			// Data still present for tables past barrier.
			tableSpan := descs[i].TableSpan(keys.SystemSQLCodec)
			tests.CheckKeyCount(t, kvDB, tableSpan, numKeys)
		}
	}

	// Push the rest of the zone configs and check all the data gets GC-ed.
	for i := barrier; i < numTables; i++ {
		pushZoneCfg(i)
	}
	for i := barrier; i < numTables; i++ {
		checkTableGCed(i)
	}
}

func writeTableDesc(
	ctx context.Context, db *kv.DB, tableDesc *sqlbase.MutableTableDescriptor,
) error {
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		tableDesc.ModificationTime = txn.CommitTimestamp()
		return txn.Put(ctx, sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.ID), tableDesc.DescriptorProto())
	})
}

// TestDropTableWhileUpgradingFormat ensures that it's safe for a migration to
// upgrade the table descriptor's format while the table is scheduled to be
// dropped.
//
// The new format must be backwards-compatible with the old format, but that's
// true in general.
func TestDropTableWhileUpgradingFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	defer gcjob.SetSmallMaxGCIntervalForTest()()

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	s, sqlDBRaw, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDBRaw)()

	const numRows = 100
	sqlutils.CreateTable(t, sqlDBRaw, "t", "a INT", numRows, sqlutils.ToRowFn(sqlutils.RowIdxFn))

	// Give the table an old format version.
	tableDesc := sqlbase.TestingGetMutableExistingTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
	tableDesc.FormatVersion = sqlbase.FamilyFormatVersion
	tableDesc.Version++
	if err := writeTableDesc(ctx, kvDB, tableDesc); err != nil {
		t.Fatal(err)
	}

	tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)
	tests.CheckKeyCount(t, kvDB, tableSpan, numRows)

	sqlDB.Exec(t, `DROP TABLE test.t`)

	// Simulate a migration upgrading the table descriptor's format version after
	// the table has been dropped but before the truncation has occurred.
	var err error
	tableDesc, err = sqlbase.GetMutableTableDescFromID(ctx, kvDB.NewTxn(ctx, ""), keys.SystemSQLCodec, tableDesc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !tableDesc.Dropped() {
		t.Fatalf("expected descriptor to be in DROP state, but was in %s", tableDesc.State)
	}
	tableDesc.FormatVersion = sqlbase.InterleavedFormatVersion
	tableDesc.Version++
	if err := writeTableDesc(ctx, kvDB, tableDesc); err != nil {
		t.Fatal(err)
	}

	// Set TTL so the data is deleted immediately.
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDBRaw, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	// Allow the schema change to proceed and verify that the data is eventually
	// deleted, despite the interleaved modification to the table descriptor.
	testutils.SucceedsSoon(t, func() error {
		return descExists(sqlDBRaw, false, tableDesc.ID)
	})
	tests.CheckKeyCount(t, kvDB, tableSpan, 0)
}

// Tests dropping a table that is interleaved within
// another table.
func TestDropTableInterleavedDeleteData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()

	defer gcjob.SetSmallMaxGCIntervalForTest()()

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	numRows := 2*sql.TableTruncateChunkSize + 1
	tests.CreateKVInterleavedTable(t, sqlDB, numRows)

	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	tableDescInterleaved := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "intlv")
	tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)

	tests.CheckKeyCount(t, kvDB, tableSpan, 3*numRows)
	if _, err := sqlDB.Exec(`DROP TABLE t.intlv`); err != nil {
		t.Fatal(err)
	}

	// Test that deleted table cannot be used. This prevents regressions where
	// name -> descriptor ID caches might make this statement erronously work.
	if _, err := sqlDB.Exec(`SELECT * FROM t.intlv`); !testutils.IsError(
		err, `relation "t.intlv" does not exist`,
	) {
		t.Fatalf("different error than expected: %v", err)
	}

	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDescInterleaved.ID); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		return descExists(sqlDB, false, tableDescInterleaved.ID)
	})

	tests.CheckKeyCount(t, kvDB, tableSpan, numRows)
}

func TestDropTableInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(`DROP TABLE t.kv`); err != nil {
		t.Fatal(err)
	}

	// We might still be able to read/write in the table inside this transaction
	// until the schema changer runs, but we shouldn't be able to ALTER it.
	if _, err := tx.Exec(`ALTER TABLE t.kv ADD COLUMN w CHAR`); !testutils.IsError(err,
		`relation "t.kv" does not exist`) {
		t.Fatalf("different error than expected: %v", err)
	}

	// Can't commit after ALTER errored, so we ROLLBACK.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

}

// Tests DROP DATABASE after DROP TABLE just before the table name has been
// recycle.
func TestDropDatabaseAfterDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Disable schema change execution so that the dropped table name
	// doesn't get recycled.
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SchemaChangeJobNoOp: func() bool {
				return true
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if err := tests.CreateKVTable(sqlDB, "kv", 100); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")

	if _, err := sqlDB.Exec(`DROP TABLE t.kv`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`DROP DATABASE t`); err != nil {
		t.Fatal(err)
	}

	// Job still running, waiting for draining names.
	// TODO (lucy): Maybe this test API should use an offset starting
	// from the most recent job instead.
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	if err := jobutils.VerifySystemJob(
		t, sqlRun, 1, jobspb.TypeSchemaChange, jobs.StatusSucceeded,
		jobs.Record{
			Username:    security.RootUser,
			Description: "DROP TABLE t.public.kv",
			DescriptorIDs: sqlbase.IDs{
				tableDesc.ID,
			},
		}); err != nil {
		t.Fatal(err)
	}
}

func TestDropAndCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = "test"
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		if _, err := db.Exec(`DROP TABLE IF EXISTS foo`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE TABLE foo (k INT PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO foo VALUES (1), (2), (3)`); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDropAndCreateDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	for i := 0; i < 20; i++ {
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS test`)
		sqlDB.Exec(t, `CREATE DATABASE test`)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1), (2), (3)`)
	}
}

// Test commands while a table is being dropped.
func TestCommandsWhileTableBeingDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	// Block schema changers so that the table we're about to DROP is not
	// actually dropped; it will be left in the "deleted" state.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SchemaChangeJobNoOp: func() bool {
				return true
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.t(a INT PRIMARY KEY);
`
	if _, err := db.Exec(sql); err != nil {
		t.Fatal(err)
	}

	// DROP the table
	if _, err := db.Exec(`DROP TABLE test.t`); err != nil {
		t.Fatal(err)
	}

	// Check that SHOW TABLES marks a dropped table with the " (dropped)"
	// suffix.
	rows, err := db.Query(`SHOW TABLES FROM test`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("table should be invisible through SHOW TABLES")
	}

	// Check that CREATE TABLE with the same name returns a proper error.
	if _, err := db.Exec(`CREATE TABLE test.t(a INT PRIMARY KEY)`); !testutils.IsError(err, `relation "t" already exists`) {
		t.Fatal(err)
	}

	// Check that DROP TABLE with the same name returns a proper error.
	if _, err := db.Exec(`DROP TABLE test.t`); !testutils.IsError(err, `relation "test.t" does not exist`) {
		t.Fatal(err)
	}
}

// Tests name reuse if a DROP VIEW|TABLE succeeds but fails
// before running the schema changer. Tests name GC via the
// asynchrous schema change path.
// TODO (lucy): This started as a test verifying that draining names still works
// in the async schema changer, which no longer exists. Should the test still
// exist?
func TestDropNameReuse(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.t(a INT PRIMARY KEY);
CREATE VIEW test.acol(a) AS SELECT a FROM test.t;
`
	if _, err := db.Exec(sql); err != nil {
		t.Fatal(err)
	}

	// DROP the view.
	if _, err := db.Exec(`DROP VIEW test.acol`); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		_, err := db.Exec(`CREATE TABLE test.acol(a INT PRIMARY KEY);`)
		return err
	})

	// DROP the table.
	if _, err := db.Exec(`DROP TABLE test.t`); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		_, err := db.Exec(`CREATE TABLE test.t(a INT PRIMARY KEY);`)
		return err
	})
}

// TestDropIndexHandlesRetriableErrors is a regression test against #48474.
// The bug was that retriable errors, which are generally possible, were being
// treated as assertion failures.
func TestDropIndexHandlesRetriableErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	rf := newDynamicRequestFilter()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter: rf.filter,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// What we want to do is have a transaction which does the planning work to
	// drop an index. Then we want to expose the execution of the DROP INDEX to
	// an error when retrieving the mutable table descriptor. We'll do this by
	// injecting a ReadWithinUncertainty error underneath the DROP INDEX
	// after planning has concluded.

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, "CREATE TABLE foo (i INT PRIMARY KEY, j INT, INDEX j_idx (j))")

	var tableID uint32
	tdb.QueryRow(t, `
SELECT
    table_id
FROM
    crdb_internal.tables
WHERE
    name = $1 AND database_name = current_database();`,
		"foo").Scan(&tableID)

	// Start the user transaction and enable tracing as we'll use the trace
	// to determine when planning has concluded.
	txn, err := tc.ServerConn(0).Begin()
	require.NoError(t, err)
	_, err = txn.Exec("SET TRACING = on")
	require.NoError(t, err)
	// Let's find out our transaction ID for our transaction by running a query.
	// We'll also use this query to install a refresh span over the table data.
	// Inject a request filter to snag the transaction ID.
	tablePrefix := keys.SystemSQLCodec.TablePrefix(tableID)
	tableSpan := roachpb.Span{
		Key:    tablePrefix,
		EndKey: tablePrefix.PrefixEnd(),
	}
	var filterState struct {
		syncutil.Mutex
		txnID uuid.UUID
	}
	getTxnID := func() uuid.UUID {
		filterState.Lock()
		defer filterState.Unlock()
		return filterState.txnID
	}
	rf.setFilter(func(ctx context.Context, request roachpb.BatchRequest) *roachpb.Error {
		if request.Txn == nil || request.Txn.Name != sql.SQLTxnName {
			return nil
		}
		filterState.Lock()
		defer filterState.Unlock()
		if filterState.txnID != (uuid.UUID{}) {
			return nil
		}
		if scanRequest, ok := request.GetArg(roachpb.Scan); ok {
			scan := scanRequest.(*roachpb.ScanRequest)
			if scan.Span().Overlaps(tableSpan) {
				filterState.txnID = request.Txn.ID
			}
		}
		return nil
	})

	// Run the scan of the table to activate the filter as well as add the
	// refresh span over the table data.
	var trash int
	require.Equal(t, gosql.ErrNoRows,
		txn.QueryRow("SELECT * FROM foo").Scan(&trash))
	rf.setFilter(nil)
	require.NotEqual(t, uuid.UUID{}, getTxnID())

	// Perform a write after the above read so that a refresh will fail and
	// observe its timestamp.
	tdb.Exec(t, "INSERT INTO foo VALUES (1)")
	var afterInsertStr string
	tdb.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&afterInsertStr)
	afterInsert, err := sql.ParseHLC(afterInsertStr)
	require.NoError(t, err)

	// Now set up a filter to detect when the DROP INDEX execution will begin
	// and inject an error forcing a refresh above the conflicting write which
	// will fail. We'll want to ensure that we get a retriable error.
	// Use the below pattern to detect when the user transaction has finished
	// planning and is now executing.
	dropIndexPlanningEndsRE := regexp.MustCompile("(?s)planning starts: DROP INDEX.*planning ends")
	rf.setFilter(func(ctx context.Context, request roachpb.BatchRequest) *roachpb.Error {
		if request.Txn == nil {
			return nil
		}
		filterState.Lock()
		defer filterState.Unlock()
		if filterState.txnID != request.Txn.ID {
			return nil
		}
		sp := opentracing.SpanFromContext(ctx)
		rec := tracing.GetRecording(sp)
		if !dropIndexPlanningEndsRE.MatchString(rec.String()) {
			return nil
		}
		if getRequest, ok := request.GetArg(roachpb.Get); ok {
			put := getRequest.(*roachpb.GetRequest)
			if put.Key.Equal(sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, sqlbase.ID(tableID))) {
				filterState.txnID = uuid.UUID{}
				return roachpb.NewError(roachpb.NewReadWithinUncertaintyIntervalError(
					request.Txn.ReadTimestamp, afterInsert, request.Txn))
			}
		}
		return nil
	})

	_, err = txn.Exec("DROP INDEX foo@j_idx")
	require.Truef(t, isRetryableErr(err), "drop index error: %v", err)
	require.NoError(t, txn.Rollback())
}

// TestDropDatabaseWithForeignKeys tests that databases containing tables with
// foreign key relationships can be dropped and GC'ed. This is a regression test
// for #50344, which is a bug ultimately caused by the fact that when we remove
// foreign keys as part of DROP DATABASE CASCADE, we create schema change jobs
// as part of updating the referenced tables. Those jobs, when running, will
// detect that the table is in a dropped state and queue an extraneous GC job.
// We test that those GC jobs don't interfere with the main GC job for the
// entire database.
func TestDropDatabaseWithForeignKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	_, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.parent(k INT PRIMARY KEY);
CREATE TABLE t.child(k INT PRIMARY KEY REFERENCES t.parent);
`)
	require.NoError(t, err)

	dbNameKey := sqlbase.NewDatabaseKey("t").Key(keys.SystemSQLCodec)
	r, err := kvDB.Get(ctx, dbNameKey)
	require.NoError(t, err)
	require.True(t, r.Exists())
	dbID := sqlbase.ID(r.ValueInt())

	parentNameKey := sqlbase.NewPublicTableKey(dbID, "parent").Key(keys.SystemSQLCodec)
	r, err = kvDB.Get(ctx, parentNameKey)
	require.NoError(t, err)
	require.True(t, r.Exists())
	parentID := sqlbase.ID(r.ValueInt())

	parentDescKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, parentID)
	desc := &sqlbase.Descriptor{}
	ts, err := kvDB.GetProtoTs(ctx, parentDescKey, desc)
	require.NoError(t, err)
	parentDesc := desc.Table(ts)

	childNameKey := sqlbase.NewPublicTableKey(dbID, "child").Key(keys.SystemSQLCodec)
	r, err = kvDB.Get(ctx, childNameKey)
	require.NoError(t, err)
	require.True(t, r.Exists())
	childID := sqlbase.ID(r.ValueInt())

	childDescKey := sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, childID)
	desc = &sqlbase.Descriptor{}
	ts, err = kvDB.GetProtoTs(ctx, childDescKey, desc)
	require.NoError(t, err)
	childDesc := desc.Table(ts)

	_, err = sqlDB.Exec(`DROP DATABASE t CASCADE;`)
	require.NoError(t, err)

	// Push a new zone config for the table with TTL=0 so the data is
	// deleted immediately.
	_, err = sqltestutils.AddImmediateGCZoneConfig(sqlDB, dbID)
	require.NoError(t, err)

	// Ensure the main GC job for the whole database succeeds.
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	testutils.SucceedsSoon(t, func() error {
		var count int
		sqlRun.QueryRow(t, `SELECT count(*) FROM [SHOW JOBS] WHERE description = 'GC for DROP DATABASE t CASCADE' AND status = 'succeeded'`).Scan(&count)
		if count != 1 {
			return errors.Newf("expected 1 result, got %d", count)
		}
		return nil
	})
	// Ensure the extra GC job that also gets queued succeeds. Currently this job
	// has a nonsensical description due to the fact that the original job queued
	// for updating the referenced table has an empty description.
	testutils.SucceedsSoon(t, func() error {
		var count int
		sqlRun.QueryRow(t, `SELECT count(*) FROM [SHOW JOBS] WHERE description = 'GC for ' AND status = 'succeeded'`).Scan(&count)
		if count != 1 {
			return errors.Newf("expected 1 result, got %d", count)
		}
		return nil
	})

	// Check that the data was cleaned up.
	tests.CheckKeyCount(t, kvDB, parentDesc.TableSpan(keys.SystemSQLCodec), 0)
	tests.CheckKeyCount(t, kvDB, childDesc.TableSpan(keys.SystemSQLCodec), 0)
}
