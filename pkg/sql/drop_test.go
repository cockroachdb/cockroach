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
	"strings"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/startupmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Returns an error if a zone config for the specified table or
// database ID doesn't match the expected parameter. If expected
// is nil, then we verify no zone config exists.
func zoneExists(sqlDB *gosql.DB, expected *zonepb.ZoneConfig, id descpb.ID) error {
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
		var storedID descpb.ID
		var val []byte
		if err := rows.Scan(&storedID, &val); err != nil {
			return errors.Wrap(err, "row scan failed")
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
func descExists(sqlDB *gosql.DB, exists bool, id descpb.ID) error {
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
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	if _, err := sqlDB.Exec(`
SET use_declarative_schema_changer = 'off';
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	tbDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	var dbDesc catalog.DatabaseDescriptor
	require.NoError(t, sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) (err error) {
		dbDesc, err = col.Direct().MustGetDatabaseDescByID(ctx, txn, tbDesc.GetParentID())
		return err
	}))

	// Add a zone config for both the table and database.
	cfg := zonepb.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tbDesc.GetID(), buf); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, dbDesc.GetID(), buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tbDesc.GetID()); err != nil {
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

	if err := descExists(sqlDB, true, tbDesc.GetID()); err != nil {
		t.Fatal(err)
	}
	tbNameKey := catalogkeys.EncodeNameKey(keys.SystemSQLCodec, tbDesc)
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

	dbNameKey := catalogkeys.EncodeNameKey(keys.SystemSQLCodec, dbDesc)
	if gr, err := kvDB.Get(ctx, dbNameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("database descriptor key still exists after database is dropped")
	}

	if err := zoneExists(sqlDB, &cfg, tbDesc.GetID()); err != nil {
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
		Username:    security.RootUserName(),
		Description: "DROP DATABASE t CASCADE",
		DescriptorIDs: descpb.IDs{
			tbDesc.GetID(),
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// Test that an empty, dropped database's zone config gets deleted immediately.
func TestDropDatabaseEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
`); err != nil {
		t.Fatal(err)
	}

	dKey := catalogkeys.MakeDatabaseNameKey(keys.SystemSQLCodec, "t")
	r, err := kvDB.Get(ctx, dKey)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Exists() {
		t.Fatalf(`database "t" does not exist`)
	}
	dbID := descpb.ID(r.ValueInt())

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
	defer log.Scope(t).Close(t)

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
SET use_declarative_schema_changer = 'off';
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
CREATE TABLE t.kv2 (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.kv2 VALUES ('c', 'd'), ('a', 'b'), ('e', 'a');
`); err != nil {
		t.Fatal(err)
	}

	tbDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	tb2Desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv2")
	var dbDesc catalog.DatabaseDescriptor
	require.NoError(t, sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) (err error) {
		dbDesc, err = col.Direct().MustGetDatabaseDescByID(ctx, txn, tbDesc.GetParentID())
		return err
	}))

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
		Username:    security.RootUserName(),
		Description: "DROP DATABASE t CASCADE",
		DescriptorIDs: descpb.IDs{
			tbDesc.GetID(), tb2Desc.GetID(),
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Push a new zone config for the table with TTL=0 so the data is
	// deleted immediately.
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tbDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		if err := descExists(sqlDB, false, tbDesc.GetID()); err != nil {
			return err
		}

		return zoneExists(sqlDB, nil, tbDesc.GetID())
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
			Username:    security.RootUserName(),
			Description: "GC for DROP DATABASE t CASCADE",
			DescriptorIDs: descpb.IDs{
				tbDesc.GetID(), tb2Desc.GetID(),
			},
		})
	})

	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tb2Desc.GetID()); err != nil {
		t.Fatal(err)
	}
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, dbDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		if err := descExists(sqlDB, false, tb2Desc.GetID()); err != nil {
			return err
		}

		return zoneExists(sqlDB, nil, tb2Desc.GetID())
	})

	// Table 2 data is deleted.
	tests.CheckKeyCount(t, kvDB, table2Span, 0)

	if err := jobutils.VerifySystemJob(t, sqlRun, migrationJobOffset, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUserName(),
		Description: "DROP DATABASE t CASCADE",
		DescriptorIDs: descpb.IDs{
			tbDesc.GetID(), tb2Desc.GetID(),
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Database zone config is removed once all table data and zone configs are removed.
	if err := zoneExists(sqlDB, nil, dbDesc.GetID()); err != nil {
		t.Fatal(err)
	}
}

func TestDropIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	tests.CheckKeyCount(t, kvDB, tableDesc.TableSpan(keys.SystemSQLCodec), 3*numRows)
	idx, err := tableDesc.FindIndexWithName("foo")
	if err != nil {
		t.Fatal(err)
	}
	indexSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, idx.GetID())
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)
	if _, err := sqlDB.Exec(`DROP INDEX t.kv@foo`); err != nil {
		t.Fatal(err)
	}

	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if _, err := tableDesc.FindIndexWithName("foo"); err == nil {
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
		Username:    security.RootUserName(),
		Description: `DROP INDEX t.public.kv@foo`,
		DescriptorIDs: descpb.IDs{
			tableDesc.GetID(),
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`CREATE INDEX foo on t.kv (v);`); err != nil {
		t.Fatal(err)
	}

	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	newIdx, err := tableDesc.FindIndexWithName("foo")
	if err != nil {
		t.Fatal(err)
	}
	newIdxSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, newIdx.GetID())
	tests.CheckKeyCount(t, kvDB, newIdxSpan, numRows)
	tests.CheckKeyCount(t, kvDB, tableDesc.TableSpan(keys.SystemSQLCodec), 4*numRows)

	clearIndexAttempt = true
	// Add a zone config for the table.
	if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		return jobutils.VerifySystemJob(t, sqlRun, migrationJobOffset+1, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
			Username:    security.RootUserName(),
			Description: `DROP INDEX t.public.kv@foo`,
			DescriptorIDs: descpb.IDs{
				tableDesc.GetID(),
			},
		})
	})

	testutils.SucceedsSoon(t, func() error {
		if err := jobutils.VerifySystemJob(t, sqlRun, 0, jobspb.TypeSchemaChangeGC, jobs.StatusSucceeded, jobs.Record{
			Username:    security.RootUserName(),
			Description: `GC for temporary index used during index backfill`,
			DescriptorIDs: descpb.IDs{
				tableDesc.GetID(),
			},
		}); err != nil {
			return err
		}

		return jobutils.VerifySystemJob(t, sqlRun, 1, jobspb.TypeSchemaChangeGC, jobs.StatusSucceeded, jobs.Record{
			Username:    security.RootUserName(),
			Description: `GC for DROP INDEX t.public.kv@foo`,
			DescriptorIDs: descpb.IDs{
				tableDesc.GetID(),
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
	defer log.Scope(t).Close(t)

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
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	index, err := tableDesc.FindIndexWithName("foo")
	if err != nil {
		t.Fatal(err)
	}
	indexSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, index.GetID())
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)

	// Hack in zone configs for the primary and secondary indexes. (You need a CCL
	// binary to do this properly.) Dropping the index will thus require
	// regenerating the zone config's SubzoneSpans, which will fail with a "CCL
	// required" error.
	zoneConfig := zonepb.ZoneConfig{
		Subzones: []zonepb.Subzone{
			{IndexID: uint32(tableDesc.GetPrimaryIndexID()), Config: s.(*server.TestServer).Cfg.DefaultZoneConfig},
			{IndexID: uint32(index.GetID()), Config: s.(*server.TestServer).Cfg.DefaultZoneConfig},
		},
	}
	zoneConfigBytes, err := protoutil.Marshal(&zoneConfig)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, `INSERT INTO system.zones VALUES ($1, $2)`, tableDesc.GetID(), zoneConfigBytes)
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

	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if _, err := tableDesc.FindIndexWithName("foo"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
}

// Tests DROP TABLE and also checks that the table data is not deleted
// via the synchronous path.
func TestDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	numRows := 2*row.TableTruncateChunkSize + 1
	if err := tests.CreateKVTable(sqlDB, "kv", numRows); err != nil {
		t.Fatal(err)
	}

	parentDatabaseID := descpb.ID(sqlutils.QueryDatabaseID(t, sqlDB, "t"))
	parentSchemaID := descpb.ID(sqlutils.QuerySchemaID(t, sqlDB, "t", "public"))

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	nameKey := catalogkeys.MakeObjectNameKey(keys.SystemSQLCodec, parentDatabaseID, parentSchemaID, "kv")
	gr, err := kvDB.Get(ctx, nameKey)

	if err != nil {
		t.Fatal(err)
	}

	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}

	// Add a zone config for the table.
	cfg, err := sqltestutils.AddDefaultZoneConfig(sqlDB, tableDesc.GetID())
	if err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)
	tests.CheckKeyCount(t, kvDB, tableSpan, 3*numRows)
	_, err = sqlDB.Exec(`SET use_declarative_schema_changer = 'off';`)
	require.NoError(t, err)
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
		Username:    security.RootUserName(),
		Description: `DROP TABLE t.public.kv`,
		DescriptorIDs: descpb.IDs{
			tableDesc.GetID(),
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

	if err := descExists(sqlDB, true, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tableDesc.GetID()); err != nil {
		t.Fatal(err)
	}
}

// Test that after a DROP TABLE the table eventually gets deleted.
func TestDropTableDeleteData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()

	defer gcjob.SetSmallMaxGCIntervalForTest()()

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	// Disable strict GC TTL enforcement because we're going to shove a zero-value
	// TTL into the system with AddImmediateGCZoneConfig.
	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	const numRows = 2*row.TableTruncateChunkSize + 1
	const numKeys = 3 * numRows
	const numTables = 5
	var descs []catalog.TableDescriptor
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("test%d", i)
		if err := tests.CreateKVTable(sqlDB, tableName, numRows); err != nil {
			t.Fatal(err)
		}

		descs = append(descs, desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", tableName))

		parentDatabaseID := descpb.ID(sqlutils.QueryDatabaseID(t, sqlDB, "t"))
		parentSchemaID := descpb.ID(sqlutils.QuerySchemaID(t, sqlDB, "t", "public"))

		nameKey := catalogkeys.MakeObjectNameKey(keys.SystemSQLCodec, parentDatabaseID, parentSchemaID, tableName)
		gr, err := kvDB.Get(ctx, nameKey)
		if err != nil {
			t.Fatal(err)
		}
		if !gr.Exists() {
			t.Fatalf("Name entry %q does not exist", nameKey)
		}

		tableSpan := descs[i].TableSpan(keys.SystemSQLCodec)
		tests.CheckKeyCount(t, kvDB, tableSpan, numKeys)
		_, err = sqlDB.Exec(`SET use_declarative_schema_changer = 'off';`)
		require.NoError(t, err)
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
		if err := descExists(sqlDB, true, descs[i].GetID()); err != nil {
			t.Fatal(err)
		}
		tableSpan := descs[i].TableSpan(keys.SystemSQLCodec)
		tests.CheckKeyCount(t, kvDB, tableSpan, numKeys)

		if err := jobutils.VerifySystemJob(t, sqlRun, 2*i+1+migrationJobOffset, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
			Username:    security.RootUserName(),
			Description: fmt.Sprintf(`DROP TABLE t.public.%s`, descs[i].GetName()),
			DescriptorIDs: descpb.IDs{
				descs[i].GetID(),
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// The closure pushes a zone config reducing the TTL to 0 for descriptor i.
	pushZoneCfg := func(i int) {
		if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, descs[i].GetID()); err != nil {
			t.Fatal(err)
		}
	}

	checkTableGCed := func(i int) {
		testutils.SucceedsSoon(t, func() error {
			if err := descExists(sqlDB, false, descs[i].GetID()); err != nil {
				return err
			}

			return zoneExists(sqlDB, nil, descs[i].GetID())
		})
		tableSpan := descs[i].TableSpan(keys.SystemSQLCodec)
		tests.CheckKeyCount(t, kvDB, tableSpan, 0)

		// Ensure that the job is marked as succeeded.
		if err := jobutils.VerifySystemJob(t, sqlRun, 2*i+1+migrationJobOffset, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
			Username:    security.RootUserName(),
			Description: fmt.Sprintf(`DROP TABLE t.public.%s`, descs[i].GetName()),
			DescriptorIDs: descpb.IDs{
				descs[i].GetID(),
			},
		}); err != nil {
			t.Fatal(err)
		}

		// Ensure that the job is marked as succeeded.
		testutils.SucceedsSoon(t, func() error {
			return jobutils.VerifySystemJob(t, sqlRun, (i*2)+1, jobspb.TypeSchemaChangeGC, jobs.StatusSucceeded, jobs.Record{
				Username:    security.RootUserName(),
				Description: fmt.Sprintf(`GC for DROP TABLE t.public.%s`, descs[i].GetName()),
				DescriptorIDs: descpb.IDs{
					descs[i].GetID(),
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

func writeTableDesc(ctx context.Context, db *kv.DB, tableDesc *tabledesc.Mutable) error {
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		tableDesc.ModificationTime = txn.CommitTimestamp()
		return txn.Put(ctx, catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.ID), tableDesc.DescriptorProto())
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	defer gcjob.SetSmallMaxGCIntervalForTest()()

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
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
	tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
	tableDesc.FormatVersion = descpb.FamilyFormatVersion
	tableDesc.Version++
	if err := writeTableDesc(ctx, kvDB, tableDesc); err != nil {
		t.Fatal(err)
	}

	tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)
	tests.CheckKeyCount(t, kvDB, tableSpan, numRows)

	sqlDB.Exec(t, `DROP TABLE test.t`)

	// Simulate a migration upgrading the table descriptor's format version after
	// the table has been dropped but before the truncation has occurred.
	if err := sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) (err error) {
		tbl, err := col.Direct().MustGetTableDescByID(ctx, txn, tableDesc.ID)
		if err != nil {
			return err
		}
		tableDesc = tabledesc.NewBuilder(tbl.TableDesc()).BuildExistingMutableTable()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !tableDesc.Dropped() {
		t.Fatalf("expected descriptor to be in DROP state, but was in %s", tableDesc.State)
	}
	tableDesc.FormatVersion = descpb.InterleavedFormatVersion
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

func TestDropTableInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)
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

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")

	_, err := sqlDB.Exec(`SET use_declarative_schema_changer = 'off';`)
	require.NoError(t, err)
	if _, err = sqlDB.Exec(`DROP TABLE t.kv`); err != nil {
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
			Username:    security.RootUserName(),
			Description: "DROP TABLE t.public.kv",
			DescriptorIDs: descpb.IDs{
				tableDesc.GetID(),
			},
		}); err != nil {
		t.Fatal(err)
	}
}

func TestDropAndCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)

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
	defer log.Scope(t).Close(t)

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
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	rf := newDynamicRequestFilter()
	dropIndexPlanningDoneCh := make(chan struct{})
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter: rf.filter,
				},
				SQLExecutor: &sql.ExecutorTestingKnobs{
					BeforeExecute: func(ctx context.Context, stmt string) {
						if strings.Contains(stmt, "DROP INDEX") {
							close(dropIndexPlanningDoneCh)
						}
					},
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

	txn, err := tc.ServerConn(0).Begin()
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
	afterInsert, err := tree.ParseHLC(afterInsertStr)
	require.NoError(t, err)

	// Now set up a filter to detect when the DROP INDEX execution will begin and
	// inject an error forcing a refresh above the conflicting write which will
	// fail. We'll want to ensure that we get a retriable error. Use the below
	// pattern to detect when the user transaction has finished planning and is
	// now executing: we don't want to inject the error during planning.
	rf.setFilter(func(ctx context.Context, request roachpb.BatchRequest) *roachpb.Error {
		if request.Txn == nil {
			return nil
		}
		filterState.Lock()
		defer filterState.Unlock()
		if filterState.txnID != request.Txn.ID {
			return nil
		}
		select {
		case <-dropIndexPlanningDoneCh:
		default:
			return nil
		}
		if getRequest, ok := request.GetArg(roachpb.Get); ok {
			put := getRequest.(*roachpb.GetRequest)
			if put.Key.Equal(catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, descpb.ID(tableID))) {
				filterState.txnID = uuid.UUID{}
				return roachpb.NewError(roachpb.NewReadWithinUncertaintyIntervalError(
					request.Txn.ReadTimestamp, afterInsert, hlc.Timestamp{}, request.Txn))
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
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

	_, err := sqlDB.Exec(`
SET use_declarative_schema_changer = 'off';
CREATE DATABASE t;
CREATE TABLE t.parent(k INT PRIMARY KEY);
CREATE TABLE t.child(k INT PRIMARY KEY REFERENCES t.parent);
`)
	require.NoError(t, err)

	parentDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "parent")
	childDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "child")

	_, err = sqlDB.Exec(`DROP DATABASE t CASCADE;`)
	require.NoError(t, err)

	// Push a new zone config for the table with TTL=0 so the data is
	// deleted immediately.
	_, err = sqltestutils.AddImmediateGCZoneConfig(sqlDB, parentDesc.GetParentID())
	require.NoError(t, err)

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	// Ensure the main and the extra GC jobs both succeed.
	sqlRun.CheckQueryResultsRetry(t, `
SELECT
	description
FROM
	[SHOW JOBS]
WHERE
	description LIKE 'GC for %' AND job_type = 'SCHEMA CHANGE GC' AND status = 'succeeded'
ORDER BY
	description;`,
		[][]string{{
			// Main GC job:
			`GC for DROP DATABASE t CASCADE`,
		}, {
			// Extra GC job:
			`GC for updating table "parent" after removing constraint "child_k_fkey" from table "t.public.child"`,
		}},
	)

	// Check that the data was cleaned up.
	tests.CheckKeyCount(t, kvDB, parentDesc.TableSpan(keys.SystemSQLCodec), 0)
	tests.CheckKeyCount(t, kvDB, childDesc.TableSpan(keys.SystemSQLCodec), 0)
}

// Test that non-physical table deletions like DROP VIEW are immediate instead
// of via a GC job.
func TestDropPhysicalTableGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	_, err := sqlDB.Exec(`SET use_declarative_schema_changer = 'off';`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`CREATE DATABASE test;`)
	require.NoError(t, err)
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)

	type tableInstance struct {
		name         string
		sqlType      string
		isPhysical   bool
		createClause string
	}

	for _, table := range [4]tableInstance{
		{name: "t", sqlType: "TABLE", isPhysical: true, createClause: `(a INT PRIMARY KEY)`},
		{name: "mv", sqlType: "MATERIALIZED VIEW", isPhysical: true, createClause: `AS SELECT 1`},
		{name: "s", sqlType: "SEQUENCE", isPhysical: true, createClause: `START 1`},
		{name: "v", sqlType: "VIEW", isPhysical: false, createClause: `AS SELECT 1`},
	} {
		// Create table.
		_, err := sqlDB.Exec(fmt.Sprintf(`CREATE %s test.%s %s;`, table.sqlType, table.name, table.createClause))
		require.NoError(t, err)
		// Fetch table descriptor ID for future system table lookups.
		tableDescriptor := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", table.name)
		require.NotNil(t, tableDescriptor)
		tableDescriptorId := tableDescriptor.GetID()
		if table.sqlType != "VIEW" {
			// Create a new zone config for the table to test its proper deletion.
			_, err = sqlDB.Exec(fmt.Sprintf(`ALTER TABLE test.%s CONFIGURE ZONE USING gc.ttlseconds = 123456;`, table.name))
			require.NoError(t, err)
		}
		// Drop table.
		_, err = sqlDB.Exec(fmt.Sprintf(`DROP %s test.%s;`, table.sqlType, table.name))
		require.NoError(t, err)
		// Check for GC job kickoff.
		var actualGCJobs int
		countSql := fmt.Sprintf(
			`SELECT sum(CASE description WHEN 'GC for DROP %s test.public.%s' THEN 1 ELSE 0 END) FROM [SHOW JOBS]`,
			table.sqlType,
			table.name)
		sqlRun.QueryRow(t, countSql).Scan(&actualGCJobs)
		if table.isPhysical {
			// GC job should be created.
			require.Equalf(t, 1, actualGCJobs, "Expected one GC job for DROP %s.", table.name, actualGCJobs)
		} else {
			// GC job should not be created.
			require.Zerof(t, actualGCJobs, "Expected no GC job for DROP %s.", actualGCJobs, table.name)
			// Test deletion of non-physical table descriptor.
			const idCountSqlFmt = `SELECT sum(CASE id WHEN %d THEN 1 ELSE 0 END) FROM system.%s`
			var actualDescriptors int
			sqlRun.QueryRow(t, fmt.Sprintf(idCountSqlFmt, tableDescriptorId, "descriptor")).Scan(&actualDescriptors)
			require.Zerof(t, actualDescriptors, "Descriptor for '%s' was not deleted as expected.", table.name)
			// Test deletion of non-physical table zone config.
			var actualZoneConfigs int
			sqlRun.QueryRow(t, fmt.Sprintf(idCountSqlFmt, tableDescriptorId, "zones")).Scan(&actualZoneConfigs)
			require.Zerof(t, actualZoneConfigs, "Zone config for '%s' was not deleted as expected.", table.name)
		}
	}
}
