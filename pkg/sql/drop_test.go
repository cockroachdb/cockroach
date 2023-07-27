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
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
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
	ctx, cancel := context.WithCancel(context.Background())
	params.Knobs.GCJob = &sql.GCJobTestingKnobs{
		RunBeforeResume: func(jobID jobspb.JobID) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	defer cancel()

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	tbDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	var dbDesc catalog.DatabaseDescriptor
	require.NoError(t, sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
		dbDesc, err = col.ByID(txn.KV()).Get().Database(ctx, tbDesc.GetParentID())
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
	if err := jobutils.VerifySystemJob(t, sqlRun, 0, jobspb.TypeNewSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:      username.RootUserName(),
		Description:   "DROP DATABASE t CASCADE",
		DescriptorIDs: nil,
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

	dKey := catalogkeys.EncodeNameKey(keys.SystemSQLCodec, descpb.NameInfo{Name: "t"})
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
	params, _ := tests.CreateTestServerParams()
	// Speed up mvcc queue scan.
	params.ScanMaxIdleTime = time.Millisecond

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	_, err := sqlDB.Exec(`SET CLUSTER SETTING sql.gc_job.wait_for_gc.interval = '1s';`)
	require.NoError(t, err)

	// Refresh protected timestamp cache immediately to make MVCC GC queue to
	// process GC immediately.
	_, err = sqlDB.Exec(`SET CLUSTER SETTING kv.protectedts.poll_interval = '1s';`)
	require.NoError(t, err)

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

	tbDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	tb2Desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv2")
	var dbDesc catalog.DatabaseDescriptor
	require.NoError(t, sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
		dbDesc, err = col.ByID(txn.KV()).Get().Database(ctx, tbDesc.GetParentID())
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

	tests.CheckKeyCountIncludingTombstoned(t, s, tableSpan, 6)
	tests.CheckKeyCountIncludingTombstoned(t, s, table2Span, 6)

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	if err := jobutils.VerifySystemJob(t, sqlRun, 0,
		jobspb.TypeNewSchemaChange, jobs.StatusSucceeded, jobs.Record{
			Username:    username.RootUserName(),
			Description: "DROP DATABASE t CASCADE",
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
	tests.CheckKeyCountIncludingTombstoned(t, s, tableSpan, 0)
	tests.CheckKeyCountIncludingTombstoned(t, s, table2Span, 6)

	def := zonepb.DefaultZoneConfig()
	if err := zoneExists(sqlDB, &def, dbDesc.GetID()); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		return jobutils.VerifySystemJob(t, sqlRun, 0, jobspb.TypeSchemaChangeGC, jobs.StatusRunning, jobs.Record{
			Username:    username.NodeUserName(),
			Description: "GC for DROP DATABASE t CASCADE",
			DescriptorIDs: descpb.IDs{
				tbDesc.GetID(), tb2Desc.GetID(), dbDesc.GetID(),
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
	tests.CheckKeyCountIncludingTombstoned(t, s, table2Span, 0)

	testutils.SucceedsSoon(t, func() error {
		return jobutils.VerifySystemJob(t, sqlRun, 0, jobspb.TypeSchemaChangeGC, jobs.StatusSucceeded, jobs.Record{
			Username:    username.NodeUserName(),
			Description: "GC for DROP DATABASE t CASCADE",
			DescriptorIDs: descpb.IDs{
				tbDesc.GetID(), tb2Desc.GetID(), dbDesc.GetID(),
			},
		})
	})

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
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
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
	idx, err := catalog.MustFindIndexByName(tableDesc, "foo")
	if err != nil {
		t.Fatal(err)
	}
	indexSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, idx.GetID())
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)
	if _, err := sqlDB.Exec(`DROP INDEX t.kv@foo`); err != nil {
		t.Fatal(err)
	}

	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if _, err := catalog.MustFindIndexByName(tableDesc, "foo"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	if err := jobutils.VerifySystemJob(t, sqlRun, 1, jobspb.TypeNewSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    username.RootUserName(),
		Description: `DROP INDEX t.public.kv@foo`,
	}); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		if err := jobutils.VerifyRunningSystemJob(t, sqlRun, 1, jobspb.TypeSchemaChangeGC, sql.RunningStatusWaitingForMVCCGC, jobs.Record{
			Username:    username.NodeUserName(),
			Description: `GC for DROP INDEX t.public.kv@foo`,
			DescriptorIDs: descpb.IDs{
				tableDesc.GetID(),
			},
		}); err != nil {
			return err
		}
		return nil
	})

	if _, err := sqlDB.Exec(`CREATE INDEX foo on t.kv (v);`); err != nil {
		t.Fatal(err)
	}

	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	newIdx, err := catalog.MustFindIndexByName(tableDesc, "foo")
	if err != nil {
		t.Fatal(err)
	}
	newIdxSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, newIdx.GetID())

	testutils.SucceedsSoon(t, func() error {
		if err := jobutils.VerifyRunningSystemJob(t, sqlRun, 2, jobspb.TypeSchemaChangeGC, sql.RunningStatusWaitingForMVCCGC, jobs.Record{
			Username:    username.NodeUserName(),
			Description: `GC for CREATE INDEX foo ON t.public.kv (v)`,
			DescriptorIDs: descpb.IDs{
				tableDesc.GetID(),
			},
		}); err != nil {
			return err
		}
		return nil
	})

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
	index, err := catalog.MustFindIndexByName(tableDesc, "foo")
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

	// TODO(benesch): Run scrub here. It can't currently handle the way t.kv
	// declares column families.

	tableDesc = desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	if _, err := catalog.MustFindIndexByName(tableDesc, "foo"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
}

// Tests DROP TABLE and also checks that the table data is not deleted
// via the synchronous path.
func TestDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	ctx, cancel := context.WithCancel(context.Background())
	params.Knobs.GCJob = &sql.GCJobTestingKnobs{
		RunBeforeResume: func(jobID jobspb.JobID) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	defer cancel()

	numRows := 2*row.TableTruncateChunkSize + 1
	if err := tests.CreateKVTable(sqlDB, "kv", numRows); err != nil {
		t.Fatal(err)
	}

	parentDatabaseID := descpb.ID(sqlutils.QueryDatabaseID(t, sqlDB, "t"))
	parentSchemaID := descpb.ID(sqlutils.QuerySchemaID(t, sqlDB, "t", "public"))

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "kv")
	nameKey := catalogkeys.EncodeNameKey(keys.SystemSQLCodec, &descpb.NameInfo{
		ParentID:       parentDatabaseID,
		ParentSchemaID: parentSchemaID,
		Name:           "kv",
	})
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
	if err := jobutils.VerifySystemJob(t, sqlRun, 1,
		jobspb.TypeNewSchemaChange, jobs.StatusSucceeded, jobs.Record{
			Username:      username.RootUserName(),
			Description:   `DROP TABLE t.public.kv`,
			DescriptorIDs: nil,
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
	// Speed up mvcc queue scan.
	params.ScanMaxIdleTime = time.Millisecond

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDB.Exec(`SET CLUSTER SETTING sql.gc_job.wait_for_gc.interval = '1s';`)
	require.NoError(t, err)

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

		nameKey := catalogkeys.EncodeNameKey(keys.SystemSQLCodec, &descpb.NameInfo{
			ParentID:       parentDatabaseID,
			ParentSchemaID: parentSchemaID,
			Name:           tableName,
		})
		gr, err := kvDB.Get(ctx, nameKey)
		if err != nil {
			t.Fatal(err)
		}
		if !gr.Exists() {
			t.Fatalf("Name entry %q does not exist", nameKey)
		}

		tableSpan := descs[i].TableSpan(keys.SystemSQLCodec)
		tests.CheckKeyCount(t, kvDB, tableSpan, numKeys)

		if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, descs[i].GetID()); err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < numTables; i++ {
		if _, err := sqlDB.Exec(fmt.Sprintf(`DROP TABLE t.%s`, descs[i].GetName())); err != nil {
			t.Fatal(err)
		}
	}

	// Data hasn't been GC-ed.
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	for i := 0; i < numTables; i++ {
		if err := descExists(sqlDB, true, descs[i].GetID()); err != nil {
			t.Fatal(err)
		}
		tableSpan := descs[i].TableSpan(keys.SystemSQLCodec)
		tests.CheckKeyCountIncludingTombstoned(t, s, tableSpan, numKeys)

		if err := jobutils.VerifySystemJob(t, sqlRun, numTables+i,
			jobspb.TypeNewSchemaChange, jobs.StatusSucceeded, jobs.Record{
				Username:    username.RootUserName(),
				Description: fmt.Sprintf(`DROP TABLE t.public.%s`, descs[i].GetName()),
			}); err != nil {
			t.Fatal(err)
		}
	}

	// Refresh protected timestamp cache immediately to make MVCC GC queue to
	// process GC immediately.
	_, err = sqlDB.Exec(`SET CLUSTER SETTING kv.protectedts.poll_interval = '1s';`)
	require.NoError(t, err)

	checkTableGCed := func(i int) {
		testutils.SucceedsSoon(t, func() error {
			if err := descExists(sqlDB, false, descs[i].GetID()); err != nil {
				return err
			}
			return zoneExists(sqlDB, nil, descs[i].GetID())
		})

		tableSpan := descs[i].TableSpan(keys.SystemSQLCodec)
		tests.CheckKeyCountIncludingTombstoned(t, s, tableSpan, 0)

		// Ensure that the job is marked as succeeded.
		if err := jobutils.VerifySystemJob(t, sqlRun, numTables+i,
			jobspb.TypeNewSchemaChange, jobs.StatusSucceeded, jobs.Record{
				Username:    username.RootUserName(),
				Description: fmt.Sprintf(`DROP TABLE t.public.%s`, descs[i].GetName()),
			}); err != nil {
			t.Fatal(err)
		}

		// Ensure that the gc job is marked as succeeded.
		testutils.SucceedsSoon(t, func() error {
			return jobutils.VerifySystemJob(t, sqlRun, numTables+i,
				jobspb.TypeSchemaChangeGC, jobs.StatusSucceeded, jobs.Record{
					Username:    username.NodeUserName(),
					Description: fmt.Sprintf(`GC for DROP TABLE t.public.%s`, descs[i].GetName()),
					DescriptorIDs: descpb.IDs{
						descs[i].GetID(),
					},
				})
		})
	}

	for i := 0; i < numTables; i++ {
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

	params, _ := tests.CreateTestServerParams()
	params.ScanMaxIdleTime = time.Millisecond
	s, sqlDBRaw, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	sqlDB.Exec(t, `SET CLUSTER SETTING sql.gc_job.wait_for_gc.interval = '1s';`)

	// Refresh protected timestamp cache immediately to make MVCC GC queue to
	// process GC immediately.
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.protectedts.poll_interval = '1s';`)

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
	tests.CheckKeyCountIncludingTombstoned(t, s, tableSpan, numRows)

	sqlDB.Exec(t, `DROP TABLE test.t`)

	// Simulate a migration upgrading the table descriptor's format version after
	// the table has been dropped but before the truncation has occurred.
	if err := sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
		tbl, err := col.ByID(txn.KV()).Get().Table(ctx, tableDesc.ID)
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
	tests.CheckKeyCountIncludingTombstoned(t, s, tableSpan, 0)
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
					BeforeExecute: func(ctx context.Context, stmt string, descriptors *descs.Collection) {
						if strings.Contains(stmt, "DROP INDEX") {
							// Force release all cached descriptors to force a kv fetch with
							// the table ID so that we can guarantee the DynamicRequestFilter
							// would see such relevant request and inject the error we want at
							// the right time.
							descriptors.ReleaseAll(ctx)
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
	rf.setFilter(func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
		if request.Txn == nil || request.Txn.Name != sql.SQLTxnName {
			return nil
		}
		filterState.Lock()
		defer filterState.Unlock()
		if filterState.txnID != (uuid.UUID{}) {
			return nil
		}
		if scanRequest, ok := request.GetArg(kvpb.Scan); ok {
			scan := scanRequest.(*kvpb.ScanRequest)
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
	afterInsert, err := hlc.ParseHLC(afterInsertStr)
	require.NoError(t, err)

	// Now set up a filter to detect when the DROP INDEX execution will begin and
	// inject an error forcing a refresh above the conflicting write which will
	// fail. We'll want to ensure that we get a retriable error. Use the below
	// pattern to detect when the user transaction has finished planning and is
	// now executing: we don't want to inject the error during planning.
	rf.setFilter(func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
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
		if getRequest, ok := request.GetArg(kvpb.Get); ok {
			put := getRequest.(*kvpb.GetRequest)
			if put.Key.Equal(catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, descpb.ID(tableID))) {
				filterState.txnID = uuid.UUID{}
				return kvpb.NewError(kvpb.NewReadWithinUncertaintyIntervalError(
					request.Txn.ReadTimestamp, hlc.ClockTimestamp{}, request.Txn, afterInsert, hlc.ClockTimestamp{}))
			}
		}
		return nil
	})

	_, err = txn.Exec("DROP INDEX foo@j_idx")
	require.Truef(t, isRetryableErr(err), "drop index error: %v", err)
	require.NoError(t, txn.Rollback())
}

// TestDropIndexOnHashShardedIndexWithStoredShardColumn tests the case when attempt to drop a hash-sharded index with
// a stored shard column (all hash-sharded index created in 21.2 and prior will have a stored shard column) without
// cascade, we skip dropping the shard column.
func TestDropIndexOnHashShardedIndexWithStoredShardColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start a test server and connect to it with notice handler (so we can get and check notices from running queries).
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, _, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	url, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	base, err := pq.NewConnector(url.String())
	if err != nil {
		t.Fatal(err)
	}
	actualNotices := make([]string, 0)
	connector := pq.ConnectorWithNoticeHandler(base, func(n *pq.Error) {
		actualNotices = append(actualNotices, n.Message)
	})
	dbWithHandler := gosql.OpenDB(connector)
	defer dbWithHandler.Close()
	tdb := sqlutils.MakeSQLRunner(dbWithHandler)

	// Create a table with a stored column with the same name as the shard column so that the hash-sharded
	// index will just use that column. This is the trick we use to be able to create a hash-shard index with
	// a STORED column.
	query :=
		`
		CREATE TABLE tbl (
			a INT,
			crdb_internal_a_shard_7 INT NOT VISIBLE AS (mod(fnv32(crdb_internal.datums_to_bytes(a)), 7:::INT)) STORED,
			INDEX idx (a) USING HASH WITH BUCKET_COUNT=7
		)
		`
	tdb.Exec(t, query)

	// Assert that the table has two indexes and the shard index uses the stored column as its shard column.
	var tableID descpb.ID
	var tableDesc catalog.TableDescriptor
	query = `SELECT id FROM system.namespace WHERE name = 'tbl'`
	tdb.QueryRow(t, query).Scan(&tableID)
	require.NoError(t, sql.TestingDescsTxn(ctx, s,
		func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
			tableDesc, err = col.ByID(txn.KV()).Get().Table(ctx, tableID)
			return err
		}))
	shardIdx, err := catalog.MustFindIndexByName(tableDesc, "idx")
	require.NoError(t, err)
	require.True(t, shardIdx.IsSharded())
	require.Equal(t, "crdb_internal_a_shard_7", shardIdx.GetShardColumnName())
	shardCol, err := catalog.MustFindColumnByName(tableDesc, "crdb_internal_a_shard_7")
	require.NoError(t, err)
	require.False(t, shardCol.IsVirtual())

	// Drop the hash-sharded index.
	query = `DROP INDEX idx`
	tdb.Exec(t, query)

	// Assert that the index is dropped but the shard column remains after dropping the index.
	require.NoError(t, sql.TestingDescsTxn(ctx, s,
		func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
			tableDesc, err = col.ByID(txn.KV()).Get().Table(ctx, tableID)
			return err
		}))
	_, err = catalog.MustFindIndexByName(tableDesc, "idx")
	require.Error(t, err)
	shardCol, err = catalog.MustFindColumnByTreeName(tableDesc, "crdb_internal_a_shard_7")
	require.NoError(t, err)
	require.False(t, shardCol.IsVirtual())

	// Assert that we get the expected notice.
	expectedNotice := "The accompanying shard column \"crdb_internal_a_shard_7\" is a physical column and dropping it " +
		"can be expensive, so, we dropped the index \"idx\" but skipped dropping \"crdb_internal_a_shard_7\". Issue " +
		"another 'ALTER TABLE tbl DROP COLUMN crdb_internal_a_shard_7' query if you want to drop column" +
		" \"crdb_internal_a_shard_7\"."
	require.Contains(t, actualNotices, expectedNotice)
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

	_, err := sqlDB.Exec(`SET CLUSTER SETTING sql.gc_job.wait_for_gc.interval = '1s';`)
	require.NoError(t, err)

	// Refresh protected timestamp cache immediately to make MVCC GC queue to
	// process GC immediately.
	_, err = sqlDB.Exec(`SET CLUSTER SETTING kv.protectedts.poll_interval = '1s';`)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`
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
		[][]string{{`GC for DROP DATABASE t CASCADE`}},
	)

	// Check that the data was cleaned up.
	tests.CheckKeyCountIncludingTombstoned(t, s, parentDesc.TableSpan(keys.SystemSQLCodec), 0)
	tests.CheckKeyCountIncludingTombstoned(t, s, childDesc.TableSpan(keys.SystemSQLCodec), 0)
}

// Test that non-physical table deletions like DROP VIEW are immediate instead
// of via a GC job.
func TestDropPhysicalTableGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())
	_, err := sqlDB.Exec(`CREATE DATABASE test;`)
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

func dropLargeDatabaseGeneric(
	t testing.TB, workloadParams sqltestutils.GenerateViewBasedGraphSchemaParams, useDeclarative bool,
) {
	// Creates a complex schema with a view based graph that nests within
	// each other, which can lead to long DROP times specially if there
	// is anything takes quadratic time
	// (see sqltestutils.GenerateViewBasedGraphSchema).
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET descriptor_validation = read_only`)
	sqlDB.Exec(t, `CREATE DATABASE largedb`)
	stmts, err := sqltestutils.GenerateViewBasedGraphSchema(workloadParams)
	require.NoError(t, err)
	for _, stmt := range stmts {
		sqlDB.Exec(t, stmt.SQL)
	}
	if !useDeclarative {
		sqlDB.Exec(t, `SET use_declarative_schema_changer=off;`)
	}
	startTime := timeutil.Now()
	if b, isB := t.(*testing.B); isB {
		b.StartTimer()
		defer b.StopTimer()
	}
	sqlDB.Exec(t, `DROP DATABASE largedb;`)
	t.Logf("Total time for drop (declarative: %t) %f",
		useDeclarative,
		timeutil.Since(startTime).Seconds())
}

func TestDropLargeDatabaseWithLegacySchemaChanger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "drop can be longer than timeout under race testing")
	dropLargeDatabaseGeneric(t,
		sqltestutils.GenerateViewBasedGraphSchemaParams{
			SchemaName:         "largedb",
			NumTablesPerDepth:  4,
			NumColumnsPerTable: 3,
			GraphDepth:         3,
		},
		false)
}

// BenchmarkDropLargeDatabase adds a benchmark which runs a large database
// drop for a connected graph of views. It can be used to compare the
// legacy and declarative schema changer.
//
// TODO(ajwerner): The parameters to the generator are a little bit opaque.
// It'd be nice to have a sense of how many views and how many total columns
// we end up dropping.
func BenchmarkDropLargeDatabase(b *testing.B) {
	defer leaktest.AfterTest(b)()
	skip.UnderShort(b)

	for _, declarative := range []bool{false, true} {
		for _, tables := range []int{3, 4} {
			for _, depth := range []int{2, 3, 4, 5} {
				for _, columns := range []int{2, 4} {
					b.Run(fmt.Sprintf("tables=%d,columns=%d,depth=%d,declarative=%t",
						tables, columns, depth, declarative), func(b *testing.B) {
						defer log.Scope(b).Close(b)
						for i := 0; i < b.N; i++ {
							b.StopTimer()
							dropLargeDatabaseGeneric(b,
								sqltestutils.GenerateViewBasedGraphSchemaParams{
									SchemaName:         "largedb",
									NumTablesPerDepth:  tables,
									NumColumnsPerTable: columns,
									GraphDepth:         depth,
								},
								declarative)
						}
					})
				}
			}
		}
	}
}

func TestDropLargeDatabaseWithDeclarativeSchemaChanger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "drop can be longer than timeout under race testing")
	// TODO(fqazi): Increase graph complexity as we improve performance.
	dropLargeDatabaseGeneric(t,
		sqltestutils.GenerateViewBasedGraphSchemaParams{
			SchemaName:         "largedb",
			NumTablesPerDepth:  5,
			NumColumnsPerTable: 2,
			GraphDepth:         2,
		},
		true)
}
