// Copyright 2017 The Cockroach Authors.
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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/gogo/protobuf/proto"
)

func TestValidSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE d; USE d; CREATE TABLE t ();`)

	yamlDefault := fmt.Sprintf("gc: {ttlseconds: %d}", s.(*server.TestServer).Cfg.DefaultZoneConfig.GC.TTLSeconds)
	yamlOverride := "gc: {ttlseconds: 42}"
	zoneOverride := s.(*server.TestServer).Cfg.DefaultZoneConfig
	zoneOverride.GC = &zonepb.GCPolicy{TTLSeconds: 42}
	partialZoneOverride := *zonepb.NewZoneConfig()
	partialZoneOverride.GC = &zonepb.GCPolicy{TTLSeconds: 42}

	defaultRow := sqlutils.ZoneRow{
		ID:     keys.RootNamespaceID,
		Config: s.(*server.TestServer).Cfg.DefaultZoneConfig,
	}
	defaultOverrideRow := sqlutils.ZoneRow{
		ID:     keys.RootNamespaceID,
		Config: zoneOverride,
	}
	metaRow := sqlutils.ZoneRow{
		ID:     keys.MetaRangesID,
		Config: zoneOverride,
	}
	systemRow := sqlutils.ZoneRow{
		ID:     keys.SystemDatabaseID,
		Config: zoneOverride,
	}
	jobsRow := sqlutils.ZoneRow{
		ID:     keys.JobsTableID,
		Config: zoneOverride,
	}

	dbID := sqlutils.QueryDatabaseID(t, db, "d")
	tableID := sqlutils.QueryTableID(t, db, "d", "public", "t")

	dbRow := sqlutils.ZoneRow{
		ID:     dbID,
		Config: zoneOverride,
	}
	tableRow := sqlutils.ZoneRow{
		ID:     tableID,
		Config: zoneOverride,
	}

	// Partially filled config rows
	partialMetaRow := sqlutils.ZoneRow{
		ID:     keys.MetaRangesID,
		Config: partialZoneOverride,
	}
	partialSystemRow := sqlutils.ZoneRow{
		ID:     keys.SystemDatabaseID,
		Config: partialZoneOverride,
	}
	partialJobsRow := sqlutils.ZoneRow{
		ID:     keys.JobsTableID,
		Config: partialZoneOverride,
	}
	partialDbRow := sqlutils.ZoneRow{
		ID:     dbID,
		Config: partialZoneOverride,
	}
	partialTableRow := sqlutils.ZoneRow{
		ID:     tableID,
		Config: partialZoneOverride,
	}

	// Remove stock zone configs installed at cluster bootstrap. Otherwise this
	// test breaks whenever these stock zone configs are adjusted.
	sqlutils.RemoveAllZoneConfigs(t, sqlDB)

	// Ensure the default is reported for all zones at first.
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)

	// Ensure a database zone config applies to that database and its tables, and
	// no other zones.
	sqlutils.SetZoneConfig(t, sqlDB, "DATABASE d", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", dbRow)

	// Ensure a table zone config applies to that table and no others.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE d.t", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialDbRow, partialTableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)

	// Ensure a named zone config applies to that named zone and no others.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE meta", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialMetaRow, partialDbRow, partialTableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", metaRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)

	// Ensure updating the default zone propagates to zones without an override,
	// but not to those with overrides.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialMetaRow, partialDbRow, partialTableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", metaRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.lease", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)

	// Ensure deleting a database deletes only the database zone, and not the
	// table zone.
	sqlutils.DeleteZoneConfig(t, sqlDB, "DATABASE d")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialMetaRow, partialTableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)

	// Ensure deleting a table zone works.
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.t")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialMetaRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultOverrideRow)

	// Ensure deleting a named zone works.
	sqlutils.DeleteZoneConfig(t, sqlDB, "RANGE meta")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", defaultOverrideRow)

	// Ensure deleting non-overridden zones is not an error.
	sqlutils.DeleteZoneConfig(t, sqlDB, "RANGE meta")
	sqlutils.DeleteZoneConfig(t, sqlDB, "DATABASE d")
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.t")

	// Ensure updating the default zone config applies to zones that have had
	// overrides added and removed.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", yamlDefault)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", defaultRow)

	// Ensure the system database zone can be configured, even though zones on
	// config tables are disallowed.
	sqlutils.SetZoneConfig(t, sqlDB, "DATABASE system", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialSystemRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", systemRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.namespace", systemRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.jobs", systemRow)

	// Ensure zones for non-config tables in the system database can be
	// configured.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE system.jobs", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, partialSystemRow, partialJobsRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.jobs", jobsRow)

	// Verify that the session database is respected.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE t", yamlOverride)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE t", tableRow)
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE t")
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE t", defaultRow)

	// Verify we can use composite values.
	sqlDB.Exec(t, fmt.Sprintf("ALTER TABLE t CONFIGURE ZONE = '' || %s || ''",
		lex.EscapeSQLString(yamlOverride)))
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE t", tableRow)

	// Ensure zone configs are read transactionally instead of from the cached
	// system config.
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	sqlutils.TxnSetZoneConfig(t, sqlDB, txn, "RANGE default", yamlOverride)
	sqlutils.TxnSetZoneConfig(t, sqlDB, txn, "TABLE d.t", "") // this should pick up the overridden default config
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.t", tableRow)

	sqlDB.Exec(t, "DROP TABLE d.t")
	_, err = db.Exec("SHOW ZONE CONFIGURATION FOR TABLE d.t")
	if !testutils.IsError(err, `relation "d.t" does not exist`) {
		t.Errorf("expected SHOW ZONE CONFIGURATION to fail on dropped table, but got %q", err)
	}
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, partialSystemRow, partialJobsRow)
}

func TestZoneInheritField(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlutils.RemoveAllZoneConfigs(t, sqlDB)
	sqlDB.Exec(t, `CREATE DATABASE d; USE d; CREATE TABLE t ();`)

	defaultRow := sqlutils.ZoneRow{
		ID:     keys.RootNamespaceID,
		Config: s.(*server.TestServer).Cfg.DefaultZoneConfig,
	}

	newReplicationFactor := 10
	tableID := sqlutils.QueryTableID(t, db, "d", "public", "t")
	newDefCfg := s.(*server.TestServer).Cfg.DefaultZoneConfig
	newDefCfg.NumReplicas = proto.Int32(int32(newReplicationFactor))

	newDefaultRow := sqlutils.ZoneRow{
		ID:     keys.RootNamespaceID,
		Config: newDefCfg,
	}

	newTableRow := sqlutils.ZoneRow{
		ID:     tableID,
		Config: s.(*server.TestServer).Cfg.DefaultZoneConfig,
	}

	// Doesn't have any values of its own.
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE t", defaultRow)

	// Solidify the num replicas value.
	sqlDB.Exec(t, `ALTER TABLE t CONFIGURE ZONE USING num_replicas = COPY FROM PARENT`)

	// Change the default replication factor.
	sqlDB.Exec(t, fmt.Sprintf("ALTER RANGE default CONFIGURE ZONE USING num_replicas = %d",
		newReplicationFactor))
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", newDefaultRow)

	// Verify the table didn't take on the new value for the replication factor.
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE t", newTableRow)
}

func TestInvalidSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	for i, tc := range []struct {
		query string
		err   string
	}{
		{
			"ALTER RANGE default CONFIGURE ZONE DISCARD",
			"cannot remove default zone",
		},
		{
			"ALTER RANGE default CONFIGURE ZONE = '&!@*@&'",
			"could not parse zone config",
		},
		{
			"ALTER TABLE system.namespace CONFIGURE ZONE USING DEFAULT",
			"cannot set zone configs for system config tables",
		},
		{
			"ALTER RANGE foo CONFIGURE ZONE USING DEFAULT",
			`"foo" is not a built-in zone`,
		},
		{
			"ALTER DATABASE foo CONFIGURE ZONE USING DEFAULT",
			`database "foo" does not exist`,
		},
		{
			"ALTER TABLE system.foo CONFIGURE ZONE USING DEFAULT",
			`relation "system.foo" does not exist`,
		},
		{
			"ALTER TABLE foo CONFIGURE ZONE USING DEFAULT",
			`relation "foo" does not exist`,
		},
		{
			"SHOW ZONE CONFIGURATION FOR RANGE foo",
			`"foo" is not a built-in zone`,
		},
		{
			"SHOW ZONE CONFIGURATION FOR DATABASE foo",
			`database "foo" does not exist`,
		},
		{
			"SHOW ZONE CONFIGURATION FOR TABLE foo",
			`relation "foo" does not exist`,
		},
		{
			"SHOW ZONE CONFIGURATION FOR TABLE system.foo",
			`relation "system.foo" does not exist`,
		},
	} {
		if _, err := db.Exec(tc.query); !testutils.IsError(err, tc.err) {
			t.Errorf("#%d: expected error matching %q, but got %v", i, tc.err, err)
		}
	}
}
