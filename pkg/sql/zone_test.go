// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestValidSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE DATABASE d; USE d; CREATE TABLE t ();`)

	yamlDefault := fmt.Sprintf("gc: {ttlseconds: %d}", config.DefaultZoneConfig().GC.TTLSeconds)
	yamlOverride := "gc: {ttlseconds: 42}"
	zoneOverride := config.DefaultZoneConfig()
	zoneOverride.GC.TTLSeconds = 42

	defaultRow := sqlutils.ZoneRow{
		ID:           keys.RootNamespaceID,
		CLISpecifier: ".default",
		Config:       config.DefaultZoneConfig(),
	}
	defaultOverrideRow := sqlutils.ZoneRow{
		ID:           keys.RootNamespaceID,
		CLISpecifier: ".default",
		Config:       zoneOverride,
	}
	metaRow := sqlutils.ZoneRow{
		ID:           keys.MetaRangesID,
		CLISpecifier: ".meta",
		Config:       zoneOverride,
	}
	systemRow := sqlutils.ZoneRow{
		ID:           keys.SystemDatabaseID,
		CLISpecifier: "system",
		Config:       zoneOverride,
	}
	jobsRow := sqlutils.ZoneRow{
		ID:           keys.JobsTableID,
		CLISpecifier: "system.jobs",
		Config:       zoneOverride,
	}
	dbRow := sqlutils.ZoneRow{
		ID:           keys.MaxReservedDescID + 1,
		CLISpecifier: "d",
		Config:       zoneOverride,
	}
	tableRow := sqlutils.ZoneRow{
		ID:           keys.MaxReservedDescID + 2,
		CLISpecifier: "d.t",
		Config:       zoneOverride,
	}
	tableDroppedRow := sqlutils.ZoneRow{
		ID:           keys.MaxReservedDescID + 2,
		CLISpecifier: "NULL",
		Config:       zoneOverride,
	}

	// Remove stock zone configs installed at cluster bootstrap. Otherwise this
	// test breaks whenever these stock zone configs are adjusted.
	sqlutils.RemoveAllZoneConfigs(t, sqlDB)

	// Ensure the default is reported for all zones at first.
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.public.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.public.t", defaultRow)

	// Ensure a database zone config applies to that database and its tables, and
	// no other zones.
	sqlutils.SetZoneConfig(t, sqlDB, "DATABASE d", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.public.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.public.t", dbRow)

	// Ensure a table zone config applies to that table and no others.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE d.public.t", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, dbRow, tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.public.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.public.t", tableRow)

	// Ensure a named zone config applies to that named zone and no others.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE meta", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, metaRow, dbRow, tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", metaRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.public.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.public.t", tableRow)

	// Ensure updating the default zone propagates to zones without an override,
	// but not to those with overrides.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, metaRow, dbRow, tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", metaRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.public.lease", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.public.t", tableRow)

	// Ensure deleting a database deletes only the database zone, and not the
	// table zone.
	sqlutils.DeleteZoneConfig(t, sqlDB, "DATABASE d")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, metaRow, tableRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.public.t", tableRow)

	// Ensure deleting a table zone works.
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.public.t")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, metaRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.public.t", defaultOverrideRow)

	// Ensure deleting a named zone works.
	sqlutils.DeleteZoneConfig(t, sqlDB, "RANGE meta")
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", defaultOverrideRow)

	// Ensure deleting non-overridden zones is not an error.
	sqlutils.DeleteZoneConfig(t, sqlDB, "RANGE meta")
	sqlutils.DeleteZoneConfig(t, sqlDB, "DATABASE d")
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE d.public.t")

	// Ensure updating the default zone config applies to zones that have had
	// overrides added and removed.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE default", yamlDefault)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.public.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.public.t", defaultRow)

	// Ensure the system database zone can be configured, even though zones on
	// config tables are disallowed.
	sqlutils.SetZoneConfig(t, sqlDB, "DATABASE system", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, systemRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "DATABASE system", systemRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.public.namespace", systemRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.public.jobs", systemRow)

	// Ensure zones for non-config tables in the system database can be
	// configured.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE system.public.jobs", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultRow, systemRow, jobsRow)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE system.public.jobs", jobsRow)

	// Verify that the session database is respected.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE t", yamlOverride)
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE t", tableRow)
	sqlutils.DeleteZoneConfig(t, sqlDB, "TABLE t")
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE t", defaultRow)

	// Ensure zone configs are read transactionally instead of from the cached
	// system config.
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	sqlutils.TxnSetZoneConfig(t, sqlDB, txn, "RANGE default", yamlOverride)
	sqlutils.TxnSetZoneConfig(t, sqlDB, txn, "TABLE d.public.t", "") // this should pick up the overridden default config
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	sqlutils.VerifyZoneConfigForTarget(t, sqlDB, "TABLE d.public.t", tableRow)

	sqlDB.Exec(t, "DROP TABLE d.public.t")
	_, err = sqlDB.DB.Exec("EXPERIMENTAL SHOW ZONE CONFIGURATION FOR TABLE d.public.t")
	if !testutils.IsError(err, `relation "d.public.t" does not exist`) {
		t.Errorf("expected SHOW ZONE CONFIGURATION to fail on dropped table, but got %q", err)
	}
	sqlutils.VerifyAllZoneConfigs(t, sqlDB, defaultOverrideRow, systemRow, jobsRow, tableDroppedRow)
}

func TestInvalidSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	for i, tc := range []struct {
		query string
		err   string
	}{
		{
			"ALTER RANGE default EXPERIMENTAL CONFIGURE ZONE NULL",
			"cannot remove default zone",
		},
		{
			"ALTER RANGE default EXPERIMENTAL CONFIGURE ZONE '&!@*@&'",
			"could not parse zone config",
		},
		{
			"ALTER TABLE system.public.namespace EXPERIMENTAL CONFIGURE ZONE ''",
			"cannot set zone configs for system config tables",
		},
		{
			"ALTER RANGE foo EXPERIMENTAL CONFIGURE ZONE ''",
			`"foo" is not a built-in zone`,
		},
		{
			"ALTER DATABASE foo EXPERIMENTAL CONFIGURE ZONE ''",
			`database "foo" does not exist`,
		},
		{
			"ALTER TABLE system.public.foo EXPERIMENTAL CONFIGURE ZONE ''",
			`relation "system.public.foo" does not exist`,
		},
		{
			"ALTER TABLE foo EXPERIMENTAL CONFIGURE ZONE ''",
			`no database specified: "foo"`,
		},
		{
			"EXPERIMENTAL SHOW ZONE CONFIGURATION FOR RANGE foo",
			`"foo" is not a built-in zone`,
		},
		{
			"EXPERIMENTAL SHOW ZONE CONFIGURATION FOR DATABASE foo",
			`database "foo" does not exist`,
		},
		{
			"EXPERIMENTAL SHOW ZONE CONFIGURATION FOR TABLE foo",
			`no database specified: "foo"`,
		},
		{
			"EXPERIMENTAL SHOW ZONE CONFIGURATION FOR TABLE system.public.foo",
			`relation "system.public.foo" does not exist`,
		},
	} {
		if _, err := db.Exec(tc.query); !testutils.IsError(err, tc.err) {
			t.Errorf("#%d: expected error matching %q, but got %v", i, tc.err, err)
		}
	}
}
