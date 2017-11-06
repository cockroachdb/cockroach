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
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestValidSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	sqlDB := sqlutils.MakeSQLRunner(t, db)
	sqlDB.Exec(`CREATE DATABASE d; USE d; CREATE TABLE t ();`)

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

	// Ensure the default is reported for all zones at first.
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE system.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", defaultRow)

	// Ensure a database zone config applies to that database and its tables, and
	// no other zones.
	sqlutils.SetZoneConfig(sqlDB, "DATABASE d", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow, dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE system.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", dbRow)

	// Ensure a table zone config applies to that table and no others.
	sqlutils.SetZoneConfig(sqlDB, "TABLE d.t", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow, dbRow, tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE system.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", tableRow)

	// Ensure a named zone config applies to that named zone and no others.
	sqlutils.SetZoneConfig(sqlDB, "RANGE meta", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow, metaRow, dbRow, tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "RANGE meta", metaRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE system.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", tableRow)

	// Ensure updating the default zone propagates to zones without an override,
	// but not to those with overrides.
	sqlutils.SetZoneConfig(sqlDB, "RANGE default", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultOverrideRow, metaRow, dbRow, tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "RANGE meta", metaRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE system", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE system.lease", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", dbRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", tableRow)

	// Ensure deleting a database deletes only the database zone, and not the
	// table zone.
	sqlutils.DeleteZoneConfig(sqlDB, "DATABASE d")
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultOverrideRow, metaRow, tableRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", tableRow)

	// Ensure deleting a table zone works.
	sqlutils.DeleteZoneConfig(sqlDB, "TABLE d.t")
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultOverrideRow, metaRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", defaultOverrideRow)

	// Ensure deleting a named zone works.
	sqlutils.DeleteZoneConfig(sqlDB, "RANGE meta")
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultOverrideRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "RANGE meta", defaultOverrideRow)

	// Ensure deleting non-overridden zones is not an error.
	sqlutils.DeleteZoneConfig(sqlDB, "RANGE meta")
	sqlutils.DeleteZoneConfig(sqlDB, "DATABASE d")
	sqlutils.DeleteZoneConfig(sqlDB, "TABLE d.t")

	// Ensure updating the default zone config applies to zones that have had
	// overrides added and removed.
	sqlutils.SetZoneConfig(sqlDB, "RANGE default", yamlDefault)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "RANGE default", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "RANGE meta", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE system", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE system.lease", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE d", defaultRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", defaultRow)

	// Ensure the system database zone can be configured, even though zones on
	// config tables are disallowed.
	sqlutils.SetZoneConfig(sqlDB, "DATABASE system", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow, systemRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "DATABASE system", systemRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE system.namespace", systemRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE system.jobs", systemRow)

	// Ensure zones for non-config tables in the system database can be
	// configured.
	sqlutils.SetZoneConfig(sqlDB, "TABLE system.jobs", yamlOverride)
	sqlutils.VerifyAllZoneConfigs(sqlDB, defaultRow, systemRow, jobsRow)
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE system.jobs", jobsRow)

	// Ensure zone configs are read transactionally instead of from the cached
	// system config.
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	sqlutils.TxnSetZoneConfig(sqlDB, txn, "RANGE default", yamlOverride)
	sqlutils.TxnSetZoneConfig(sqlDB, txn, "TABLE d.t", "") // this should pick up the overridden default config
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	sqlutils.VerifyZoneConfigForTarget(sqlDB, "TABLE d.t", tableRow)
}

func TestInvalidSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
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
			"ALTER TABLE system.namespace EXPERIMENTAL CONFIGURE ZONE ''",
			"cannot set zone configs for system config tables",
		},
		{
			"ALTER RANGE foo EXPERIMENTAL CONFIGURE ZONE ''",
			"\"foo\" is not a built-in zone",
		},
		{
			"ALTER DATABASE foo EXPERIMENTAL CONFIGURE ZONE ''",
			"database \"foo\" does not exist",
		},
		{
			"ALTER TABLE foo EXPERIMENTAL CONFIGURE ZONE ''",
			"relation \"foo\" does not exist",
		},
		{
			"EXPERIMENTAL SHOW ZONE CONFIGURATION FOR RANGE foo",
			"\"foo\" is not a built-in zone",
		},
		{
			"EXPERIMENTAL SHOW ZONE CONFIGURATION FOR DATABASE foo",
			"database \"foo\" does not exist",
		},
		{
			"EXPERIMENTAL SHOW ZONE CONFIGURATION FOR TABLE foo",
			"relation \"foo\" does not exist",
		},
	} {
		if _, err := db.Exec(tc.query); !testutils.IsError(err, tc.err) {
			t.Errorf("#%d: expected error matching %q, but got %v", i, tc.err, err)
		}
	}
}
