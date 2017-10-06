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
	gosql "database/sql"
	"fmt"
	"testing"

	"golang.org/x/net/context"
	yaml "gopkg.in/yaml.v2"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func TestValidSetShowZones(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	sqlDB := sqlutils.MakeSQLRunner(t, db)
	sqlDB.Exec(`CREATE DATABASE d; USE d; CREATE TABLE t ();`)

	type zoneRow struct {
		id           uint32
		cliSpecifier string
		config       config.ZoneConfig
	}

	rowToString := func(row zoneRow) []string {
		configProto, err := protoutil.Marshal(&row.config)
		if err != nil {
			t.Fatal(err)
		}
		configYAML, err := yaml.Marshal(row.config)
		if err != nil {
			t.Fatal(err)
		}
		return []string{
			fmt.Sprintf("%d", row.id),
			row.cliSpecifier,
			string(configYAML),
			string(configProto),
		}
	}

	deleteZoneConfig := func(target string) {
		t.Helper()
		sqlDB.Exec(fmt.Sprintf("ALTER %s EXPERIMENTAL CONFIGURE ZONE NULL", target))
	}

	setZoneConfig := func(target string, config string) {
		t.Helper()
		sqlDB.Exec(fmt.Sprintf("ALTER %s EXPERIMENTAL CONFIGURE ZONE %s",
			target, parser.EscapeSQLString(config)))
	}

	txnSetZoneConfig := func(txn *gosql.Tx, target string, config string) {
		t.Helper()
		_, err := txn.Exec(fmt.Sprintf("ALTER %s EXPERIMENTAL CONFIGURE ZONE %s",
			target, parser.EscapeSQLString(config)))
		if err != nil {
			t.Fatal(err)
		}
	}

	verifyZoneConfig := func(target string, row zoneRow) {
		t.Helper()
		sqlDB.CheckQueryResults(fmt.Sprintf("EXPERIMENTAL SHOW ZONE CONFIGURATION FOR %s", target),
			[][]string{rowToString(row)})
	}

	verifyZoneConfigs := func(rows ...zoneRow) {
		t.Helper()
		expected := make([][]string, len(rows))
		for i, row := range rows {
			expected[i] = rowToString(row)
		}
		sqlDB.CheckQueryResults("EXPERIMENTAL SHOW ALL ZONE CONFIGURATIONS", expected)
	}

	yamlDefault := fmt.Sprintf("gc: {ttlseconds: %d}", config.DefaultZoneConfig().GC.TTLSeconds)
	yamlOverride := "gc: {ttlseconds: 42}"
	zoneOverride := config.DefaultZoneConfig()
	zoneOverride.GC.TTLSeconds = 42

	defaultRow := zoneRow{keys.RootNamespaceID, ".default", config.DefaultZoneConfig()}
	defaultOverrideRow := zoneRow{keys.RootNamespaceID, ".default", zoneOverride}
	metaRow := zoneRow{keys.MetaRangesID, ".meta", zoneOverride}
	systemRow := zoneRow{keys.SystemDatabaseID, "system", zoneOverride}
	jobsRow := zoneRow{keys.JobsTableID, "system.jobs", zoneOverride}
	dbRow := zoneRow{keys.MaxReservedDescID + 1, "d", zoneOverride}
	tableRow := zoneRow{keys.MaxReservedDescID + 2, "d.t", zoneOverride}

	// Ensure the default is reported for all zones at first.
	verifyZoneConfigs(defaultRow)
	verifyZoneConfig("RANGE default", defaultRow)
	verifyZoneConfig("RANGE meta", defaultRow)
	verifyZoneConfig("DATABASE system", defaultRow)
	verifyZoneConfig("TABLE system.lease", defaultRow)
	verifyZoneConfig("DATABASE d", defaultRow)
	verifyZoneConfig("TABLE d.t", defaultRow)

	// Ensure a database zone config applies to that database and its tables, and
	// no other zones.
	setZoneConfig("DATABASE d", yamlOverride)
	verifyZoneConfigs(defaultRow, dbRow)
	verifyZoneConfig("RANGE meta", defaultRow)
	verifyZoneConfig("DATABASE system", defaultRow)
	verifyZoneConfig("TABLE system.lease", defaultRow)
	verifyZoneConfig("DATABASE d", dbRow)
	verifyZoneConfig("TABLE d.t", dbRow)

	// Ensure a table zone config applies to that table and no others.
	setZoneConfig("TABLE d.t", yamlOverride)
	verifyZoneConfigs(defaultRow, dbRow, tableRow)
	verifyZoneConfig("RANGE meta", defaultRow)
	verifyZoneConfig("DATABASE system", defaultRow)
	verifyZoneConfig("TABLE system.lease", defaultRow)
	verifyZoneConfig("DATABASE d", dbRow)
	verifyZoneConfig("TABLE d.t", tableRow)

	// Ensure a named zone config applies to that named zone and no others.
	setZoneConfig("RANGE meta", yamlOverride)
	verifyZoneConfigs(defaultRow, metaRow, dbRow, tableRow)
	verifyZoneConfig("RANGE meta", metaRow)
	verifyZoneConfig("DATABASE system", defaultRow)
	verifyZoneConfig("TABLE system.lease", defaultRow)
	verifyZoneConfig("DATABASE d", dbRow)
	verifyZoneConfig("TABLE d.t", tableRow)

	// Ensure updating the default zone propagates to zones without an override,
	// but not to those with overrides.
	setZoneConfig("RANGE default", yamlOverride)
	verifyZoneConfigs(defaultOverrideRow, metaRow, dbRow, tableRow)
	verifyZoneConfig("RANGE meta", metaRow)
	verifyZoneConfig("DATABASE system", defaultOverrideRow)
	verifyZoneConfig("TABLE system.lease", defaultOverrideRow)
	verifyZoneConfig("DATABASE d", dbRow)
	verifyZoneConfig("TABLE d.t", tableRow)

	// Ensure deleting a database deletes only the database zone, and not the
	// table zone.
	deleteZoneConfig("DATABASE d")
	verifyZoneConfigs(defaultOverrideRow, metaRow, tableRow)
	verifyZoneConfig("DATABASE d", defaultOverrideRow)
	verifyZoneConfig("TABLE d.t", tableRow)

	// Ensure deleting a table zone works.
	deleteZoneConfig("TABLE d.t")
	verifyZoneConfigs(defaultOverrideRow, metaRow)
	verifyZoneConfig("TABLE d.t", defaultOverrideRow)

	// Ensure deleting a named zone works.
	deleteZoneConfig("RANGE meta")
	verifyZoneConfigs(defaultOverrideRow)
	verifyZoneConfig("RANGE meta", defaultOverrideRow)

	// Ensure deleting non-overridden zones is not an error.
	deleteZoneConfig("RANGE meta")
	deleteZoneConfig("DATABASE d")
	deleteZoneConfig("TABLE d.t")

	// Ensure updating the default zone config applies to zones that have had
	// overrides added and removed.
	setZoneConfig("RANGE default", yamlDefault)
	verifyZoneConfigs(defaultRow)
	verifyZoneConfig("RANGE default", defaultRow)
	verifyZoneConfig("RANGE meta", defaultRow)
	verifyZoneConfig("DATABASE system", defaultRow)
	verifyZoneConfig("TABLE system.lease", defaultRow)
	verifyZoneConfig("DATABASE d", defaultRow)
	verifyZoneConfig("TABLE d.t", defaultRow)

	// Ensure the system database zone can be configured, even though zones on
	// config tables are disallowed.
	setZoneConfig("DATABASE system", yamlOverride)
	verifyZoneConfigs(defaultRow, systemRow)
	verifyZoneConfig("DATABASE system", systemRow)
	verifyZoneConfig("TABLE system.namespace", systemRow)
	verifyZoneConfig("TABLE system.jobs", systemRow)

	// Ensure zones for non-config tables in the system database can be
	// configured.
	setZoneConfig("TABLE system.jobs", yamlOverride)
	verifyZoneConfigs(defaultRow, systemRow, jobsRow)
	verifyZoneConfig("TABLE system.jobs", jobsRow)

	// Ensure zone configs are read transactionally instead of from the cached
	// system config.
	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	txnSetZoneConfig(txn, "RANGE default", yamlOverride)
	txnSetZoneConfig(txn, "TABLE d.t", "") // this should pick up the overridden default config
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	verifyZoneConfig("TABLE d.t", tableRow)
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
