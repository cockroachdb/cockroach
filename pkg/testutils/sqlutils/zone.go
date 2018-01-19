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

package sqlutils

import (
	gosql "database/sql"
	"fmt"
	"testing"

	yaml "gopkg.in/yaml.v2"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// ZoneRow represents a row returned by EXPERIMENTAL SHOW ZONE CONFIGURATION.
type ZoneRow struct {
	ID           uint32
	CLISpecifier string
	Config       config.ZoneConfig
}

func (row ZoneRow) sqlRowString() ([]string, error) {
	configProto, err := protoutil.Marshal(&row.Config)
	if err != nil {
		return nil, err
	}
	configYAML, err := yaml.Marshal(row.Config)
	if err != nil {
		return nil, err
	}
	return []string{
		fmt.Sprintf("%d", row.ID),
		row.CLISpecifier,
		string(configYAML),
		string(configProto),
	}, nil
}

// RemoveAllZoneConfigs removes all installed zone configs.
func RemoveAllZoneConfigs(t testing.TB, sqlDB *SQLRunner) {
	t.Helper()
	for _, zone := range sqlDB.QueryStr(t, "SELECT cli_specifier FROM crdb_internal.zones") {
		zs, err := config.ParseCLIZoneSpecifier(zone[0])
		if err != nil {
			t.Fatal(err)
		}
		if zs.NamedZone == config.DefaultZoneName {
			// The default zone cannot be removed.
			continue
		}
		sqlDB.Exec(t, fmt.Sprintf("ALTER %s EXPERIMENTAL CONFIGURE ZONE NULL", &zs))
	}
}

// DeleteZoneConfig deletes the specified zone config through the SQL interface.
func DeleteZoneConfig(t testing.TB, sqlDB *SQLRunner, target string) {
	t.Helper()
	sqlDB.Exec(t, fmt.Sprintf("ALTER %s EXPERIMENTAL CONFIGURE ZONE NULL", target))
}

// SetZoneConfig updates the specified zone config through the SQL interface.
func SetZoneConfig(t testing.TB, sqlDB *SQLRunner, target string, config string) {
	t.Helper()
	sqlDB.Exec(t, fmt.Sprintf("ALTER %s EXPERIMENTAL CONFIGURE ZONE %s",
		target, lex.EscapeSQLString(config)))
}

// TxnSetZoneConfig updates the specified zone config through the SQL interface
// using the provided transaction.
func TxnSetZoneConfig(t testing.TB, sqlDB *SQLRunner, txn *gosql.Tx, target string, config string) {
	t.Helper()
	_, err := txn.Exec(fmt.Sprintf("ALTER %s EXPERIMENTAL CONFIGURE ZONE %s",
		target, lex.EscapeSQLString(config)))
	if err != nil {
		t.Fatal(err)
	}
}

// VerifyZoneConfigForTarget verifies that the specified zone matches the specified
// ZoneRow.
func VerifyZoneConfigForTarget(t testing.TB, sqlDB *SQLRunner, target string, row ZoneRow) {
	t.Helper()
	sqlRow, err := row.sqlRowString()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.CheckQueryResults(t, fmt.Sprintf("EXPERIMENTAL SHOW ZONE CONFIGURATION FOR %s", target),
		[][]string{sqlRow})
}

// VerifyAllZoneConfigs verifies that the specified ZoneRows exactly match the
// list of active zone configs.
func VerifyAllZoneConfigs(t testing.TB, sqlDB *SQLRunner, rows ...ZoneRow) {
	t.Helper()
	expected := make([][]string, len(rows))
	for i, row := range rows {
		var err error
		expected[i], err = row.sqlRowString()
		if err != nil {
			t.Fatal(err)
		}
	}
	sqlDB.CheckQueryResults(t, "EXPERIMENTAL SHOW ALL ZONE CONFIGURATIONS", expected)
}

// ZoneConfigExists returns whether a zone config with the provided cliSpecifier
// exists.
func ZoneConfigExists(t testing.TB, sqlDB *SQLRunner, cliSpecifier string) bool {
	t.Helper()
	var exists bool
	sqlDB.QueryRow(
		t, "SELECT EXISTS (SELECT 1 FROM crdb_internal.zones WHERE cli_specifier = $1)", cliSpecifier,
	).Scan(&exists)
	return exists
}
