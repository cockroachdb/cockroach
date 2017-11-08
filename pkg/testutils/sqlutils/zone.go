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

// DeleteZoneConfig deletes the specified zone config through the SQL interface.
func DeleteZoneConfig(sqlDB *SQLRunner, target string) {
	sqlDB.Helper()
	sqlDB.Exec(fmt.Sprintf("ALTER %s EXPERIMENTAL CONFIGURE ZONE NULL", target))
}

// SetZoneConfig updates the specified zone config through the SQL interface.
func SetZoneConfig(sqlDB *SQLRunner, target string, config string) {
	sqlDB.Helper()
	sqlDB.Exec(fmt.Sprintf("ALTER %s EXPERIMENTAL CONFIGURE ZONE %s",
		target, lex.EscapeSQLString(config)))
}

// TxnSetZoneConfig updates the specified zone config through the SQL interface
// using the provided transaction.
func TxnSetZoneConfig(sqlDB *SQLRunner, txn *gosql.Tx, target string, config string) {
	sqlDB.Helper()
	_, err := txn.Exec(fmt.Sprintf("ALTER %s EXPERIMENTAL CONFIGURE ZONE %s",
		target, lex.EscapeSQLString(config)))
	if err != nil {
		sqlDB.Fatal(err)
	}
}

// VerifyZoneConfigForTarget verifies that the specified zone matches the specified
// ZoneRow.
func VerifyZoneConfigForTarget(sqlDB *SQLRunner, target string, row ZoneRow) {
	sqlDB.Helper()
	sqlRow, err := row.sqlRowString()
	if err != nil {
		sqlDB.Fatal(err)
	}
	sqlDB.CheckQueryResults(fmt.Sprintf("EXPERIMENTAL SHOW ZONE CONFIGURATION FOR %s", target),
		[][]string{sqlRow})
}

// VerifyAllZoneConfigs verifies that the specified ZoneRows exactly match the
// list of active zone configs.
func VerifyAllZoneConfigs(sqlDB *SQLRunner, rows ...ZoneRow) {
	sqlDB.Helper()
	expected := make([][]string, len(rows))
	for i, row := range rows {
		var err error
		expected[i], err = row.sqlRowString()
		if err != nil {
			sqlDB.Fatal(err)
		}
	}
	sqlDB.CheckQueryResults("EXPERIMENTAL SHOW ALL ZONE CONFIGURATIONS", expected)
}
