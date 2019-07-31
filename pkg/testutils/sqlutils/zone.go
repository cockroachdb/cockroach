// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// ZoneRow represents a row returned by SHOW ZONE CONFIGURATION.
type ZoneRow struct {
	ID     uint32
	Config config.ZoneConfig
}

func (row ZoneRow) sqlRowString() ([]string, error) {
	configProto, err := protoutil.Marshal(&row.Config)
	if err != nil {
		return nil, err
	}
	return []string{
		fmt.Sprintf("%d", row.ID),
		string(configProto),
	}, nil
}

// GetAllZoneSpecifiers returns specifiers for all installed zone configs.
func GetAllZoneSpecifiers(t testing.TB, sqlDB *SQLRunner) []tree.ZoneSpecifier {
	rows := sqlDB.Query(t, `
SELECT range_name, database_name, table_name, index_name, partition_name
FROM crdb_internal.zones`)
	defer rows.Close()

	var result []tree.ZoneSpecifier
	for rows.Next() {
		var rangeName, databaseName, tableName, indexName, partitionName gosql.NullString
		var zs tree.ZoneSpecifier
		err := rows.Scan(&rangeName, &databaseName, &tableName, &indexName, &partitionName)
		if err != nil {
			t.Fatal(err)
		}
		if rangeName.Valid {
			zs.NamedZone = tree.UnrestrictedName(rangeName.String)
		}
		if databaseName.Valid && !tableName.Valid {
			zs.Database = tree.Name(databaseName.String)
		}
		if tableName.Valid {
			zs.TableOrIndex.Table = *tree.NewTableName(
				tree.Name(databaseName.String), tree.Name(tableName.String))
		}
		if indexName.Valid {
			zs.TableOrIndex.Index = tree.UnrestrictedName(indexName.String)
		}
		if partitionName.Valid {
			zs.Partition = tree.Name(partitionName.String)
		}
		result = append(result, zs)
	}
	return result
}

// RemoveAllZoneConfigs removes all installed zone configs.
func RemoveAllZoneConfigs(t testing.TB, sqlDB *SQLRunner) {
	t.Helper()
	for _, zs := range GetAllZoneSpecifiers(t, sqlDB) {
		if zs.NamedZone == config.DefaultZoneName {
			// The default zone cannot be removed.
			continue
		}
		sqlDB.Exec(t, fmt.Sprintf("ALTER %s CONFIGURE ZONE DISCARD", &zs))
	}
}

// DeleteZoneConfig deletes the specified zone config through the SQL interface.
func DeleteZoneConfig(t testing.TB, sqlDB *SQLRunner, target string) {
	t.Helper()
	sqlDB.Exec(t, fmt.Sprintf("ALTER %s CONFIGURE ZONE DISCARD", target))
}

// SetZoneConfig updates the specified zone config through the SQL interface.
func SetZoneConfig(t testing.TB, sqlDB *SQLRunner, target string, config string) {
	t.Helper()
	sqlDB.Exec(t, fmt.Sprintf("ALTER %s CONFIGURE ZONE = %s",
		target, lex.EscapeSQLString(config)))
}

// TxnSetZoneConfig updates the specified zone config through the SQL interface
// using the provided transaction.
func TxnSetZoneConfig(t testing.TB, sqlDB *SQLRunner, txn *gosql.Tx, target string, config string) {
	t.Helper()
	_, err := txn.Exec(fmt.Sprintf("ALTER %s CONFIGURE ZONE = %s",
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
	sqlDB.CheckQueryResults(t, fmt.Sprintf(`
SELECT zone_id, config_protobuf
  FROM [SHOW ZONE CONFIGURATION FOR %s]`, target),
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
	sqlDB.CheckQueryResults(t, `
SELECT zone_id, config_protobuf
  FROM crdb_internal.zones
  WHERE zone_name IS NOT NULL`, expected)
}

// ZoneConfigExists returns whether a zone config with the provided name exists.
func ZoneConfigExists(t testing.TB, sqlDB *SQLRunner, name string) bool {
	t.Helper()
	var exists bool
	sqlDB.QueryRow(
		t, "SELECT EXISTS (SELECT 1 FROM crdb_internal.zones WHERE zone_name = $1)", name,
	).Scan(&exists)
	return exists
}
