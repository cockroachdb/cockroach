// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instancestorage

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
)

// GetTableSQLForDatabase is a testing helper to construct the appropriate
// table schema for a given database taking into consideration whether we're
// configured for the MR schema.
func GetTableSQLForDatabase(dbName string) string {
	return strings.Replace(systemschema.SQLInstancesTableSchema,
		`CREATE TABLE system.sql_instances`,
		`CREATE TABLE "`+dbName+`".sql_instances`, 1)
}
