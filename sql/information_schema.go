// Copyright 2016 The Cockroach Authors.
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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package sql

import "github.com/cockroachdb/cockroach/sql/parser"

var informationSchema = virtualSchema{
	name: "information_schema",
	tables: []virtualSchemaTable{
		informationSchemaTablesTable,
	},
}

var informationSchemaTablesTable = virtualSchemaTable{
	schema: `
CREATE TABLE information_schema.tables (
  TABLE_CATALOG STRING NOT NULL DEFAULT '',
  TABLE_SCHEMA STRING NOT NULL DEFAULT '',
  TABLE_NAME STRING NOT NULL DEFAULT '',
  TABLE_TYPE STRING NOT NULL DEFAULT '',
  ENGINE STRING,
  VERSION INT,
  ROW_FORMAT STRING,
  TABLE_ROWS INT,
  AVG_ROW_LENGTH INT,
  DATA_LENGTH INT,
  MAX_DATA_LENGTH INT,
  INDEX_LENGTH INT,
  DATA_FREE INT,
  AUTO_INCREMENT INT,
  CREATE_TIME TIMESTAMP,
  UPDATE_TIME TIMESTAMP,
  CHECK_TIME TIMESTAMP,
  TABLE_COLLATION STRING,
  CHECKSUM INT,
  CREATE_OPTIONS STRING,
  TABLE_COMMENT STRING NOT NULL DEFAULT ''
);`,
	populate: func(p *planner, addRow func(...parser.Datum)) error {
		// TODO(nvanbenschoten) This isn't actually the correct implementation
		// for this table. Fixing this will come later.
		if p.session.Database == "" {
			return errNoDatabase
		}
		dbDesc, err := p.mustGetDatabaseDesc(p.session.Database)
		if err != nil {
			return err
		}

		tableNames, err := p.getTableNames(dbDesc)
		if err != nil {
			return err
		}
		for _, name := range tableNames {
			addRow(
				parser.DNull,
				parser.DNull,
				parser.NewDString(name.Table()),
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
				parser.DNull,
			)
		}
		return nil
	},
}
