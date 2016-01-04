// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage

import (
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/privilege"
)

// TODO(mrtracy) rangeEventTableSchema defines the schema for the range event
// schema.  Currently there is no useful information in this table; this is a
// work in progress.
const rangeEventTableSchema = `
CREATE TABLE rangelog.event (
  timestamp     TIMESTAMP  NOT NULL,
  rangeID       INT        NOT NULL,
  PRIMARY KEY (timestamp, rangeID)
);`

// AddEventLogToMetadataSchema adds the range event log table to the supplied
// MetadataSchema.
func AddEventLogToMetadataSchema(schema *sql.MetadataSchema) {
	allPrivileges := sql.NewPrivilegeDescriptor(security.RootUser, privilege.List{privilege.ALL})
	db := sql.MakeMetadataDatabase("rangelog", allPrivileges)
	db.AddTable(rangeEventTableSchema, allPrivileges)
	schema.AddDatabase(db)
}
