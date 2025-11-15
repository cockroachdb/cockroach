// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vtable

// IndexUsageStatistics describes the schema of the internal index_usage_statistics table.
const CRDBIndexUsageStatistics = `
CREATE TABLE information_schema.crdb_index_usage_statistics (
  table_id        INT NOT NULL,
  index_id        INT NOT NULL,
  total_reads     INT NOT NULL,
  last_read       TIMESTAMPTZ
)`
