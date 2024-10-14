// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

export enum TableColName {
  NAME = "Name",
  REPLICATION_SIZE = "Replication Size",
  RANGE_COUNT = "Ranges",
  COLUMN_COUNT = "Columns",
  INDEX_COUNT = "Indexes",
  NODE_REGIONS = "Regions / Nodes",
  LIVE_DATA_PERCENTAGE = "% of Live data",
  STATS_LAST_UPDATED = "Stats last updated",
  AUTO_STATS_ENABLED = "Table auto stats enabled",
}
