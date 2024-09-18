// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

export enum TableColName {
  NAME = "Name",
  REPLICATION_SIZE = "Replication Size",
  RANGE_COUNT = "Ranges",
  COLUMN_COUNT = "Columns",
  NODE_REGIONS = "Regions / Nodes",
  LIVE_DATA_PERCENTAGE = "% of Live data",
  AUTO_STATS_COLLECTION = "Table auto stats collection",
  STATS_LAST_UPDATED = "Stats last updated",
}
