// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  DatabaseMetadata,
  DatabaseSortOptions,
} from "src/api/databases/getDatabaseMetadataApi";

import { DatabaseColName } from "./constants";
import { DatabaseRow } from "./databaseTypes";

export const getSortKeyFromColTitle = (
  col: DatabaseColName,
): DatabaseSortOptions => {
  switch (col) {
    case DatabaseColName.NAME:
      return DatabaseSortOptions.NAME;
    case DatabaseColName.SIZE:
      return DatabaseSortOptions.REPLICATION_SIZE;
    case DatabaseColName.RANGE_COUNT:
      return DatabaseSortOptions.RANGES;
    case DatabaseColName.TABLE_COUNT:
      return DatabaseSortOptions.LIVE_DATA;
    default:
      throw new Error(`Unsupported sort column ${col}`);
  }
};

export const getColTitleFromSortKey = (
  sortKey: DatabaseSortOptions,
): DatabaseColName => {
  switch (sortKey) {
    case DatabaseSortOptions.NAME:
      return DatabaseColName.NAME;
    case DatabaseSortOptions.REPLICATION_SIZE:
      return DatabaseColName.SIZE;
    case DatabaseSortOptions.RANGES:
      return DatabaseColName.RANGE_COUNT;
    case DatabaseSortOptions.LIVE_DATA:
      return DatabaseColName.TABLE_COUNT;
    default:
      return DatabaseColName.NAME;
  }
};

export const rawDatabaseMetadataToDatabaseRows = (
  raw: DatabaseMetadata[],
): DatabaseRow[] => {
  return raw.map(
    (db: DatabaseMetadata): DatabaseRow => ({
      name: db.db_name,
      id: db.db_id,
      tableCount: db.table_count,
      approximateDiskSizeBytes: db.size_bytes,
      rangeCount: db.table_count,
      nodesByRegion: {},
      schemaInsightsCount: 0,
      key: db.db_id.toString(),
    }),
  );
};
