// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { DatabaseMetadata } from "src/api/databases/getDatabaseMetadataApi";

import { DatabaseRow } from "./databaseTypes";

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
