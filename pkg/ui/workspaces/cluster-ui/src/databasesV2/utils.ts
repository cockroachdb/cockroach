// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { DatabaseMetadata } from "src/api/databases/getDatabaseMetadataApi";
import { NodeStatus } from "src/api/nodesApi";
import { NodeID, StoreID } from "src/types/clusterTypes";
import { mapStoreIDsToNodeRegions } from "src/util/nodeUtils";

import { DatabaseRow } from "./databaseTypes";

export const rawDatabaseMetadataToDatabaseRows = (
  raw: DatabaseMetadata[],
  nodesInfo: {
    nodeStatusByID: Record<NodeID, NodeStatus>;
    storeIDToNodeID: Record<StoreID, NodeID>;
    isLoading: boolean;
  },
): DatabaseRow[] => {
  return raw.map((db: DatabaseMetadata): DatabaseRow => {
    const nodesByRegion = mapStoreIDsToNodeRegions(
      db.storeIds,
      nodesInfo?.nodeStatusByID,
      nodesInfo?.storeIDToNodeID,
    );
    return {
      name: db.dbName,
      id: db.dbId,
      tableCount: db.tableCount ?? 0,
      approximateDiskSizeBytes: db.sizeBytes ?? 0,
      rangeCount: db.tableCount ?? 0,
      schemaInsightsCount: 0,
      key: db.dbId.toString(),
      nodesByRegion: {
        isLoading: nodesInfo.isLoading,
        data: nodesByRegion,
      },
    };
  });
};
