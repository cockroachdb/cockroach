// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { DatabaseMetadata } from "src/api/databases/getDatabaseMetadataApi";
import { NodeID, StoreID } from "src/types/clusterTypes";

import { DatabaseRow } from "./databaseTypes";

export const rawDatabaseMetadataToDatabaseRows = (
  raw: DatabaseMetadata[],
  nodesInfo: {
    nodeIDToRegion: Record<NodeID, string>;
    storeIDToNodeID: Record<StoreID, NodeID>;
    isLoading: boolean;
  },
): DatabaseRow[] => {
  return raw.map((db: DatabaseMetadata): DatabaseRow => {
    const nodesByRegion: Record<string, NodeID[]> = {};
    if (!nodesInfo.isLoading) {
      db.storeIds?.forEach(storeID => {
        const nodeID = nodesInfo.storeIDToNodeID[storeID as StoreID];
        const region = nodesInfo.nodeIDToRegion[nodeID];
        if (!nodesByRegion[region]) {
          nodesByRegion[region] = [];
        }
        nodesByRegion[region].push(nodeID);
      });
    }
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
