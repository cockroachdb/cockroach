// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { NodeStatus } from "src/api";
import { TableMetadata } from "src/api/databases/getTableMetadataApi";
import { NodeID, StoreID } from "src/types/clusterTypes";
import { mapStoreIDsToNodeRegions } from "src/util/nodeUtils";

import { TableRow } from "./types";

export const tableMetadataToRows = (
  tables: TableMetadata[],
  nodesInfo: {
    nodeStatusByID: Record<NodeID, NodeStatus>;
    storeIDToNodeID: Record<StoreID, NodeID>;
    isLoading: boolean;
  },
): TableRow[] => {
  return tables.map(table => {
    const nodesByRegion = mapStoreIDsToNodeRegions(
      table.storeIds,
      nodesInfo?.nodeStatusByID,
      nodesInfo?.storeIDToNodeID,
    );
    return {
      ...table,
      nodesByRegion: {
        isLoading: nodesInfo.isLoading,
        data: nodesByRegion,
      },
      key: table.tableId.toString(),
      qualifiedNameWithSchema: `${table.schemaName}.${table.tableName}`,
    };
  });
};
