// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import { TableMetadata } from "src/api/databases/getTableMetadataApi";
import { NodeID, StoreID } from "src/types/clusterTypes";

import { TableRow } from "./types";

export const rawTableMetadataToRows = (
  tables: TableMetadata[],
  nodesInfo: {
    nodeIDToRegion: Record<NodeID, string>;
    storeIDToNodeID: Record<StoreID, NodeID>;
    isLoading: boolean;
  },
): TableRow[] => {
  return tables.map(table => {
    const nodesByRegion: Record<string, NodeID[]> = {};
    if (!nodesInfo.isLoading) {
      table.store_ids?.forEach(storeID => {
        const nodeID = nodesInfo.storeIDToNodeID[storeID as StoreID];
        const region = nodesInfo.nodeIDToRegion[nodeID];
        if (!nodesByRegion[region]) {
          nodesByRegion[region] = [];
        }
        nodesByRegion[region].push(nodeID);
      });
    }
    return {
      name: table.table_name,
      dbName: table.db_name,
      tableID: table.table_id,
      dbID: table.db_id,
      replicationSizeBytes: table.replication_size_bytes,
      rangeCount: table.range_count,
      columnCount: table.column_count,
      nodesByRegion: nodesByRegion,
      liveDataPercentage: table.percent_live_data,
      liveDataBytes: table.total_live_data_bytes,
      totalDataBytes: table.total_data_bytes,
      statsLastUpdated: moment.utc(table.stats_last_updated),
      autoStatsEnabled: table.auto_stats_enabled,
      key: table.table_id.toString(),
      qualifiedNameWithSchema: `${table.schema_name}.${table.table_name}`,
    };
  });
};
