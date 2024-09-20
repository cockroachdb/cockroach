// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment-timezone";

import {
  TableMetadata,
  TableSortOption,
} from "src/api/databases/getTableMetadataApi";

import { NodeID, StoreID } from "../types/clusterTypes";

import { TableColName } from "./constants";
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
      dbID: table.db_id,
      replicationSizeBytes: table.replication_size_bytes,
      rangeCount: table.range_count,
      columnCount: table.column_count,
      nodesByRegion: nodesByRegion,
      liveDataPercentage: table.percent_live_data,
      liveDataBytes: table.total_live_data_bytes,
      totalDataBytes: table.total_data_bytes,
      statsLastUpdated: moment.utc(table.last_updated), // TODO (xinhaoz): populate this
      key: table.table_id.toString(),
      qualifiedNameWithSchema: `${table.schema_name}.${table.table_name}`,
    };
  });
};
