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

type FlatGrant = {
  grantee: string;
  privilege: string;
};

export const groupGrantsByGrantee = (grants: FlatGrant[]) => {
  if (!grants?.length) {
    return [];
  }

  const grantsByUser = {} as Record<string, string[]>;
  grants.forEach(grant => {
    if (!grantsByUser[grant.grantee]) {
      grantsByUser[grant.grantee] = [];
    }
    grantsByUser[grant.grantee].push(grant.privilege);
  });

  return Object.entries(grantsByUser).map(([grantee, privileges]) => ({
    key: grantee,
    grantee,
    privileges,
  }));
};

export const getNodesFromStores = (
  stores: string[],
  nodes: Record<NodeID, NodeStatus>,
): NodeID[] => {
  const storeSet = new Set(
    stores.map((store: string) => parseInt(store, 10) as StoreID),
  );
  const result = [];
  for (const [id, node] of Object.entries(nodes)) {
    if (node?.stores?.some((sid: StoreID) => storeSet.has(sid))) {
      result.push(parseInt(id, 10) as NodeID);
    }
  }
  return result;
};
