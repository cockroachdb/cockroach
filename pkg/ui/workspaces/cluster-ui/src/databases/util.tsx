// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import React from "react";
import { Skeleton, Tooltip } from "antd";
import { Caution } from "@cockroachlabs/icons";
import "antd/lib/tooltip/style";
import "antd/lib/skeleton/style";
import {
  isMaxSizeError,
  isPrivilegeError,
  sqlApiErrorMessage,
  SqlExecutionErrorMessage,
} from "../api";
type IndexUsageStatistic =
  cockroach.server.serverpb.TableIndexStatsResponse.IExtendedCollectedIndexUsageStatistics;

export function combineLoadingErrors(
  detailsErr: Error,
  isMaxSizeError: boolean,
  dbList: string,
): Error {
  if (dbList && detailsErr) {
    return new GetDatabaseInfoError(
      `Failed to load all databases and database details. Partial results are shown. Debug info: ${dbList}, details error: ${detailsErr}`,
    );
  }

  if (dbList) {
    return new GetDatabaseInfoError(
      `Failed to load all databases. Partial results are shown. Debug info: ${dbList}`,
    );
  }

  if (detailsErr) {
    return detailsErr;
  }

  if (isMaxSizeError) {
    return new GetDatabaseInfoError(
      `Failed to load all databases and database details. Partial results are shown. Debug info: Max size limit reached fetching database details`,
    );
  }

  return;
}
export class GetDatabaseInfoError extends Error {
  constructor(message: string) {
    super(message);

    this.name = this.constructor.name;
  }
}

/** Store ids and node ids are both of type `number[]`. To disambiguate, a
 * `kind` field is included in the type. */
export type Stores = { kind: "store"; ids: number[] };

/** Node ids and store IDs are both of type `number[]`. To disambiguate, a
 * `kind` field is included in the type. */
export type Nodes = { kind: "node"; ids: number[] };

/** getNodeIdsFromStoreIds converts a list of store IDs to a list of node IDs.
 *
 * @param stores - Store ids for the cluster.
 * @param nodeStatuses - A list of nodes that includes store information.
 * @returns A list of node ids for the cluster.
 */
export function getNodeIdsFromStoreIds(
  stores: Stores,
  nodeStatuses: cockroach.server.status.statuspb.INodeStatus[],
): Nodes {
  /** Associates stores with their node. Nodes can have multiple stores:
   * `{ store1: node1, store2: node1 }` */
  const nodeByStoreMap: Record<number, number> = {};
  nodeStatuses?.map(node =>
    node.store_statuses?.map(store => {
      nodeByStoreMap[store.desc.store_id] = node.desc.node_id;
    }),
  );

  /** A unique list of node IDs derived from the nodeByStoreMap. */
  const nodeIds = Array.from(
    new Set(stores.ids.map(id => nodeByStoreMap[id])),
  ).filter(value => value !== undefined);

  return { kind: "node", ids: nodeIds };
}

/** getNodesByRegionString converts a list of node IDs to a user-facing string.
 *
 * @param nodes - Node ids for the cluster.
 * @param nodeRegions - A map of node IDs to region IDs.
 * @param isTenant - Whether the cluster is a tenant cluster.
 * @returns A string of node IDs by region, ordered by region name, e.g.
 * `regionA(n1, n2), regionB(n2,n3), ...`
 */
export function getNodesByRegionString(
  nodes: Nodes,
  nodeRegions: Record<string, string>,
  isTenant: boolean,
): string {
  return nodesByRegionMapToString(
    createNodesByRegionMap(nodes.ids, nodeRegions),
    isTenant,
  );
}

/** nodesByRegionMapToString converts a map of regions to node ids,
 * ordered by region name, e.g. converts:
 * `{ regionA: [1, 2], regionB: [2, 3] }`
 * to:
 * `regionA(n1, n2), regionB(n2,n3), ...`
 *
 * If the cluster is a tenant cluster, then we redact node info
 * and only display the region name, e.g.
 * `regionA(n1, n2), regionB(n2,n3), ...` becomes:
 * `regionA, regionB, ...` */
function nodesByRegionMapToString(
  nodesByRegion: Record<string, number[]>,
  isTenant: boolean,
): string {
  // Sort the nodes on each key.
  const regions = Object.keys(nodesByRegion).sort();
  regions.forEach((region: string) => {
    nodesByRegion[region].sort();
  });

  return regions
    .map((region: string) => {
      const nodes = nodesByRegion[region];
      return isTenant
        ? `${region}`
        : `${region}(${nodes.map(id => `n${id}`).join(",")})`;
    })
    .join(", ");
}

function createNodesByRegionMap(
  nodes: number[],
  nodeRegions: Record<string, string>,
): Record<string, number[]> {
  const nodesByRegionMap: Record<string, number[]> = {};
  nodes.forEach((node: number) => {
    // If the node's region doesn't exist skip it.
    if (nodeRegions[node.toString()] == null) {
      return;
    }
    const region: string = nodeRegions[node.toString()];
    if (nodesByRegionMap[region] == null) {
      nodesByRegionMap[region] = [];
    }
    nodesByRegionMap[region].push(node);
  });
  return nodesByRegionMap;
}

// sortByPrecedence sorts a list of strings via a "precedence" mapping.
function sortByPrecedence(
  vals: string[],
  precedenceMapping: Record<string, number>,
  removeDuplicates?: boolean,
): string[] {
  // Sorting function. First compare by precedence.
  // If both items have the same precedence level,
  // sort alphabetically.
  const compareFn = (a: string, b: string) => {
    const aPrecedence = precedenceMapping[a];
    const bPrecendence = precedenceMapping[b];
    if (aPrecedence && bPrecendence) {
      return precedenceMapping[a] - precedenceMapping[b];
    }
    if (aPrecedence) {
      return -1;
    }
    if (bPrecendence) {
      return 1;
    }
    return a.localeCompare(b);
  };

  if (removeDuplicates) {
    return [...new Set(vals)].sort(compareFn);
  }
  return [...vals].sort(compareFn);
}

export function normalizeRoles(raw: string[]): string[] {
  const rolePrecedence: Record<string, number> = {
    root: 1,
    admin: 2,
    public: 3,
  };

  // Unique roles, sorted by precedence.
  return sortByPrecedence(raw, rolePrecedence, true);
}

// normalizePrivileges sorts privileges by privilege precedence.
export function normalizePrivileges(raw: string[]): string[] {
  const privilegePrecedence: Record<string, number> = {
    ALL: 1,
    CREATE: 2,
    DROP: 3,
    GRANT: 4,
    SELECT: 5,
    INSERT: 6,
    UPDATE: 7,
    DELETE: 8,
  };

  // Unique privileges, sorted by precedence.
  const rawUppers = raw.map(priv => priv.toUpperCase());
  return sortByPrecedence(rawUppers, privilegePrecedence, true);
}

export function buildIndexStatToRecommendationsMap(
  stats: IndexUsageStatistic[],
  recs: cockroach.sql.IIndexRecommendation[],
): Record<number, cockroach.sql.IIndexRecommendation[]> {
  const recommendationsMap: Record<
    string,
    cockroach.sql.IIndexRecommendation[]
  > = {};
  stats.forEach(stat => {
    const recsForStat =
      recs.filter(rec => rec.index_id === stat?.statistics.key.index_id) || [];
    if (!recommendationsMap[stat?.statistics.key.index_id]) {
      recommendationsMap[stat?.statistics.key.index_id] = recsForStat;
    } else {
      recommendationsMap[stat?.statistics.key.index_id].push(...recsForStat);
    }
  });
  return recommendationsMap;
}

interface LoadingCellProps {
  requestError: Error;
  queryError?: Error;
  loading: boolean;
  errorClassName: string;
}

export const LoadingCell: React.FunctionComponent<LoadingCellProps> = ({
  loading,
  requestError,
  queryError,
  errorClassName,
  children,
}) => {
  if (loading) {
    return (
      <Skeleton loading={true} active={true} paragraph={false} title={true} />
    );
  }

  let tooltipMsg = "";
  if (requestError) {
    tooltipMsg = `Encountered a network error fetching data for this cell: ${requestError.name}`;
  } else if (queryError) {
    tooltipMsg = getQueryErrorMessage(queryError);
  }

  let childrenOrNoData = <>{children}</>;
  if (children == null) {
    childrenOrNoData = <>{"No data"}</>;
  }

  // If we encounter an error gathering data for this cell,
  // render a warning icon with a tooltip message for the error.
  if (tooltipMsg !== "") {
    return (
      <Tooltip
        overlayStyle={{ whiteSpace: "pre-line" }}
        placement="bottom"
        title={tooltipMsg}
        className={errorClassName}
      >
        <Caution role={"status"} />
        {childrenOrNoData}
      </Tooltip>
    );
  } else {
    return childrenOrNoData;
  }
};

export const getNetworkErrorMessage = (requestError: Error): string => {
  return `Encountered a network error: ${requestError.message}`;
};

export const getQueryErrorMessage = (
  queryError: SqlExecutionErrorMessage | Error,
): string => {
  if (checkPrivilegeError(queryError)) {
    return (
      `User has insufficient privileges:\n` +
      sqlApiErrorMessage(queryError.message)
    );
  }
  if (isMaxSizeError(queryError.message)) {
    return `Only partial data available, total data size exceeds limit in the console`;
  }
  // Unexpected error - return the error message.
  return sqlApiErrorMessage(queryError.message);
};

const checkPrivilegeError = (
  err: SqlExecutionErrorMessage | Error,
): boolean => {
  if ("code" in err) {
    return isPrivilegeError(err.code);
  }
  // If the error message includes any mention of privilege, consider it a privilege error.
  return err.message.includes("privilege");
};
