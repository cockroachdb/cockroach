// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { BreadcrumbItem } from "../breadcrumbs";

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

// getNodesByRegionString converts a list of node ids and map of
// node ids to region to a string of node ids by region, ordered
// by region name, e.g.
// regionA(n1, n2), regionB(n2,n3), ...
export function getNodesByRegionString(
  nodes: number[],
  nodeRegions: Record<string, string>,
  isTenant: boolean,
): string {
  return nodesByRegionMapToString(
    createNodesByRegionMap(nodes, nodeRegions),
    isTenant,
  );
}

// nodesByRegionMapToString converts a map of regions to node ids,
// ordered by region name, e.g. converts:
// { regionA: [1, 2], regionB: [2, 3] }
// to:
// regionA(n1, n2), regionB(n2,n3), ...
// If the cluster is a tenant cluster, then we redact node info
// and only display the region name, e.g.
// regionA(n1, n2), regionB(n2,n3), ... becomes:
// regionA, regionB, ...
export function nodesByRegionMapToString(
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

export function createNodesByRegionMap(
  nodes: number[],
  nodeRegions: Record<string, string>,
): Record<string, number[]> {
  const nodesByRegionMap: Record<string, number[]> = {};
  nodes.forEach((node: number) => {
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

// Note: if the managed-service routes to the index detail or the previous
// database pages change, the breadcrumbs displayed here need to be updated.
// TODO(thomas): ensure callers are splitting schema/table name correctly
export function createManagedServiceBreadcrumbs(
  database: string,
  schema: string,
  table: string,
  index: string,
): BreadcrumbItem[] {
  return [
    { link: "/databases", name: "Databases" },
    {
      link: `/databases/${database}`,
      name: "Tables",
    },
    {
      link: `/databases/${database}/${schema}/${table}`,
      name: `Table: ${table}`,
    },
    {
      link: `/databases/${database}/${schema}/${table}/${index}`,
      name: `Index: ${index}`,
    },
  ];
}
