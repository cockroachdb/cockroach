// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/**
 * GraphDashboardProps are the properties accepted by the renderable component
 * of each graph dashboard.
 */
export interface GraphDashboardProps {
  /**
   * List of node IDs which should be used in graphs which display a series per
   * node.
   */
  nodeIDs: string[];
  /**
   * List of nodes which should be queried for data. This will be empty if all
   * nodes should be queried.
   */
  nodeSources: string[];
  /**
   * List of stores which should be displayed in the dashboard. This will be
   * empty if all stores should be queried.
   */
  storeSources: string[];
  /**
   * tooltipSelection is a string used in tooltips to reference the currently
   * selected nodes. This is a prepositional phrase, currently either "across
   * all nodes" or "on node X".
   */
  tooltipSelection: string;

  nodeDisplayNameByID: {
    [key: string]: string;
  };

  storeIDsByNodeID: {
    [key: string]: string[];
  };

  // Tenant ID which should be queried for data. This is empty if all tenants
  // should be queried.
  tenantSource?: string;
}

export function nodeDisplayName(
  nodeDisplayNameByID: { [nodeId: string]: string },
  nid: string,
): string {
  return nodeDisplayNameByID[nid] || "unknown node";
}

export function storeIDsForNode(
  storeIDsByNodeID: { [key: string]: string[] },
  nid: string,
): string[] {
  return storeIDsByNodeID[nid] || [];
}
