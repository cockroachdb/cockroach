// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { NodesSummary } from "src/redux/nodes";

/**
 * GraphDashboardProps are the properties accepted by the renderable component
 * of each graph dashboard.
 */
export interface GraphDashboardProps {
  /**
   * Summary of nodes data.
   */
  nodesSummary: NodesSummary;
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
}

export function nodeDisplayName(nodesSummary: NodesSummary, nid: string) {
  const ns = nodesSummary.nodeStatusByID[nid];
  if (!ns) {
    // This should only happen immediately after loading a page, and
    // associated graphs should display no data.
    return "unknown node";
  }
  return nodesSummary.nodeDisplayNameByID[ns.desc.node_id];
}

export function storeIDsForNode(
  nodesSummary: NodesSummary,
  nid: string,
): string[] {
  return nodesSummary.storeIDsByNodeID[nid] || [];
}
