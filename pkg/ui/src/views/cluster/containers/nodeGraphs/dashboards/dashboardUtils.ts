import _ from "lodash";

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

export function nodeAddress(nodesSummary: NodesSummary, nid: string) {
  const ns = nodesSummary.nodeStatusByID[nid];
  if (!ns) {
    // This should only happen immediately after loading a page, and
    // associated graphs should display no data.
    return "unknown address";
  }
  return ns.desc.address.address_field;
}

export function storeIDsForNode(nodesSummary: NodesSummary, nid: string): string[] {
  const ns = nodesSummary.nodeStatusByID[nid];
  if (!ns) {
    return [];
  }
  return _.map(ns.store_statuses, (ss) => ss.desc.store_id.toString());
}
