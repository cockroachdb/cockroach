// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";

import { LocalityTier, LocalityTree } from "src/redux/localities";
import { INodeStatus } from "src/util/proto";

/*
 * parseLocalityRoute parses the URL fragment used to route to a particular
 * locality and returns the locality tiers it represents.
 */
export function parseLocalityRoute(route: string): LocalityTier[] {
  if (_.isEmpty(route)) {
    return [];
  }

  const segments = route.split("/");
  return segments.map((segment) => {
    const [key, value] = segment.split("=");
    return { key, value };
  });
}

/*
 * generateLocalityRoute generates the URL fragment to route to a particular
 * locality from the locality tiers.
 */
export function generateLocalityRoute(tiers: LocalityTier[]): string {
  return "/" + tiers.map(({ key, value }) => key + "=" + value).join("/");
}

/*
 * getNodeLocalityTiers returns the locality tiers of a node, typed as an array
 * of LocalityTier rather than Tier$Properties.
 */
export function getNodeLocalityTiers(node: INodeStatus): LocalityTier[] {
  return node.desc.locality.tiers.map(({ key, value }) => ({ key, value }));
}

/*
 * getChildLocalities returns an array of the locality trees for localities that
 * are the immediate children of this one.
 */
export function getChildLocalities(locality: LocalityTree): LocalityTree[] {
  const children: LocalityTree[] = [];

  _.values(locality.localities).forEach((tier) => {
    children.push(..._.values(tier));
  });

  return children;
}

/*
 * getLocality gets the locality within this tree which corresponds to a set of
 * locality tiers, or null if the locality is not present.
 */
export function getLocality(
  localityTree: LocalityTree,
  tiers: LocalityTier[],
): LocalityTree {
  let result = localityTree;
  for (let i = 0; i < tiers.length; i += 1) {
    const { key, value } = tiers[i];

    const thisTier = result.localities[key];
    if (_.isNil(thisTier)) {
      return null;
    }

    result = thisTier[value];
    if (_.isNil(result)) {
      return null;
    }
  }

  return result;
}

/* getLeaves returns the leaves under the given locality tree. Confusingly,
 * in this tree the "leaves" of the tree are nodes, i.e. servers.
 */
export function getLeaves(tree: LocalityTree): INodeStatus[] {
  const output: INodeStatus[] = [];
  function recur(curTree: LocalityTree) {
    output.push(...curTree.nodes);
    _.forEach(curTree.localities, (localityValues) => {
      _.forEach(localityValues, recur);
    });
  }
  recur(tree);
  return output;
}

/*
 * getLocalityLabel returns the human-readable label for the locality.
 */
export function getLocalityLabel(path: LocalityTier[]): string {
  if (path.length === 0) {
    return "Cluster";
  }

  const thisTier = path[path.length - 1];
  return `${thisTier.key}=${thisTier.value}`;
}

/*
 * allNodesHaveLocality returns true if there exists a node without a locality flag.
 */
export function allNodesHaveLocality(nodes: INodeStatus[]): boolean {
  const nodesWithoutLocality = nodes.filter(
    (n) => n.desc.locality.tiers.length === 0,
  );
  return nodesWithoutLocality.length === 0;
}
