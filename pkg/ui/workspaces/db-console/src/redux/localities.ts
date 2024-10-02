// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import groupBy from "lodash/groupBy";
import isEmpty from "lodash/isEmpty";
import mapValues from "lodash/mapValues";
import partition from "lodash/partition";
import { createSelector } from "reselect";

import { selectCommissionedNodeStatuses } from "src/redux/nodes";
import { INodeStatus } from "src/util/proto";

function buildLocalityTree(nodes: INodeStatus[] = [], depth = 0): LocalityTree {
  const exceedsDepth = (node: INodeStatus) =>
    node.desc.locality.tiers.length > depth;
  const [subsequentNodes, thisLevelNodes] = partition(nodes, exceedsDepth);

  const localityKeyGroups = groupBy(
    subsequentNodes,
    node => node.desc.locality.tiers[depth].key,
  );

  const localityValueGroups = mapValues(
    localityKeyGroups,
    (group: INodeStatus[]) =>
      groupBy(group, node => node.desc.locality.tiers[depth].value),
  );

  const childLocalities = mapValues(localityValueGroups, groups =>
    mapValues(groups, (group: INodeStatus[]) =>
      buildLocalityTree(group, depth + 1),
    ),
  );

  const tiers = isEmpty(nodes)
    ? []
    : <LocalityTier[]>nodes[0].desc.locality.tiers.slice(0, depth);

  return {
    tiers: tiers,
    nodes: thisLevelNodes,
    localities: childLocalities,
  };
}

export const selectLocalityTree = createSelector(
  selectCommissionedNodeStatuses,
  buildLocalityTree,
);

export interface LocalityTier {
  key: string;
  value: string;
}

export interface LocalityTree {
  tiers: LocalityTier[];
  localities: {
    [localityKey: string]: {
      [localityValue: string]: LocalityTree;
    };
  };
  nodes: INodeStatus[];
}

// selectNodeLocalities returns a map of node ID to stringified version
// of locality values (i.e. "region=us-east1, az=2").
export const selectNodeLocalities = createSelector(
  selectCommissionedNodeStatuses,
  nodes => {
    return new Map(
      nodes.map(n => {
        const locality = (n.desc?.locality?.tiers || [])
          .map(t => `${t.key}=${t.value}`)
          .join(", ");
        return [n.desc.node_id, locality];
      }),
    );
  },
);
