// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import groupBy from "lodash/groupBy";
import isEmpty from "lodash/isEmpty";
import mapValues from "lodash/mapValues";
import partition from "lodash/partition";

import { INodeStatus } from "src/util/proto";

export function buildLocalityTree(
  nodes: INodeStatus[] = [],
  depth = 0,
): LocalityTree {
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
