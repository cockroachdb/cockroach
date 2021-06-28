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
import { createSelector } from "reselect";

import { selectCommissionedNodeStatuses } from "src/redux/nodes";
import { INodeStatus } from "src/util/proto";

function buildLocalityTree(nodes: INodeStatus[] = [], depth = 0): LocalityTree {
  const exceedsDepth = (node: INodeStatus) =>
    node.desc.locality.tiers.length > depth;
  const [subsequentNodes, thisLevelNodes] = _.partition(nodes, exceedsDepth);

  const localityKeyGroups = _.groupBy(
    subsequentNodes,
    (node) => node.desc.locality.tiers[depth].key,
  );

  const localityValueGroups = _.mapValues(
    localityKeyGroups,
    (group: INodeStatus[]) =>
      _.groupBy(group, (node) => node.desc.locality.tiers[depth].value),
  );

  const childLocalities = _.mapValues(localityValueGroups, (groups) =>
    _.mapValues(groups, (group: INodeStatus[]) =>
      buildLocalityTree(group, depth + 1),
    ),
  );

  const tiers = _.isEmpty(nodes)
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
