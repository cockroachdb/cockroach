import _ from "lodash";
import { createSelector } from "reselect";

import { nodeStatusesSelector } from "src/redux/nodes";
import { NodeStatus$Properties } from "src/util/proto";

function buildLocalityTree(nodes: NodeStatus$Properties[] = [], depth = 0): LocalityTree {
  const exceedsDepth = (node: NodeStatus$Properties) => node.desc.locality.tiers.length > depth;
  const [subsequentNodes, thisLevelNodes] = _.partition(nodes, exceedsDepth);

  const localityKeyGroups = _.groupBy(subsequentNodes, (node) => node.desc.locality.tiers[depth].key);

  const localityValueGroups = _.mapValues(
    localityKeyGroups,
    (group: NodeStatus$Properties[]) => _.groupBy(group, (node) => node.desc.locality.tiers[depth].value),
  );

  const childLocalities = _.mapValues(
    localityValueGroups,
    (groups) =>
      _.mapValues(
        groups,
        (group: NodeStatus$Properties[]) => buildLocalityTree(group, depth + 1),
      ),
  );

  const tiers = _.isEmpty(nodes) ? [] : <LocalityTier[]>nodes[0].desc.locality.tiers.slice(0, depth);

  return {
    tiers: tiers,
    nodes: thisLevelNodes,
    localities: childLocalities,
  };
}

export const selectLocalityTree = createSelector(
  nodeStatusesSelector,
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
      [localityValue: string]: LocalityTree,
    },
  };
  nodes: NodeStatus$Properties[];
}

export function getLeaves(tree: LocalityTree): NodeStatus$Properties[] {
  const output: NodeStatus$Properties[] = [];
  function recur(curTree: LocalityTree) {
    output.push(...curTree.nodes);
    _.forEach(curTree.localities, (localityValues) => {
      _.forEach(localityValues, (localityValue) => {
        recur(localityValue);
      });
    });
  }
  recur(tree);
  return output;
}
