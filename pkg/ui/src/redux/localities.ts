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

  return {
    nodes: thisLevelNodes,
    localities: childLocalities,
  };
}

export const selectLocalityTree = createSelector(
  nodeStatusesSelector,
  buildLocalityTree,
);

export interface LocalityTree {
  localities: {
    [localityKey: string]: {
      [localityValue: string]: LocalityTree,
    },
  };
  nodes: NodeStatus$Properties[];
}
