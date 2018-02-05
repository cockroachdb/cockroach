import _ from "lodash";

import { LocalityTier, LocalityTree } from "src/redux/localities";
import { NodeStatus$Properties } from "src/util/proto";

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
  return tiers.map(({ key, value }) => key + "=" + value).join("/");
}

/*
 * getNodeLocalityTiers returns the locality tiers of a node, typed as an array
 * of LocalityTier rather than Tier$Properties.
 */
export function getNodeLocalityTiers(node: NodeStatus$Properties): LocalityTier[] {
  return node.desc.locality.tiers.map(({ key, value }) => ({ key, value }));
}

/*
 * getLocality gets the locality within this tree which corresponds to a set of
 * locality tiers, or null if the locality is not present.
 */
export function getLocality(localityTree: LocalityTree, tiers: LocalityTier[]): LocalityTree {
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
