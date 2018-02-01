import _ from "lodash";

import { LocalityTier } from "src/redux/localities";
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
