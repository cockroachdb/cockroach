import _ from "lodash";

import { LocalityTier } from "src/redux/localities";

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
