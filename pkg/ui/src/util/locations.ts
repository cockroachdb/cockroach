import _ from "lodash";

import { LocalityTier } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";

/*
 * getLocation retrieves the location for a given locality tier from the
 * LocationTree, or null if none is found.
 */
export function getLocation(locations: LocationTree, tier: LocalityTier) {
  if (!locations[tier.key]) {
    return null;
  }

  return locations[tier.key][tier.value];
}

/*
 * hasLocation is a predicate to determine if a given locality tier exists in
 * the LocationTree.
 */
export function hasLocation(locations: LocationTree, tier: LocalityTier) {
  return !_.isNil(getLocation(locations, tier));
}

/*
 * findMostSpecificLocation searches for a location matching the given locality
 * tiers in the LocationTree.  It tries to find the most specific location that
 * applies, and thus begins searching from the end of the list of tiers for a
 * tier with a matching location.  Returns null if none is found.
 */
export function findMostSpecificLocation(locations: LocationTree, tiers: LocalityTier[]) {
  let currentIndex = tiers.length - 1;
  while (currentIndex >= 0) {
    const currentTier = tiers[currentIndex];
    const location = getLocation(locations, currentTier);

    if (!_.isNil(location)) {
      return location;
    }

    currentIndex -= 1;
  }

  return null;
}
