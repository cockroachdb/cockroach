import _ from "lodash";

import { LocalityTier } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";

export function getLocation(locations: LocationTree, tier: LocalityTier) {
  if (!locations[tier.key]) {
    return null;
  }

  return locations[tier.key][tier.value];
}

export function hasLocation(locations: LocationTree, tier: LocalityTier) {
  return !_.isNil(getLocation(locations, tier));
}

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
