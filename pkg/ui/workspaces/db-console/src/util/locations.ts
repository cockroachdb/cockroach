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

import { LocalityTier, LocalityTree } from "src/redux/localities";
import { ILocation, LocationTree } from "src/redux/locations";
import * as vector from "src/util/vector";

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
export function findMostSpecificLocation(
  locations: LocationTree,
  tiers: LocalityTier[],
) {
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

/*
 * findOrCalculateLocation tries to place a locality on the map.  If there is
 * no location assigned to the locality itself, calculate the centroid of the
 * children.
 */
export function findOrCalculateLocation(
  locations: LocationTree,
  locality: LocalityTree,
) {
  // If a location is assigned to this locality, return it.
  const thisTier = locality.tiers[locality.tiers.length - 1];
  const thisLocation = getLocation(locations, thisTier);
  if (!_.isNil(thisLocation)) {
    return thisLocation;
  }

  // If this locality has nodes directly, we can't calculate a location; bail.
  if (!_.isEmpty(locality.nodes)) {
    return null;
  }

  // If this locality has no child localities, we can't calculate a location.
  // Note, this shouldn't ever actually happen.
  if (_.isEmpty(locality.localities)) {
    return null;
  }

  // Find (or calculate) the location of each child locality.
  const childLocations: ILocation[] = [];
  _.values(locality.localities).forEach((tier) => {
    _.values(tier).forEach((child) => {
      childLocations.push(findOrCalculateLocation(locations, child));
    });
  });

  // If any child location is missing, bail.
  if (_.some(childLocations, _.isNil)) {
    return null;
  }

  // Calculate the centroid of the child locations.
  let centroid: [number, number] = [0, 0];
  childLocations.forEach(
    (loc) => (centroid = vector.add(centroid, [loc.longitude, loc.latitude])),
  );
  centroid = vector.mult(centroid, 1 / childLocations.length);
  return { longitude: centroid[0], latitude: centroid[1] };
}
