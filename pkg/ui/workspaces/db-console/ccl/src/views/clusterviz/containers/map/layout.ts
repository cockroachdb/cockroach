// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import _ from "lodash";

import { LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { getChildLocalities } from "src/util/localities";
import { findOrCalculateLocation } from "src/util/locations";

export function renderAsMap(
  locationTree: LocationTree,
  localityTree: LocalityTree,
) {
  // If there are any nodes directly under this locality, don't show a map.
  if (!_.isEmpty(localityTree.nodes)) {
    return false;
  }

  // Otherwise, show a map as long as we're able to find or calculate a location
  // for every child locality.
  const children = getChildLocalities(localityTree);
  return _.every(
    children,
    (child) => !_.isNil(findOrCalculateLocation(locationTree, child)),
  );
}
