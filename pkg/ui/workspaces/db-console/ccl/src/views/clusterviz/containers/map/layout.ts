// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import every from "lodash/every";
import isEmpty from "lodash/isEmpty";
import isNil from "lodash/isNil";

import { LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { getChildLocalities } from "src/util/localities";
import { findOrCalculateLocation } from "src/util/locations";

export function renderAsMap(
  locationTree: LocationTree,
  localityTree: LocalityTree,
) {
  // If there are any nodes directly under this locality, don't show a map.
  if (!isEmpty(localityTree.nodes)) {
    return false;
  }

  // Otherwise, show a map as long as we're able to find or calculate a location
  // for every child locality.
  const children = getChildLocalities(localityTree);
  return every(
    children,
    child => !isNil(findOrCalculateLocation(locationTree, child)),
  );
}
