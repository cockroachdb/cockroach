// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

import d3 from "d3";
import { createSelector } from "reselect";

import * as protos from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import { Pick } from "src/util/pick";

export type ILocation = protos.cockroach.server.serverpb.LocationsResponse.ILocation;

type LocationState = Pick<AdminUIState, "cachedData", "locations">;

export function selectLocationsRequestStatus(state: LocationState) {
  return state.cachedData.locations;
}

export function selectLocations(state: LocationState) {
  if (!state.cachedData.locations.data) {
    return [];
  }

  return state.cachedData.locations.data.locations;
}

const nestLocations = d3.nest()
  .key((loc: ILocation) => loc.locality_key)
  .key((loc: ILocation) => loc.locality_value)
  .rollup((locations) => locations[0]) // cannot collide since ^^ is primary key
  .map;

export interface LocationTree {
  [key: string]: {
    [value: string]: ILocation,
  };
}

export const selectLocationTree = createSelector(
  selectLocations,
  (ls: ILocation[]) => nestLocations(ls), // TSLint won't let this be point-free
);
