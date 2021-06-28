// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

const nestLocations = d3
  .nest()
  .key((loc: ILocation) => loc.locality_key)
  .key((loc: ILocation) => loc.locality_value)
  .rollup((locations) => locations[0]).map; // cannot collide since ^^ is primary key

export interface LocationTree {
  [key: string]: {
    [value: string]: ILocation;
  };
}

export const selectLocationTree = createSelector(
  selectLocations,
  (ls: ILocation[]) => nestLocations(ls), // TSLint won't let this be point-free
);
